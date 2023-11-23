"""Convenience functions related to Hail."""

import asyncio
import inspect
import logging
import os
import tempfile
import textwrap
from typing import Dict, List, Literal, Optional, Union

import hail as hl
import hailtop.batch as hb
from hail.utils.java import Env

from cpg_utils import Path, to_path
from cpg_utils.config import (
    AR_GUID_NAME,
    ConfigError,
    get_config,
    retrieve,
    try_get_ar_guid,
)

# template commands strings
GCLOUD_AUTH_COMMAND = """\
export GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json
gcloud -q auth activate-service-account \
--key-file=$GOOGLE_APPLICATION_CREDENTIALS
"""


_batch: Optional['Batch'] = None


def reset_batch():
    """Reset the global batch reference, useful for tests"""
    global _batch  # pylint: disable=global-statement
    _batch = None


def get_batch(
    name: str | None = None,
    *,
    default_python_image: str | None = None,
    attributes: Optional[Dict[str, str]] = None,
    **kwargs,
) -> 'Batch':
    """
    Wrapper around Hail's `Batch` class, which allows to register created jobs
    This has been migrated (currently duplicated) out of cpg_workflows

    Parameters
    ----------
    name : str, optional, name for the batch
    default_python_image : str, optional, default python image to use

    Returns
    -------
    If there are scheduled jobs, return the batch
    If there are no jobs to create, return None
    """
    global _batch  # pylint: disable=global-statement
    backend: hb.Backend
    if _batch is None:
        if get_config()['hail'].get('backend', 'batch') == 'local':
            logging.info('Initialising Hail Batch with local backend')
            backend = hb.LocalBackend(
                tmp_dir=tempfile.mkdtemp('batch-tmp'),
            )
        else:
            logging.info('Initialising Hail Batch with service backend')
            backend = hb.ServiceBackend(
                billing_project=get_config()['hail']['billing_project'],
                remote_tmpdir=dataset_path('batch-tmp', category='tmp'),
                token=os.environ.get('HAIL_TOKEN'),
            )
        _batch = Batch(
            name=name or get_config()['workflow'].get('name'),
            backend=backend,
            pool_label=get_config()['hail'].get('pool_label'),
            cancel_after_n_failures=get_config()['hail'].get('cancel_after_n_failures'),
            default_timeout=get_config()['hail'].get('default_timeout'),
            default_memory=get_config()['hail'].get('default_memory'),
            default_python_image=default_python_image
            or get_config()['workflow']['driver_image'],
            attributes=attributes,
            **kwargs,
        )
    return _batch


class Batch(hb.Batch):
    """
    Thin subclass of the Hail `Batch` class. The aim is to be able to register
    created jobs, in order to print statistics before submitting the Batch.
    """

    def __init__(
        self,
        name,
        backend,
        *,
        pool_label: Optional[str] = None,
        attributes: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        _attributes = attributes or {}
        if AR_GUID_NAME not in _attributes:
            _attributes[AR_GUID_NAME] = try_get_ar_guid()

        super().__init__(name, backend, attributes=_attributes, **kwargs)
        # Job stats registry:
        self.job_by_label: Dict = {}
        self.job_by_stage: Dict = {}
        self.job_by_tool: Dict = {}
        self.total_job_num = 0
        self.pool_label = pool_label

    def _process_job_attributes(
        self,
        name: str | None = None,
        attributes: dict | None = None,
    ) -> tuple[str, dict[str, str]]:
        """
        Use job attributes to make the job name more descriptive, and add
        labels for Batch pre-submission stats.
        """
        if not name:
            raise ValueError('Error: job name must be defined')

        self.total_job_num += 1

        attributes = attributes or {}
        stage = attributes.get('stage')
        dataset = attributes.get('dataset')
        sequencing_group = attributes.get('sequencing_group')
        participant_id = attributes.get('participant_id')
        sequencing_groups: set[str] = set(attributes.get('sequencing_groups') or [])
        if sequencing_group:
            sequencing_groups.add(sequencing_group)
        part = attributes.get('part')
        label = attributes.get('label', name)
        tool = attributes.get('tool')
        if not tool and name.endswith('Dataproc cluster'):
            tool = 'hailctl dataproc'

        # pylint: disable=W1116
        assert isinstance(stage, str | None)
        assert isinstance(dataset, str | None)
        assert isinstance(sequencing_group, str | None)
        assert isinstance(participant_id, str | None)
        assert isinstance(part, str | None)
        assert isinstance(label, str | None)

        name = make_job_name(
            name=name,
            sequencing_group=sequencing_group,
            participant_id=participant_id,
            dataset=dataset,
            part=part,
        )

        if label not in self.job_by_label:
            self.job_by_label[label] = {'job_n': 0, 'sequencing_groups': set()}
        self.job_by_label[label]['job_n'] += 1
        self.job_by_label[label]['sequencing_groups'] |= sequencing_groups

        if stage not in self.job_by_stage:
            self.job_by_stage[stage] = {'job_n': 0, 'sequencing_groups': set()}
        self.job_by_stage[stage]['job_n'] += 1
        self.job_by_stage[stage]['sequencing_groups'] |= sequencing_groups

        if tool not in self.job_by_tool:
            self.job_by_tool[tool] = {'job_n': 0, 'sequencing_groups': set()}
        self.job_by_tool[tool]['job_n'] += 1
        self.job_by_tool[tool]['sequencing_groups'] |= sequencing_groups

        attributes['sequencing_groups'] = list(sorted(list(sequencing_groups)))
        fixed_attrs = {k: str(v) for k, v in attributes.items()}
        return name, fixed_attrs

    def run(self, **kwargs):  # pylint: disable=R1710,W0221
        """
        Execute a batch. Overridden to print pre-submission statistics.
        Pylint disables:
        - R1710: Either all return statements in a function should return an expression,
          or none of them should.
          - if no jobs are present, no batch is returned. Hail should have this behaviour...
        - W0221: Arguments number differs from overridden method
          - this wrapper makes use of **kwargs, which is being passed to the super().run() method
        """
        if not self._jobs:
            logging.error('No jobs to submit')
            return

        for job in self._jobs:
            job.name, job.attributes = self._process_job_attributes(
                job.name, job.attributes
            )
            # We only have dedicated pools for preemptible machines.
            # _preemptible defaults to None, so check explicitly for False.
            # pylint: disable=W0212
            if self.pool_label and job._preemptible is not False:
                job._pool_label = self.pool_label
            copy_common_env(job)

        logging.info(f'Will submit {self.total_job_num} jobs')

        def _print_stat(prefix: str, _d: dict, default_label: str | None = None):
            m = (prefix or ' ') + '\n'
            for label, stat in _d.items():
                label = label or default_label
                msg = f'{stat["job_n"]} job'
                if stat['job_n'] > 1:
                    msg += 's'
                if (sg_count := len(stat['sequencing_groups'])) > 0:
                    msg += f' for {sg_count} sequencing group'
                    if sg_count > 1:
                        msg += 's'
                m += f'  {label}: {msg}'
            logging.info(m)

        _print_stat(
            'Split by stage:', self.job_by_stage, default_label='<not in stage>'
        )
        _print_stat(
            'Split by tool:', self.job_by_tool, default_label='<tool is not defined>'
        )

        kwargs.setdefault('dry_run', get_config()['hail'].get('dry_run'))
        kwargs.setdefault(
            'delete_scratch_on_exit', get_config()['hail'].get('delete_scratch_on_exit')
        )
        if isinstance(self._backend, hb.LocalBackend):
            # Local backend does not support "wait"
            if 'wait' in kwargs:
                del kwargs['wait']
        return super().run(**kwargs)


def make_job_name(
    name: str,
    sequencing_group: str | None = None,
    participant_id: str | None = None,
    dataset: str | None = None,
    part: str | None = None,
) -> str:
    """
    Extend the descriptive job name to reflect job attributes.
    """
    if sequencing_group and participant_id:
        sequencing_group = f'{sequencing_group}/{participant_id}'
    if sequencing_group and dataset:
        name = f'{dataset}/{sequencing_group}: {name}'
    elif dataset:
        name = f'{dataset}: {name}'
    if part:
        name += f', {part}'
    return name


def init_batch(**kwargs):
    """
    Initializes the Hail Query Service from within Hail Batch.
    Requires the `hail/billing_project` and `hail/bucket` config variables to be set.

    Parameters
    ----------
    kwargs : keyword arguments
        Forwarded directly to `hl.init_batch`.
    """
    # noinspection PyProtectedMember
    if Env._hc:  # pylint: disable=W0212
        return  # already initialised
    dataset = get_config()['workflow']['dataset']
    kwargs.setdefault('token', os.environ.get('HAIL_TOKEN'))
    asyncio.get_event_loop().run_until_complete(
        hl.init_batch(
            default_reference=genome_build(),
            billing_project=get_config()['hail']['billing_project'],
            remote_tmpdir=remote_tmpdir(f'cpg-{dataset}-hail'),
            **kwargs,
        )
    )


def copy_common_env(job: hb.batch.job.Job) -> None:
    """Copies common environment variables that we use to run Hail jobs.

    These variables are typically set up in the analysis-runner driver, but need to be
    passed through for "batch-in-batch" use cases.

    The environment variable values are extracted from the current process and
    copied to the environment dictionary of the given Hail Batch job.
    """
    # If possible, please don't add new environment variables here, but instead add
    # config variables.
    for key in ('CPG_CONFIG_PATH',):
        val = os.getenv(key)
        if val:
            job.env(key, val)

    ar_guid = try_get_ar_guid()
    if ar_guid:
        job.attributes[AR_GUID_NAME] = ar_guid


def remote_tmpdir(hail_bucket: Optional[str] = None) -> str:
    """Returns the remote_tmpdir to use for Hail initialization.

    If `hail_bucket` is not specified explicitly, requires the `hail/bucket` config variable to be set.
    """
    bucket = hail_bucket or get_config().get('hail', {}).get('bucket')
    assert bucket, 'hail_bucket was not set by argument or configuration'
    return f'gs://{bucket}/batch-tmp'


def dataset_path(
    suffix: str,
    category: str | None = None,
    dataset: str | None = None,
    test: bool = False,
) -> str:
    """
    Returns a full path for the current dataset, given a category and a path suffix.

    This is useful for specifying input files, as in contrast to the `output_path`
    function, `dataset_path` does _not_ take the `workflow/output_prefix` config
    variable into account.

    Assumes the config structure like below, which is auto-generated by
    the analysis-runner:

    ```toml
    [storage.default]
    default = "gs://thousand-genomes-main"
    web = "gs://cpg-thousand-genomes-main-web"
    analysis = "gs://cpg-thousand-genomes-main-analysis"
    tmp = "gs://cpg-thousand-genomes-main-tmp"
    web_url = "https://main-web.populationgenomics.org.au/thousand-genomes"

    [storage.thousand-genomes]
    default = "gs://cpg-thousand-genomes-main"
    web = "gs://cpg-thousand-genomes-main-web"
    analysis = "gs://cpg-thousand-genomes-main-analysis"
    tmp = "gs://cpg-thousand-genomes-main-tmp"
    web_url = "https://main-web.populationgenomics.org.au/thousand-genomes"
    ```

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test`:

    >>> from cpg_utils.hail_batch import dataset_path
    >>> dataset_path('1kg_densified/combined.mt')
    'gs://cpg-fewgenomes-test/1kg_densified/combined.mt'
    >>> dataset_path('1kg_densified/report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'
    >>> dataset_path('1kg_densified/report.html', 'web', test=True)
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'

    Notes
    -----
    Requires either the
    * `workflow/dataset` and `workflow/access_level` config variables to be set.

    Parameters
    ----------
    suffix : str
        A path suffix to append to the bucket.
    category : str, optional
        A category like "tmp", "web", etc., defaults to "default" if omited.
    dataset : str, optional
        Dataset name, takes precedence over the `workflow/dataset` config variable
    test : bool
        Return "test" namespace version of the path

    Returns
    -------
    str
    """
    if dataset and dataset not in get_config()['storage']:
        raise ConfigError(
            f'Storage section for dataset "{dataset}" not found in config. '
            f'Please check that you have permissions to the dataset. '
            f'Expected section: [storage.{dataset}]'
        )
    dataset = dataset or 'default'

    # manual redirect to test paths
    if test and not get_config()['workflow']['access_level'] == 'test':
        section = get_config()['storage'][dataset]['test']
    else:
        section = get_config()['storage'][dataset]

    category = category or 'default'
    if not (prefix := section.get(category)):
        raise ConfigError(
            f'Category "{category}" not found in storage section '
            f'for dataset "{dataset}": {section}'
        )

    return os.path.join(prefix, suffix)


def cpg_test_dataset_path(
    suffix: str,
    category: str | None = None,
    dataset: str | None = None,
) -> str:
    """
    CPG-specific method to get corresponding test paths when running
    from the main namespace.
    """
    dataset = dataset or 'default'
    category = category or 'default'
    prefix = get_config()['storage'][dataset]['test'][category]
    return os.path.join(prefix, suffix)


def web_url(suffix: str = '', dataset: str | None = None) -> str:
    """
    Web URL to match the dataset_path of category 'web'.
    """
    return dataset_path(suffix=suffix, dataset=dataset, category='web_url')


def output_path(
    suffix: str,
    category: Optional[str] = None,
    dataset: str | None = None,
    test: bool = False,
) -> str:
    """
    Returns a full path for the given category and path suffix.

    In contrast to the `dataset_path` function, `output_path` takes the
    `workflow/output_prefix` config variable into account.

    Examples
    --------
    If using the analysis-runner, the `workflow/output_prefix` would be set to the
    value provided using the --output argument, e.g.:
    ```
    analysis-runner --dataset fewgenomes --access-level test --output 1kg_pca/v42` ...
    ```
    will use '1kg_pca/v42' as the base path to build upon in this method:

    >>> from cpg_utils.hail_batch import output_path
    >>> output_path('loadings.ht')
    'gs://cpg-fewgenomes-test/1kg_pca/v42/loadings.ht'
    >>> output_path('report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_pca/v42/report.html'

    Notes
    -----
    Requires the `workflow/output_prefix` config variable to be set, in addition to the
    requirements for `dataset_path`.

    Parameters
    ----------
    suffix : str
        A path suffix to append to the bucket + output directory.
    category : str, optional
        A category like "tmp", "web", etc., defaults to "default" if ommited.
    dataset : str, optional
        Dataset name, takes precedence over the `workflow/dataset` config variable
    test : bool, optional
        Boolean - if True, generate a test bucket path. Default to False.

    Returns
    -------
    str
    """
    return dataset_path(
        os.path.join(get_config()['workflow']['output_prefix'], suffix),
        category=category,
        dataset=dataset,
        test=test,
    )


def image_path(key: str) -> str:
    """
    Returns a path to a container image using key in config's "images" section.

    Examples
    --------
    >>> image_path('bcftools')
    'australia-southeast1-docker.pkg.dev/cpg-common/images/bcftools:1.10.2'

    Assuming config structure as follows:

    ```toml
    [images]
    bcftools = 'australia-southeast1-docker.pkg.dev/cpg-common/images/bcftools:1.10.2'
    ```

    Parameters
    ----------
    key : str
        Describes the key within the `images` config section. Can list sections
        separated with '/'.

    Returns
    -------
    str
    """
    return retrieve(['images'] + key.strip('/').split('/'))


def reference_path(key: str) -> Path:
    """
    Returns a path to a reference resource using key in config's "references" section.

    Examples
    --------
    >>> reference_path('vep_mount')
    CloudPath('gs://cpg-common-main/references/vep/105.0/mount')
    >>> reference_path('broad/genome_calling_interval_lists')
    CloudPath('gs://cpg-common-main/references/hg38/v0/wgs_calling_regions.hg38.interval_list')

    Assuming config structure as follows:

    ```toml
    [references]
    vep_mount = 'gs://cpg-common-main/references/vep/105.0/mount'
    [references.broad]
    genome_calling_interval_lists = 'gs://cpg-common-main/references/hg38/v0/wgs_calling_regions.hg38.interval_list'
    ```

    Parameters
    ----------
    key : str
        Describes the key within the `references` config section. Can list sections
        separated with '/'.

    Returns
    -------
    str
    """
    return to_path(retrieve(['references'] + key.strip('/').split('/')))


def genome_build() -> str:
    """
    Return the default genome build name
    """
    return retrieve(['references', 'genome_build'], default='GRCh38')


def fasta_res_group(b: hb.Batch, indices: list[str] | None = None):
    """
    Hail Batch resource group for fasta reference files.
    @param b: Hail Batch object.
    @param indices: list of extensions to add to the base fasta file path.
    """
    ref_fasta = get_config()['workflow'].get('ref_fasta') or reference_path(
        'broad/ref_fasta'
    )
    ref_fasta = to_path(ref_fasta)
    d = {
        'base': str(ref_fasta),
        'fai': str(ref_fasta) + '.fai',
        'dict': str(ref_fasta.with_suffix('.dict')),
    }
    if indices:
        for ext in indices:
            d[ext] = f'{ref_fasta}.{ext}'
    return b.read_input_group(**d)


def authenticate_cloud_credentials_in_job(
    job,
    print_all_statements: bool = True,
):
    """
    Takes a hail batch job, activates the appropriate service account

    Once multiple environments are supported this method will decide
    on which authentication method is appropriate

    Parameters
    ----------
    job
        * A hail BashJob
    print_all_statements
        * logging toggle

    Returns
    -------
    None
    """

    # Use "set -x" to print the commands for easier debugging.
    if print_all_statements:
        job.command('set -x')

    # activate the google service account
    job.command(GCLOUD_AUTH_COMMAND)


# commands that declare functions that pull files on an instance,
# handling transitive errors
RETRY_CMD = """\
function fail {
  echo $1 >&2
  exit 1
}

function retry {
  local n_attempts=10
  local delay=30
  local n=1
  while ! eval "$@"; do
    if [[ $n -lt $n_attempts ]]; then
      ((n++))
      echo "Command failed. Attempt $n/$n_attempts after ${delay}s..."
      sleep $delay;
    else
      fail "The command has failed after $n attempts."
    fi
  done
}

function retry_gs_cp {
  src=$1

  if [ -n "$2" ]; then
    dst=$2
  else
    dst=/io/batch/${basename $src}
  fi

  retry gsutil -o GSUtil:check_hashes=never cp $src $dst
}
"""

# command that monitors the instance storage space
MONITOR_SPACE_CMD = 'df -h; du -sh /io; du -sh /io/batch'

ADD_SCRIPT_CMD = """\
cat <<EOT >> {script_name}
{script_contents}
EOT\
"""


def command(
    cmd: Union[str, List[str]],
    monitor_space: bool = False,
    setup_gcp: bool = False,
    define_retry_function: bool = False,
    rm_leading_space: bool = True,
    python_script_path: Optional[Path] = None,
) -> str:
    """
    Wraps a command for Batch.

    @param cmd: command to wrap (can be a list of commands)
    @param monitor_space: add a background process that checks the instance disk
        space every 5 minutes and prints it to the screen
    @param setup_gcp: authenticate on GCP
    @param define_retry_function: when set, adds bash functions `retry` that attempts
        to redo a command after every 30 seconds (useful to pull inputs
        and get around GoogleEgressBandwidth Quota or other google quotas)
    @param rm_leading_space: remove all leading spaces and tabs from the command lines
    @param python_script_path: if provided, copy this python script into the command
    """
    if isinstance(cmd, list):
        cmd = '\n'.join(cmd)

    if define_retry_function:
        setup_gcp = True

    cmd = f"""\
    set -o pipefail
    set -ex
    {GCLOUD_AUTH_COMMAND if setup_gcp else ''}
    {RETRY_CMD if define_retry_function else ''}

    {f'(while true; do {MONITOR_SPACE_CMD}; sleep 600; done) &'
    if monitor_space else ''}

    {{copy_script_cmd}}

    {cmd}

    {MONITOR_SPACE_CMD if monitor_space else ''}
    """

    if rm_leading_space:
        # remove any leading spaces and tabs
        cmd = '\n'.join(line.strip() for line in cmd.split('\n'))
        # remove stretches of spaces
        cmd = '\n'.join(' '.join(line.split()) for line in cmd.split('\n'))
    else:
        # Remove only common leading space:
        cmd = textwrap.dedent(cmd)

    # We don't want the python script tabs to be stripped, so
    # we are inserting it after leading space is removed
    if python_script_path:
        with python_script_path.open() as f:
            script_contents = f.read()
        cmd = cmd.replace(
            '{copy_script_cmd}',
            ADD_SCRIPT_CMD.format(
                script_name=python_script_path.name,
                script_contents=script_contents,
            ),
        )
    else:
        cmd = cmd.replace('{copy_script_cmd}', '')

    return cmd


def query_command(
    module,
    func_name: str,
    *func_args,
    setup_gcp: bool = False,
    setup_hail: bool = True,
    packages: Optional[List[str]] = None,
) -> str:
    """
    Construct a command to run a python function inside a Hail Batch job.
    If hail_billing_project is provided, Hail Query will be also initialised.

    Run a Python Hail Query function inside a Hail Batch job.
    Constructs a command string to use with job.command().
    If hail_billing_project is provided, Hail Query will be initialised.
    """
    init_hail_code = """
from cpg_utils.hail_batch import init_batch
init_batch()
"""

    python_code = f"""
{'' if not setup_hail else init_hail_code}
{inspect.getsource(module)}
{func_name}{func_args}
"""

    return f"""\
set -o pipefail
set -ex
{GCLOUD_AUTH_COMMAND if setup_gcp else ''}

{('pip3 install ' + ' '.join(packages)) if packages else ''}

cat << EOT >> script.py
{python_code}
EOT
python3 script.py
"""


def start_query_context(
    query_backend: Literal['spark', 'batch', 'local', 'spark_local'] | None = None,
    log_path: str | None = None,
    dataset: str | None = None,
    billing_project: str | None = None,
):
    """
    Start Hail Query context, depending on the backend class specified in
    the hail/query_backend TOML config value.
    """
    query_backend = query_backend or get_config().get('hail', {}).get(
        'query_backend', 'spark'
    )
    if query_backend == 'spark':
        hl.init(default_reference=genome_build())
    elif query_backend == 'spark_local':
        local_threads = 2  # https://stackoverflow.com/questions/32356143/what-does-setmaster-local-mean-in-spark
        hl.init(
            default_reference=genome_build(),
            master=f'local[{local_threads}]',  # local[2] means "run spark locally with 2 threads"
            quiet=True,
            log=log_path or dataset_path('hail-log.txt', category='tmp'),
        )
    elif query_backend == 'local':
        hl.utils.java.Env.hc()  # force initialization
    else:
        assert query_backend == 'batch'
        if hl.utils.java.Env._hc:  # pylint: disable=W0212
            return  # already initialised
        dataset = dataset or get_config()['workflow']['dataset']
        billing_project = billing_project or get_config()['hail']['billing_project']

        asyncio.get_event_loop().run_until_complete(
            hl.init_batch(
                billing_project=billing_project,
                remote_tmpdir=f'gs://cpg-{dataset}-hail/batch-tmp',
                token=os.environ.get('HAIL_TOKEN'),
                default_reference='GRCh38',
            )
        )


def cpg_namespace(access_level) -> str:
    """
    Get storage namespace from the access level.
    """
    return 'test' if access_level == 'test' else 'main'
