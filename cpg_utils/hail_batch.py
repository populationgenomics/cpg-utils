"""Convenience functions related to Hail."""

import asyncio
import inspect
import os
import tempfile
import textwrap
import typing
from enum import Enum
from typing import Optional, List, Union
from abc import ABC, abstractmethod

import hail as hl
import hailtop.batch as hb
from hail.utils.java import Env

from cpg_utils.config import get_config
from cpg_utils import to_path, Path


# template commands strings
GCLOUD_AUTH_COMMAND = """\
export GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json
gcloud -q auth activate-service-account \
--key-file=$GOOGLE_APPLICATION_CREDENTIALS
"""


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


def remote_tmpdir(hail_bucket: Optional[str] = None) -> str:
    """Returns the remote_tmpdir to use for Hail initialization.

    If `hail_bucket` is not specified explicitly, requires the `hail/bucket` config variable to be set.
    """
    bucket = hail_bucket or get_config().get('hail', {}).get('bucket')
    assert bucket, f'hail_bucket was not set by argument or configuration'
    return f'gs://{bucket}/batch-tmp'


def dataset_path(
    suffix: str,
    category: str | None = None,
    dataset: str | None = None,
    access_level: str | None = None,
    path_scheme: str | None = None,
) -> str:
    """
    Returns a full path for the current dataset, given a category and path suffix.

    This is useful for specifying input files, as in contrast to the output_path
    function, dataset_path does _not_ take the `workflow/output_prefix` config variable
    into account.

    Path schemes can be customised using the `path_schemes` section in the config.
    See the `config-template.toml` file for examples and default values.

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test`:

    >>> from cpg_utils.hail_batch import dataset_path
    >>> dataset_path('1kg_densified/combined.mt')
    'gs://cpg-fewgenomes-test/1kg_densified/combined.mt'
    >>> dataset_path('1kg_densified/report.html', 'web')
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
        A category like "upload", "tmp", "web". Corresponds to the keys in your
        path_scheme configuration (see config-template.toml for examples); defaults
        to the "default" key if ommited.
    dataset : str, optional
        Dataset name, takes precedence over the `workflow/dataset` config variable
    access_level : str, optional
        Access level, takes precedence over the `workflow/access_level` config variable
    path_scheme: str, optional
        Cloud storage path scheme, takes precedence over the `workflow/path_scheme`
        config variable.

    Returns
    -------
    str
    """
    config = get_config()
    dataset = dataset or config['workflow'].get('dataset')
    access_level = access_level or config['workflow'].get('access_level')
    if not (dataset and access_level):
        raise ValueError(
            'workflow/dataset and workflow/access_level must be defined in config. '
            'Make sure you are running your application through the analysis-runner, '
            'or set required values in your configuration file provided by '
            'CPG_CONFIG_PATH.'
        )
    scheme_by_category = config['path_schemes'][
        path_scheme or config['workflow'].get('path_scheme', 'google_storage')
    ]
    scheme = scheme_by_category.get(category) or scheme_by_category['default']
    namespace = 'test' if access_level == 'test' else 'main'
    prefix = scheme.format(**{'dataset': dataset, 'main/test': namespace})
    return os.path.join(prefix, suffix)


def web_url(suffix: str = '') -> str:
    """
    Web URL to match the dataset_path of category 'web'.
    """
    return dataset_path(suffix=suffix, category='web_url')


def output_path(suffix: str, category: Optional[str] = None) -> str:
    """Returns a full path for the given category and path suffix.

    In contrast to the `dataset_path` function, output_path takes the
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

    Assuming with the default `cpg_google_storage` path_scheme in config-template.toml.

    Notes
    -----
    Requires the `workflow/output_prefix` config variable to be set, in addition to the
    requirements for `dataset_path`.

    Parameters
    ----------
    suffix : str
        A path suffix to append to the bucket + output directory.
    category : str, optional
        A category like "upload", "tmp", "web". Corresponds to the keys in your
        path_scheme configuration (see config-template.toml for examples); defaults
        to the "default" key if ommited.

    Returns
    -------
    str
    """
    return dataset_path(
        os.path.join(get_config()['workflow']['output_prefix'], suffix), category
    )


def image_path(key: str) -> str:
    """Returns a path to a container image in the default registry using the
    key in the config's images section.

    Examples
    --------
    >>> image_path('bcftools')
    'australia-southeast1-docker.pkg.dev/cpg-common/images/bcftools:1.10.2'

    Notes
    -----
    Requires config variables `workflow/image_registry_prefix` and `images/<key>`.

    Parameters
    ----------
    key : str
        Describes the key within the `images` config section.

    Returns
    -------
    str
    """
    suffix = get_config()['images'][key]
    return os.path.join(get_config()['workflow']['image_registry_prefix'], suffix)


def reference_path(key: str) -> Path:
    """Returns a path to a file in the references bucket using the key in
    the config's references section.

    Examples
    --------
    >>> reference_path('vep_mount')
    CloudPath('gs://cpg-reference/vep/105.0/mount')
    >>> reference_path('broad/genome_calling_interval_lists')
    CloudPath('gs://cpg-reference/hg38/v0/wgs_calling_regions.hg38.interval_list')

    Notes
    -----
    Requires the `workflow/reference_prefix` config variable to be set.

    Parameters
    ----------
    key : str
        Describes the key within the `references` config section. Can specify
        nested sections with a "/" separator.

    Returns
    -------
    str
    """
    prefix = to_path(get_config()['workflow']['reference_prefix'])
    d = get_config()['references']
    sections = key.strip('/').split('/')
    for section in sections[:-1]:
        if section not in d:
            raise ValueError(f'No subsection {section} in {str(d)}')
        d = d[section]
        if extra_prefix := d.get('prefix'):
            prefix /= extra_prefix
    suffix = d[sections[-1]]
    return prefix / suffix


def genome_build() -> str:
    """Return the genome build name"""
    return get_config()['references'].get('genome_build', 'GRCh38')


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
    d = dict(
        base=str(ref_fasta),
        fai=str(ref_fasta) + '.fai',
        dict=str(ref_fasta.with_suffix('.dict')),
    )
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
MONITOR_SPACE_CMD = f'df -h; du -sh /io; du -sh /io/batch'

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
    query_backend: typing.Literal['spark', 'batch', 'local', 'spark_local']
    | None = None,
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
