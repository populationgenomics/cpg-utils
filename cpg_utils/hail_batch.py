"""Convenience functions related to Hail."""

import asyncio
import inspect
import logging
import os
import tempfile
import textwrap
import uuid
from shlex import quote
from typing import Any, Literal

import toml
from deprecated import deprecated

import hail as hl
import hailtop.batch as hb
from hail.backend.service_backend import ServiceBackend as InternalServiceBackend
from hail.utils.java import Env
from hailtop.config import get_deploy_config

from cpg_utils import Path, to_path
from cpg_utils.config import (
    AR_GUID_NAME,
    config_retrieve,
    dataset_path,
    genome_build,
    get_config,
    set_config_paths,
    try_get_ar_guid,
)
from cpg_utils.config import (
    reference_path as ref_path,
)
from cpg_utils.constants import DEFAULT_GITHUB_ORGANISATION

# template commands strings
GCLOUD_AUTH_COMMAND = """\
export GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json
gcloud -q auth activate-service-account \
--key-file=$GOOGLE_APPLICATION_CREDENTIALS
"""


_batch: 'Batch | None' = None


def reset_batch():
    """Reset the global batch reference, useful for tests"""
    global _batch  # pylint: disable=global-statement
    _batch = None


def get_batch(
    name: str | None = None,
    *,
    default_python_image: str | None = None,
    attributes: dict[str, str] | None = None,
    **kwargs: Any,
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
        _backend = config_retrieve(['hail', 'backend'], default='batch')
        if _backend == 'local':
            logging.info('Initialising Hail Batch with local backend')
            backend = hb.LocalBackend(
                tmp_dir=tempfile.mkdtemp('batch-tmp'),
            )
        else:
            logging.info('Initialising Hail Batch with service backend')
            backend = hb.ServiceBackend(
                billing_project=config_retrieve(['hail', 'billing_project']),
                remote_tmpdir=dataset_path('batch-tmp', category='tmp'),
                token=os.environ.get('HAIL_TOKEN'),
            )
        _batch = Batch(
            name=name or config_retrieve(['workflow', 'name'], default=None),
            backend=backend,
            pool_label=config_retrieve(['hail', 'pool_label'], default=None),
            cancel_after_n_failures=config_retrieve(
                ['hail', 'cancel_after_n_failures'],
                default=None,
            ),
            default_timeout=config_retrieve(['hail', 'default_timeout'], default=None),
            default_memory=config_retrieve(['hail', 'default_memory'], default=None),
            default_python_image=default_python_image
            or config_retrieve(['workflow', 'driver_image']),
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
        name: str,
        backend: hb.backend.LocalBackend | hb.backend.ServiceBackend,
        *,
        pool_label: str | None = None,
        attributes: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        _attributes = attributes or {}
        if AR_GUID_NAME not in _attributes:  # noqa: SIM102
            if ar_guid := try_get_ar_guid():
                _attributes[AR_GUID_NAME] = ar_guid

        super().__init__(name, backend, attributes=_attributes, **kwargs)
        # Job stats registry:
        self.job_by_label: dict = {}
        self.job_by_stage: dict = {}
        self.job_by_tool: dict = {}
        self.total_job_num = 0
        self.pool_label = pool_label
        dry_run = config_retrieve(['hail', 'dry_run'], default=False)
        if not dry_run and not isinstance(self._backend, hb.LocalBackend):
            self._copy_configs_to_remote()

    def _copy_configs_to_remote(self) -> None:
        """
        Combine all config files into a single entry
        Write that entry to a cloud path
        Set that cloud path as the config path

        This is crucial in production-pipelines as we combine remote
        and local files in the driver image, but we can only pass
        cloudpaths to the worker job containers
        """
        if not isinstance(self._backend, hb.backend.ServiceBackend):
            return

        remote_dir = to_path(self._backend.remote_tmpdir) / 'config'
        config_path = remote_dir / (str(uuid.uuid4()) + '.toml')
        with config_path.open('w') as f:
            toml.dump(dict(get_config()), f)
        set_config_paths([str(config_path)])

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

        attributes['sequencing_groups'] = sorted(sequencing_groups)
        fixed_attrs = {k: str(v) for k, v in attributes.items()}
        return name, fixed_attrs

    def run(self, **kwargs: Any):
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
            return None

        for job in self._jobs:
            job.name, job.attributes = self._process_job_attributes(
                job.name,
                job.attributes,
            )
            # We only have dedicated pools for preemptible machines.
            # _preemptible defaults to None, so check explicitly for False.
            # pylint: disable=W0212
            if self.pool_label and job._preemptible is not False:
                job._pool_label = self.pool_label
            copy_common_env(job)

        logging.info(f'Will submit {self.total_job_num} jobs')

        def _print_stat(
            prefix: str,
            _d: dict,
            default_label: str | None = None,
        ) -> None:
            m = (prefix or ' ') + '\n'
            for label, stat in _d.items():
                lbl = label or default_label
                msg = f'{stat["job_n"]} job'
                if stat['job_n'] > 1:
                    msg += 's'
                if (sg_count := len(stat['sequencing_groups'])) > 0:
                    msg += f' for {sg_count} sequencing group'
                    if sg_count > 1:
                        msg += 's'
                m += f'  {lbl}: {msg}'
            logging.info(m)

        _print_stat(
            'Split by stage:',
            self.job_by_stage,
            default_label='<not in stage>',
        )
        _print_stat(
            'Split by tool:',
            self.job_by_tool,
            default_label='<tool is not defined>',
        )

        kwargs.setdefault('dry_run', config_retrieve(['hail', 'dry_run'], default=None))
        kwargs.setdefault(
            'delete_scratch_on_exit',
            config_retrieve(['hail', 'delete_scratch_on_exit'], default=None),
        )
        # Local backend does not support "wait"
        if isinstance(self._backend, hb.LocalBackend) and 'wait' in kwargs:
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


_default_override_revision = None


class DefaultOverrideServiceBackend(InternalServiceBackend):
    @property
    def jar_spec(self) -> dict:
        return {'type': 'git_revision', 'value': _default_override_revision}


def init_batch(**kwargs: Any):
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
    dataset = config_retrieve(['workflow', 'dataset'])
    kwargs.setdefault('token', os.environ.get('HAIL_TOKEN'))
    asyncio.get_event_loop().run_until_complete(
        hl.init_batch(
            default_reference=genome_build(),
            billing_project=config_retrieve(['hail', 'billing_project']),
            remote_tmpdir=remote_tmpdir(f'cpg-{dataset}-hail'),
            **kwargs,
        ),
    )

    if revision := config_retrieve(['workflow', 'default_jar_spec_revision'], False):
        global _default_override_revision
        _default_override_revision = revision
        backend = Env.backend()
        if isinstance(backend, InternalServiceBackend):
            backend.__class__ = DefaultOverrideServiceBackend


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

    if not job.attributes:
        job.attributes = {}

    ar_guid = try_get_ar_guid()
    if ar_guid:
        job.attributes[AR_GUID_NAME] = ar_guid


def remote_tmpdir(hail_bucket: str | None = None) -> str:
    """Returns the remote_tmpdir to use for Hail initialization.

    If `hail_bucket` is not specified explicitly, requires the `hail/bucket` config variable to be set.
    """
    bucket = hail_bucket or config_retrieve(['hail', 'bucket'], default=None)
    assert bucket, 'hail_bucket was not set by argument or configuration'
    return f'gs://{bucket}/batch-tmp'


def fasta_res_group(b: hb.Batch, indices: list[str] | None = None):
    """
    Hail Batch resource group for fasta reference files.
    @param b: Hail Batch object.
    @param indices: list of extensions to add to the base fasta file path.
    """

    ref_fasta = config_retrieve(['workflow', 'ref_fasta'], default=None)
    if not ref_fasta:
        ref_fasta = ref_path('broad/ref_fasta')

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
    job: hb.batch.job.BashJob,
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


def prepare_git_job(
    job: hb.batch.job.BashJob,
    repo_name: str,
    commit: str,
    organisation: str = DEFAULT_GITHUB_ORGANISATION,
    is_test: bool = True,
    print_all_statements: bool = True,
    get_deploy_token: bool = True,
):
    """
    Takes a hail batch job, and:
        * Clones the repository
            * if access_level != "test": check the desired commit is on 'main'
        * Check out the specific commit

    Parameters
    ----------
    job                     - A hail BashJob
    organisation            - The GitHub individual or organisation
    repo_name               - The repository name to check out
    commit                  - The commit hash to check out
    is_test                 - CPG specific: only Main commits can run on Main data
    print_all_statements    - logging toggle

    Returns
    -------
    No return required
    """
    authenticate_cloud_credentials_in_job(
        job,
        print_all_statements=print_all_statements,
    )

    # Note: for private GitHub repos we'd need to use a token to clone.
    #   - store the token on secret manager
    #   - The git_credentials_secret_{name,project} values are set by cpg-infrastructure
    #   - check at runtime whether we can get the token
    #   - if so, set up the git credentials store with that value
    if get_deploy_token:
        job.command(
            """
# get secret names from config if they exist
secret_name=$(python3 -c '
try:
    from cpg_utils.config import config_retrieve
    print(config_retrieve(["infrastructure", "git_credentials_secret_name"], default=""))
except:
    pass
' || echo '')

secret_project=$(python3 -c '
try:
    from cpg_utils.config import config_retrieve
    print(config_retrieve(["infrastructure", "git_credentials_secret_project"], default=""))
except:
    pass
' || echo '')

if [ ! -z "$secret_name" ] && [ ! -z "$secret_project" ]; then
    # configure git credentials store if credentials are set
    gcloud --project $secret_project secrets versions access --secret $secret_name latest > ~/.git-credentials
    git config --global credential.helper "store"
else
    echo 'No git credentials secret found, unable to check out private repositories.'
fi
        """,
        )

    # Any job commands here are evaluated in a bash shell, so user arguments should
    # be escaped to avoid command injection.
    repo_path = f'https://github.com/{organisation}/{repo_name}.git'
    job.command(f'git clone --recurse-submodules {quote(repo_path)}')
    job.command(f'cd {quote(repo_name)}')
    # Except for the "test" access level, we check whether commits have been
    # reviewed by verifying that the given commit is in the main branch.
    if not is_test:
        job.command('git checkout main')
        job.command(
            f'git merge-base --is-ancestor {quote(commit)} HEAD || '
            '{ echo "error: commit not merged into main branch"; exit 1; }',
        )
    job.command(f'git checkout {quote(commit)}')
    job.command('git submodule update')

    return job


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
cat <<'EOT' >> {script_name}
{script_contents}
EOT\
"""


def command(
    cmd: str | list[str],
    monitor_space: bool = False,
    setup_gcp: bool = False,
    define_retry_function: bool = False,
    rm_leading_space: bool = True,
    python_script_path: Path | None = None,
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
    module: Any,
    func_name: str,
    *func_args: Any,
    setup_gcp: bool = False,
    setup_hail: bool = True,
    packages: list[str] | None = None,
    init_batch_args: dict[str, str | int] | None = None,
) -> str:
    """
    Construct a command to run a python function inside a Hail Batch job.
    If hail_billing_project is provided, Hail Query will be also initialised.

    Run a Python Hail Query function inside a Hail Batch job.
    Constructs a command string to use with job.command().
    If hail_billing_project is provided, Hail Query will be initialised.

    init_batch_args can be used to pass additional arguments to init_batch.
    this is a dict of args, which will be placed into the batch initiation command
    e.g. {'worker_memory': 'highmem'} -> 'init_batch(worker_memory="highmem")'
    """

    # translate any input arguments into an embeddable String
    if init_batch_args:
        batch_overrides = ', '.join(f'{k}={v!r}' for k, v in init_batch_args.items())
    else:
        batch_overrides = ''

    init_hail_code = f"""
from cpg_utils.hail_batch import init_batch
init_batch({batch_overrides})
"""

    # the code will be copied verbatim
    python_code = f"""
{'' if not setup_hail else init_hail_code}
{inspect.getsource(module)}
"""

    # but the function call will be shell-expanded, as the arguments may
    # contain variables requiring expansion, ${BATCH_TMPDIR} in particular
    python_call = f"""
{func_name}{func_args}
"""

    return f"""\
set -o pipefail
set -ex
{GCLOUD_AUTH_COMMAND if setup_gcp else ''}

{('pip3 install ' + ' '.join(packages)) if packages else ''}

cat <<'EOT' > script.py
{python_code}
EOT
cat <<EOT >> script.py
{python_call}
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
    query_backend = query_backend or config_retrieve(
        ['hail', 'query_backend'],
        default='spark',
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
        dataset = dataset or config_retrieve(['workflow', 'dataset'])
        billing_project = billing_project or config_retrieve(
            ['hail', 'billing_project'],
        )

        asyncio.get_event_loop().run_until_complete(
            hl.init_batch(
                billing_project=billing_project,
                remote_tmpdir=f'gs://cpg-{dataset}-hail/batch-tmp',
                token=os.environ.get('HAIL_TOKEN'),
                default_reference='GRCh38',
            ),
        )


def run_batch_job_and_print_url(
    batch: Batch,
    wait: bool,
    environment: str,
) -> str | None:
    """Call batch.run(), return the URL, and wait for job to  finish if wait=True"""
    if not environment == 'gcp':
        raise ValueError(
            f'Unsupported Hail Batch deploy config environment: {environment}',
        )
    bc_batch = batch.run(wait=False)

    if not bc_batch:
        return None

    deploy_config = get_deploy_config()
    url = deploy_config.url('batch', f'/batches/{bc_batch.id}')

    if wait:
        status = bc_batch.wait()
        if status['state'] != 'success':
            raise Exception(f'{url} failed')

    return url


# these methods were removed from this location, put in config


@deprecated('Use cpg_utils.config.image_path instead')
def image_path(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.config import image_path as _image_path

    return _image_path(*args, **kwargs)


@deprecated('Use cpg_utils.config.output_path instead')
def output_path(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.config import output_path as _output_path

    return _output_path(*args, **kwargs)


@deprecated('Use cpg_utils.config.web_url instead')
def web_url(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.config import web_url as _web_url

    return _web_url(*args, **kwargs)


# cpg_test_dataset_path
@deprecated('Use cpg_utils.config.dataset_path instead')
def cpg_test_dataset_path(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.config import cpg_test_dataset_path as _cpg_test_dataset_path

    return _cpg_test_dataset_path(*args, **kwargs)


@deprecated(
    'Use to_path(cpg_utils.config.reference_path) instead, note the '
    'config.reference_path does not return an AnyPath object',
)
def reference_path(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.config import reference_path as _reference_path

    return to_path(_reference_path(*args, **kwargs))


@deprecated('Use cpg_utils.config.get_cpg_namespace instead')
def cpg_namespace(*args, **kwargs):  # noqa: ANN002, ANN003
    from cpg_utils.config import get_cpg_namespace as _cpg_namespace

    return _cpg_namespace(*args, **kwargs)
