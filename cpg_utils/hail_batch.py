"""Convenience functions related to Hail."""

import asyncio
import inspect
import os
import textwrap
from enum import Enum
from typing import Optional, List
from abc import ABC, abstractmethod

import hail as hl
import hailtop.batch as hb

from cpg_utils.config import get_config
from cpg_utils import to_path, Path


# template commands strings
GCLOUD_AUTH_COMMAND = (
    'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'
)
BASE_CMD = """
import logging
logger = logging.getLogger(__file__)
logging.basicConfig(format='%(levelname)s (%(name)s %(lineno)s): %(message)s')
logger.setLevel(logging.INFO)
"""
HAIL_STARTUP = """
import asyncio
import hail as hl
asyncio.get_event_loop().run_until_complete(
    hl.init_batch(
        default_reference='{default_reference}',
        billing_project='{hail_billing_project}',
        remote_tmpdir='{hail_bucket}',
    )
)
"""
CMD_MODULE = """
{source_module}
{func_name}{func_args}
"""
CMD_SCRIPT = """
set -o pipefail
set -ex
{gcloud_auth}

{packages}

cat << EOT >> script.py
{command}
EOT
python3 script.py
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
    return asyncio.get_event_loop().run_until_complete(
        hl.init_batch(
            default_reference=genome_build(),
            billing_project=get_config()['hail']['billing_project'],
            remote_tmpdir=remote_tmpdir(),
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
    for key in ('CPG_CONFIG_PATH', 'HAIL_TOKEN'):
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


class PathScheme(ABC):
    """
    Cloud storage path scheme. Constructs full paths to buckets and files.
    """

    @abstractmethod
    def path_prefix(self, dataset: str, category: str) -> str:
        """Build path prefix used in dataset_path"""

    @abstractmethod
    def full_path(self, prefix: str, suffix: str) -> str:
        """Build full path from prefix and suffix"""

    @staticmethod
    def parse(val: str) -> 'PathScheme':
        """Parse subclass name from string"""
        match val:
            case 'gs':
                return GSPathScheme()
            case 'hail-az':
                return AzurePathScheme()
            case _:
                raise ValueError(
                    f'Unsupported path format: {val}. Available: gs, hail-az'
                )


class GSPathScheme(PathScheme):
    """
    Google Cloud Storage path scheme.
    """

    def __init__(self):
        self.scheme = 'gs'
        self.prefix = 'cpg'

    def path_prefix(self, dataset: str, category: str) -> str:
        """Build path prefix used in dataset_path"""
        return f'{self.prefix}-{dataset}-{category}'

    def full_path(self, prefix: str, suffix: str) -> str:
        """Build full path from prefix and suffix"""
        return os.path.join(f'{self.scheme}://', prefix, suffix)


class AzurePathScheme(PathScheme):
    """
    Azure Blob Storage path scheme, following the Hail Batch hail-az format.
    """

    def __init__(self, account: Optional[str] = 'cpg'):
        config = get_config()
        self.scheme = 'hail-az'
        self.account = config['workflow'].get('azure_account', account)

    def path_prefix(self, dataset: str, category: str) -> str:
        """Build path prefix used in dataset_path"""
        return f'{self.account}/{dataset}-{category}'

    def full_path(self, prefix: str, suffix: str) -> str:
        """Build full path from prefix and suffix"""
        return os.path.join(f'{self.scheme}://', prefix, suffix)


class Namespace(Enum):
    """
    Storage namespace.
    https://github.com/populationgenomics/team-docs/tree/main/storage_policies#main-vs-test
    """

    MAIN = 'main'
    TEST = 'test'

    @staticmethod
    def from_access_level(val: str) -> 'Namespace':
        """
        Parse value from a access level string.
        >>> Namespace.from_access_level('test')
        Namespace.TEST
        >>> Namespace.from_access_level('standard')
        Namespace.MAIN
        >>> Namespace.from_access_level('main')
        Namespace.MAIN
        """
        for val, str_vals in {
            Namespace.MAIN: ['main', 'standard', 'full'],
            Namespace.TEST: ['test'],
        }.items():
            if val in str_vals:
                return val
        raise ValueError(f'Cannot parse namespace or access level {val}')


def dataset_path(
    suffix: str = '',
    category: Optional[str] = None,
    dataset: Optional[str] = None,
    access_level: Optional[str] = None,
    path_scheme: Optional[str] = None,
) -> str:
    """
    Returns a full path for the current dataset, given a category and path suffix.

    This is useful for specifying input files, as in contrast to the output_path
    function, dataset_path does _not_ take the `workflow/output_prefix` config variable
    into account.

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test --output 1kg_pca/v42`:

    >>> from cpg_utils.hail_batch import dataset_path
    >>> dataset_path('1kg_densified/combined.mt')
    'gs://cpg-fewgenomes-test/1kg_densified/combined.mt'
    >>> dataset_path('1kg_densified/report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'
    >>> dataset_path('1kg_densified/report.html', path_scheme='hail-az')
    'hail-az://cpg/fewgenomes-test/1kg_densified/report.html'

    Notes
    -----
    Requires either the
    * `workflow/dataset` and `workflow/access_level` config variables, or the
    * `workflow/dataset_path` config variable
    to be set, where the former takes precedence.

    Parameters
    ----------
    suffix : str
        A path suffix to append to the bucket.
    category : str, optional
        A category like "upload", "tmp", "web". If omitted, defaults to the "main" and
        "test" buckets based on the access level. See
        https://github.com/populationgenomics/team-docs/tree/main/storage_policies
        for a full list of categories and their use cases.
    dataset : str, optional
        Dataset name, takes precedence over the `workflow/dataset` config variable
    access_level : str, optional
        Access level, takes precedence over the `workflow/access_level` config variable
    path_scheme: str, optional
        Cloud storage path scheme, takes precedence over the `workflow/path_scheme`
        config variable

    Returns
    -------
    str
    """
    config = get_config()
    dataset = dataset or config['workflow'].get('dataset')
    access_level = access_level or config['workflow'].get('access_level')
    path_scheme = path_scheme or config['workflow'].get('path_scheme', 'gs')

    if dataset and access_level:
        namespace = Namespace.from_access_level(access_level)
        if category is None:
            category = namespace.value
        elif category not in ('archive', 'upload'):
            category = f'{namespace.value}-{category}'
        prefix = PathScheme.from_access_level(path_scheme).path_prefix(dataset, category)
    else:
        prefix = config['workflow']['dataset_path']

    return PathScheme.from_access_level(path_scheme).full_path(prefix, suffix)


def web_url(
    suffix: str = '',
    dataset: Optional[str] = None,
    access_level: Optional[str] = None,
) -> str:
    """Returns URL corresponding to a dataset path of category 'web',
    assuming other arguments are the same.
    """
    config = get_config()
    dataset = dataset or config['workflow'].get('dataset')
    access_level = access_level or config['workflow'].get('access_level')
    namespace = Namespace.from_access_level(access_level)
    web_url_template = config['workflow'].get('web_url_template')
    try:
        url = web_url_template.format(dataset=dataset, namespace=namespace.value)
    except KeyError as e:
        raise ValueError(
            f'`workflow/web_url_template` should be parametrised by "dataset" and '
            f'"namespace" in curly braces, for example: '
            f'https://{{namespace}}-web.populationgenomics.org.au/{{dataset}}. '
            f'Got: {web_url_template}'
        ) from e
    return os.path.join(url, suffix)


def output_path(suffix: str, category: Optional[str] = None) -> str:
    """Returns a full path for the given category and path suffix.

    In contrast to the dataset_path function, output_path takes the `workflow/output_prefix`
    config variable into account.

    Examples
    --------
    If using the analysis-runner, the `workflow/output_prefix` would be set to the argument
    provided using the --output argument, e.g.
    `--dataset fewgenomes --access-level test --output 1kg_pca/v42`:
    will use '1kg_pca/v42' as the base path to build upon in this method

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
        A category like "upload", "tmp", "web". If omitted, defaults to the "main" and
        "test" buckets based on the access level. See
        https://github.com/populationgenomics/team-docs/tree/main/storage_policies
        for a full list of categories and their use cases.

    Returns
    -------
    str
    """
    return dataset_path(
        os.path.join(get_config()['workflow']['output_prefix'], suffix), category
    )


def image_path(suffix: str = '') -> str:
    """Returns a full path to a container image in the default registry.

    Examples
    --------
    >>> image_path('bcftools:1.10.2')
    'australia-southeast1-docker.pkg.dev/cpg-common/images/bcftools:1.10.2'

    Notes
    -----
    Requires the `workflow/image_registry_prefix` config variable to be set.

    Parameters
    ----------
    suffix : str
        Describes the location within the registry, or key in the `images` config
        section.

    Returns
    -------
    str
    """
    if (d := get_config().get('images')) and suffix and suffix in d:
        suffix = d[suffix]
    return os.path.join(get_config()['workflow']['image_registry_prefix'], suffix)


def reference_path(suffix: str = '', section: Optional[str] = None) -> Path:
    """Returns a full path to a reference file.

    Examples
    --------
    >>> reference_path('hg38/v0/wgs_calling_regions.hg38.interval_list')
    'gs://cpg-reference/hg38/v0/wgs_calling_regions.hg38.interval_list'

    Notes
    -----
    Requires the `workflow/reference_prefix` config variable to be set.

    Parameters
    ----------
    suffix : str
        Describes path relative to the reference prefix. Can be a key in the
        `references` config section.
    section : str, optional
        Subsection name inside the references config section.

    Returns
    -------
    str
    """
    prefix = to_path(get_config()['workflow']['reference_prefix'])

    if d := get_config().get('references'):
        if section:
            if section not in d:
                raise ValueError(
                    f'No subsection {section} in the "references" config section'
                )
            d = d[section]
            if extra_prefix := d.get('prefix'):
                prefix /= extra_prefix
        if suffix and suffix in d:
            suffix = d[suffix]

    # A leading slash results in the prefix being ignored, therefore use strip below.
    suffix = suffix.strip('/')
    return prefix / suffix


def genome_build() -> str:
    """Return the genome build name"""
    return get_config()['references'].get('genome_build', 'GRCh38')


def fasta_res_group(b, indices: list | None = None):
    """
    Hail Batch resource group for fasta reference files.
    @param b: Hail Batch object.
    @param indices: list of extensions to add to the base fasta file path.
    """
    ref_fasta = reference_path('ref_fasta', section='broad')
    d = dict(
        base=str(ref_fasta),
        fai=str(ref_fasta) + '.fai',
        dict=str(ref_fasta.with_suffix('.dict')),
    )
    if indices:
        d |= {ext: f'{ref_fasta}.{ext}' for ext in indices}
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


def query_command(
    module,
    func_name: str,
    *func_args,
    setup_gcp: bool = False,
    hail_billing_project: Optional[str] = None,
    hail_bucket: Optional[str] = None,
    default_reference: str = 'GRCh38',
    packages: Optional[List[str]] = None,
) -> str:
    """
    Run a Python Hail Query function inside a Hail Batch job.
    Constructs a command string to use with job.command().
    If hail_billing_project is provided, Hail Query will be initialised.
    """
    python_cmd = BASE_CMD
    if hail_billing_project:
        assert hail_bucket
        python_cmd += HAIL_STARTUP.format(
            default_reference=default_reference,
            hail_billing_project=hail_billing_project,
            hail_bucket=hail_bucket,
        )
    python_cmd += CMD_MODULE.format(
        source_module=textwrap.dedent(inspect.getsource(module)),
        func_name=func_name,
        func_args=func_args,
    )

    return CMD_SCRIPT.format(
        gcloud_auth=GCLOUD_AUTH_COMMAND if setup_gcp else '',
        packages=('pip3 install ' + ' '.join(packages)) if packages else '',
        python_cmd=python_cmd,
    )
