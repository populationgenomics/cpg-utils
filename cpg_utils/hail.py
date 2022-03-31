"""Convenience functions related to Hail."""

import asyncio
import os
from typing import Optional
import hail as hl
import hailtop.batch as hb


def init_batch(**kwargs):
    """Initializes the Hail Query Service from within Hail Batch.

    Requires the HAIL_BILLING_PROJECT and HAIL_BUCKET environment variables to be set.

    Parameters
    ----------
    kwargs : keyword arguments
        Forwarded directly to `hl.init_batch`.
    """

    billing_project = os.getenv('HAIL_BILLING_PROJECT')
    assert billing_project
    return asyncio.get_event_loop().run_until_complete(
        hl.init_batch(
            default_reference='GRCh38',
            billing_project=billing_project,
            remote_tmpdir=remote_tmpdir(),
            **kwargs,
        )
    )


def copy_common_env(job: hb.job.Job) -> None:
    """Copies common environment variables that we use to run Hail jobs.

    These variables are typically set up in the analysis-runner driver, but need to be
    passed through for "batch-in-batch" use cases.

    The environment variable values are extracted from the current process and
    copied to the environment dictionary of the given Hail Batch job."""

    for key in (
        'CPG_ACCESS_LEVEL',
        'CPG_DATASET',
        'CPG_DATASET_GCP_PROJECT',
        'CPG_DATASET_PATH',
        'CPG_DRIVER_IMAGE',
        'CPG_OUTPUT_PREFIX',
        'HAIL_BILLING_PROJECT',
        'HAIL_BUCKET',
    ):
        val = os.getenv(key)
        if val:
            job.env(key, val)


def remote_tmpdir(hail_bucket: Optional[str] = None) -> str:
    """Returns the remote_tmpdir to use for Hail initialization.

    If `hail_bucket` is not specified explicitly, requires the HAIL_BUCKET environment variable to be set."""

    if not hail_bucket:
        hail_bucket = os.getenv('HAIL_BUCKET')
        assert hail_bucket
    return f'gs://{hail_bucket}/batch-tmp'


def dataset_path(suffix: str, category: Optional[str] = None) -> str:
    """Returns a full path for the current dataset, given a category and path suffix.

    This is useful for specifying input files, as in contrast to the output_path
    function, dataset_path does _not_ take the CPG_OUTPUT_PREFIX environment variable
    into account.

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test --output 1kg_pca/v42`:

    >>> from analysis_runner import bucket_path
    >>> bucket_path('1kg_densified/combined.mt')
    'gs://cpg-fewgenomes-test/1kg_densified/combined.mt'
    >>> bucket_path('1kg_densified/report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'

    Notes
    -----
    Requires either the
    * `CPG_DATASET` and `CPG_ACCESS_LEVEL` environment variables, or the
    * `CPG_DATASET_PATH` environment variable
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

    Returns
    -------
    str
    """
    dataset = os.getenv('CPG_DATASET')
    access_level = os.getenv('CPG_ACCESS_LEVEL')

    if dataset and access_level:
        namespace = 'test' if access_level == 'test' else 'main'
        if category is None:
            category = namespace
        elif category not in ('archive', 'upload'):
            category = f'{namespace}-{category}'
        prefix = f'cpg-{dataset}-{category}'
    else:
        prefix = os.getenv('CPG_DATASET_PATH') or ''  # coerce to str
        assert prefix

    return os.path.join('gs://', prefix, suffix)


def output_path(suffix: str, category: Optional[str] = None) -> str:
    """Returns a full path for the given category and path suffix.

    In contrast to the dataset_path function, output_path takes the CPG_OUTPUT_PREFIX
    environment variable into account.

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test --output 1kg_pca/v42`:

    >>> from analysis_runner import output_path
    >>> output_path('loadings.ht')
    'gs://cpg-fewgenomes-test/1kg_pca/v42/loadings.ht'
    >>> output_path('report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_pca/v42/report.html'

    Notes
    -----
    Requires the `CPG_OUTPUT_PREFIX` environment variable to be set, in addition to the
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
    output = os.getenv('CPG_OUTPUT_PREFIX')
    assert output
    return dataset_path(os.path.join(output, suffix), category)
