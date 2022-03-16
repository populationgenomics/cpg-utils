"""Convenience functions related to Hail."""

import asyncio
import os
import hail as hl
import hailtop.batch as hb


def init_query_service():
    """Initializes the Hail Query Service from within Hail Batch.

    Requires the HAIL_BILLING_PROJECT and HAIL_BUCKET environment variables to be set."""

    billing_project = os.getenv('HAIL_BILLING_PROJECT')
    assert billing_project
    return asyncio.get_event_loop().run_until_complete(
        hl.init_service(
            default_reference='GRCh38',
            billing_project=billing_project,
            remote_tmpdir=remote_tmpdir(),
        )
    )


def copy_common_env(job: hb.job.Job) -> None:
    """Copies common environment variables that we use to run Hail jobs.

    These variables are typically set up in the analysis-runner driver, but need to be
    passed through for "batch-in-batch" use cases.

    The environment variable values are extracted from the current process and
    copied to the environment dictionary of the given Hail Batch job."""

    for key in (
        'DRIVER_IMAGE',
        'DATASET',
        'ACCESS_LEVEL',
        'HAIL_BILLING_PROJECT',
        'HAIL_BUCKET',
        'HAIL_JAR_URL',
        'HAIL_SHA',
        'DATASET_GCP_PROJECT',
        'OUTPUT',
    ):
        val = os.getenv(key)
        if val:
            job.env(key, val)


def remote_tmpdir() -> str:
    """Returns the remote_tmpdir to use for Hail initialization.

    Requires the HAIL_BUCKET environment variable to be set."""

    hail_bucket = os.getenv('HAIL_BUCKET')
    assert hail_bucket
    return f'gs://{hail_bucket}/batch-tmp'
