"""Convenience functions related to Hail."""

import asyncio
import os
import hail as hl


def init_query_service():
    """Initializes the Hail Query Service from within Hail Batch.

    Requires the HAIL_BILLING_PROJECT and HAIL_BUCKET environment variables to be set."""

    billing_project = os.getenv('HAIL_BILLING_PROJECT')
    assert billing_project
    hail_bucket = os.getenv('HAIL_BUCKET')
    assert hail_bucket
    return asyncio.get_event_loop().run_until_complete(
        hl.init_service(
            default_reference='GRCh38',
            billing_project=billing_project,
            remote_tmpdir=f'gs://{hail_bucket}/batch-tmp',
        )
    )
