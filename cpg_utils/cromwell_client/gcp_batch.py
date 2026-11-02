"""
Workflow options for Cromwell's GCP Batch backend.

This is the only backend-specific part of ``cpg_utils.cromwell_client``. Every value is
passed in explicitly; CPG naming conventions appear only in the examples.
"""

import json
import logging
from collections.abc import Mapping
from typing import Any

from cpg_utils.cloud import read_secret

logger = logging.getLogger(__name__)

COMPUTE_CATEGORY_LABEL = 'compute-category'
AR_GUID_LABEL = 'ar-guid'


def build_workflow_options(
    *,
    service_account_secret: str,
    secret_project: str,
    google_project: str,
    logs_dir: str,
    gcs_root: str,
    outputs_dir: str | None = None,
    labels: Mapping[str, str] | None = None,
    ar_guid: str | None = None,
) -> dict[str, Any]:
    """
    Build Workflow options for Cromwell's GCP Batch backend.

    The service account key is read from GCP Secret Manager, and Cromwell runs the
    Workflow's jobs as that service account.

    Args:
        service_account_secret: name of the secret holding the service account key
            JSON.
        secret_project: GCP project that holds the secret. This can differ from
            ``google_project``.
        google_project: GCP project the Workflow's jobs run (and are billed) in.
        logs_dir: where Cromwell writes call and Workflow logs.
        gcs_root: Cromwell's execution root for intermediate files.
        outputs_dir: if given, Cromwell copies the final Workflow outputs here.
        labels: extra ``google_labels`` attached to the cloud resources.
        ar_guid: if given, added to ``google_labels`` as ``ar-guid``.

    Returns:
        The Workflow options dict, ready to pass to ``CromwellClient.submit`` or
        ``run_workflow``.

    Example, reproducing what ``cpg_utils.cromwell.run_cromwell_workflow`` builds for
    a dataset (``dataset_bucket`` and friends are the dataset's bucket URLs)::

        options = build_workflow_options(
            service_account_secret=f'{dataset}-cromwell-{access_level}-key',
            secret_project=dataset_gcp_project,
            google_project=dataset_gcp_project,
            logs_dir=f'{analysis_bucket}/cromwell_logs/{output_prefix}',
            gcs_root=f'{tmp_bucket}/cromwell',
            outputs_dir=f'{dataset_bucket}/{output_prefix}',
            labels={'sequencing-type': 'genome'},
            ar_guid=ar_guid,
        )
    """
    service_account_json = read_secret(secret_project, service_account_secret)
    if not service_account_json:
        raise ValueError(f"Couldn't find secret: {service_account_secret}")

    service_account_email = json.loads(service_account_json).get('client_email')
    if not service_account_email:
        raise ValueError("The service_account didn't contain an entry for client_email")

    google_labels: dict[str, str] = {
        **(labels or {}),
        COMPUTE_CATEGORY_LABEL: 'cromwell',
    }
    if ar_guid:
        google_labels[AR_GUID_LABEL] = ar_guid

    options: dict[str, Any] = {
        'backend': 'batch',
        'user_service_account_json': service_account_json,
        'google_compute_service_account': service_account_email,
        'google_project': google_project,
        'google_labels': google_labels,
        'final_call_logs_dir': logs_dir,
        'final_workflow_log_dir': logs_dir,
        'gcp_batch_gcs_root': gcs_root,
    }
    if outputs_dir is not None:
        options['final_workflow_outputs_dir'] = outputs_dir
    return options
