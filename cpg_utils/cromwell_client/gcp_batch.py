"""
Workflow options for Cromwell's GCP Batch backend.

This is the only backend-specific part of ``cpg_utils.cromwell_client``. Every value is
passed in explicitly; CPG naming conventions appear only in the examples.
"""

import json
import logging
from collections.abc import Mapping
from typing import Any

from cpg_utils import Path
from cpg_utils.cloud import read_secret

logger = logging.getLogger(__name__)

COMPUTE_CATEGORY_LABEL = 'compute-category'
AR_GUID_LABEL = 'ar-guid'


def build_workflow_options(
    *,
    service_account_secret: str | None = None,
    secret_project: str | None = None,
    service_account_json: str | None = None,
    google_project: str,
    logs_dir: str | Path,
    gcs_root: str | Path,
    outputs_dir: str | Path | None = None,
    google_labels: Mapping[str, str] | None = None,
    ar_guid: str | None = None,
) -> dict[str, Any]:
    """
    Build Workflow options for Cromwell's GCP Batch backend.

    Cromwell runs the Workflow's jobs as the service account whose key JSON is either
    read from GCP Secret Manager (``service_account_secret`` and ``secret_project``)
    or given directly (``service_account_json``), e.g. by a test or a dry run that
    must not read Secret Manager.

    Args:
        service_account_secret: name of the secret holding the service account key
            JSON.
        secret_project: GCP project that holds the secret. This can differ from
            ``google_project``.
        service_account_json: the service account key JSON itself, instead of the
            two arguments above.
        google_project: GCP project the Workflow's jobs run (and are billed) in.
        logs_dir: where Cromwell writes call and Workflow logs.
        gcs_root: Cromwell's execution root for intermediate files.
        outputs_dir: if given, Cromwell copies the final Workflow outputs here.
        google_labels: extra labels attached to the cloud resources the Workflow
            creates. These are not Cromwell workflow labels.
        ar_guid: if given, added to ``google_labels`` as ``ar-guid``.

    Returns:
        The Workflow options dict, ready to pass to ``CromwellClient.submit`` or
        ``run_workflow``.

    Raises:
        ValueError: neither or both ways of giving the key were used, the secret
            doesn't exist, or the key has no ``client_email``.

    Example, reproducing what ``cpg_utils.cromwell.run_cromwell_workflow`` builds for
    a dataset (``dataset_bucket`` and friends are the dataset's bucket URLs)::

        options = build_workflow_options(
            service_account_secret=f'{dataset}-cromwell-{access_level}-key',
            secret_project=dataset_gcp_project,
            google_project=dataset_gcp_project,
            logs_dir=f'{analysis_bucket}/cromwell_logs/{output_prefix}',
            gcs_root=f'{tmp_bucket}/cromwell',
            outputs_dir=f'{dataset_bucket}/{output_prefix}',
            google_labels={'sequencing-type': 'genome'},
            ar_guid=ar_guid,
        )
    """
    if service_account_json is None:
        if service_account_secret is None or secret_project is None:
            raise ValueError(
                'Give either service_account_secret and secret_project, '
                'or service_account_json',
            )
        service_account_json = read_secret(secret_project, service_account_secret)
        if not service_account_json:
            raise ValueError(f"Couldn't find secret: {service_account_secret}")
    elif service_account_secret is not None or secret_project is not None:
        raise ValueError(
            'Give either service_account_secret and secret_project, '
            'or service_account_json, not both',
        )

    service_account_email = json.loads(service_account_json).get('client_email')
    if not service_account_email:
        raise ValueError("The service_account didn't contain an entry for client_email")

    labels: dict[str, str] = {
        **(google_labels or {}),
        COMPUTE_CATEGORY_LABEL: 'cromwell',
    }
    if ar_guid:
        labels[AR_GUID_LABEL] = ar_guid

    options: dict[str, Any] = {
        'backend': 'batch',
        'user_service_account_json': service_account_json,
        'google_compute_service_account': service_account_email,
        'google_project': google_project,
        'google_labels': labels,
        'final_call_logs_dir': str(logs_dir),
        'final_workflow_log_dir': str(logs_dir),
        'gcp_batch_gcs_root': str(gcs_root),
    }
    if outputs_dir is not None:
        options['final_workflow_outputs_dir'] = str(outputs_dir)
    return options
