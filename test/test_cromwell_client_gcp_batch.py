"""Tests for cpg_utils.cromwell_client.gcp_batch that don't need Hail."""

import json
import pathlib
from typing import Any
from unittest.mock import patch

import pytest
from cloudpathlib import AnyPath

from cpg_utils.cromwell_client.gcp_batch import build_workflow_options

KEY_JSON = json.dumps({'client_email': 'sa@project.iam.gserviceaccount.com'})


def _build(**kwargs: Any) -> dict[str, Any]:
    kwargs = {
        'service_account_secret': 'ds-cromwell-test-key',
        'secret_project': 'ds-project',
        'google_project': 'ds-project',
        'logs_dir': 'gs://ds-analysis/cromwell_logs/x',
        'gcs_root': 'gs://ds-tmp/cromwell',
        **kwargs,
    }
    with patch(
        'cpg_utils.cromwell_client.gcp_batch.read_secret',
        return_value=KEY_JSON,
    ):
        return build_workflow_options(**kwargs)


def test_google_labels_are_merged():
    options = _build(google_labels={'stage': 'align'}, ar_guid='guid-1')
    assert options['google_labels'] == {
        'stage': 'align',
        'compute-category': 'cromwell',
        'ar-guid': 'guid-1',
    }


@pytest.mark.parametrize(
    'make_path',
    [str, AnyPath, pathlib.Path],
    ids=['str', 'cloudpath', 'pathlib'],
)
def test_directories_accept_paths(make_path: Any, tmp_path: pathlib.Path):
    base = str(tmp_path) if make_path is pathlib.Path else 'gs://b'
    logs, root, out = f'{base}/logs', f'{base}/root', f'{base}/out'
    options = _build(
        logs_dir=make_path(logs),
        gcs_root=make_path(root),
        outputs_dir=make_path(out),
    )
    assert options['final_call_logs_dir'] == str(logs)
    assert options['final_workflow_log_dir'] == str(logs)
    assert options['gcp_batch_gcs_root'] == str(root)
    assert options['final_workflow_outputs_dir'] == str(out)
    # every value must survive the JSON encoding the client does on submit
    json.dumps(options)
