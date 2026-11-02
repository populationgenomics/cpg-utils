"""
Parity between cpg_utils.cromwell.run_cromwell_workflow and
cpg_utils.cromwell_client.build_workflow_options (spec §2.2).

The old function derives everything from config; the new one takes explicit values.
Resolving those values from the same config must give identical Workflow options,
including key order.
"""

# ruff: noqa: E402 - the cpg_utils imports need hail, so they follow importorskip

import json
from os.path import join
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip('hailtop')

from cpg_utils.config import dataset_path, get_gcp_project, try_get_ar_guid
from cpg_utils.cromwell import run_cromwell_workflow
from cpg_utils.cromwell_client import build_workflow_options

DATASET = 'test-dataset'
ACCESS_LEVEL = 'test'
PREFIX = 'output-prefix'
FAKE_KEY = json.dumps(
    {'type': 'service_account', 'client_email': 'sa@test-gcp-project.iam'},
)
BUCKETS = {
    'default': 'gs://cpg-test-dataset-test',
    'analysis': 'gs://cpg-test-dataset-test-analysis',
    'tmp': 'gs://cpg-test-dataset-test-tmp',
}
CONFIG = {
    'workflow': {
        'ar-guid': 'config-ar-guid',
        'access_level': ACCESS_LEVEL,
        'dataset': DATASET,
        'dataset_gcp_project': 'test-gcp-project',
    },
    'storage': {'default': BUCKETS, DATASET: BUCKETS},
}


def _old_options(job: MagicMock) -> dict[str, Any]:
    """Pull the options JSON out of the `echo '…' > workflow-options.json` line."""
    for call in job.command.call_args_list:
        for line in call.args[0].splitlines():
            if line.strip().endswith('> workflow-options.json'):
                return json.loads(line.split("'", 1)[1].rsplit("'", 1)[0])
    raise AssertionError('workflow-options.json command not found')


@pytest.mark.parametrize('ar_guid_override', [None, 'override-ar-guid'])
@pytest.mark.parametrize('copy_outputs_to_gcp', [True, False])
@pytest.mark.parametrize('labels', [None, {'zeta': 'z', 'alpha': 'a'}])
@pytest.mark.parametrize('project', [None, 'override-project'])
def test_options_match_old_wrapper(
    ar_guid_override: str | None,
    copy_outputs_to_gcp: bool,
    labels: dict[str, str] | None,
    project: str | None,
):
    with (
        patch('cpg_utils.config.get_config', return_value=CONFIG),
        patch('cpg_utils.cromwell.read_secret', return_value=FAKE_KEY) as old_secret,
        patch(
            'cpg_utils.cromwell_client.gcp_batch.read_secret',
            return_value=FAKE_KEY,
        ) as new_secret,
    ):
        job = MagicMock()
        run_cromwell_workflow(
            job=job,
            dataset=DATASET,
            access_level=ACCESS_LEVEL,
            workflow='workflow.wdl',
            cwd=None,
            libs=[],
            output_prefix=PREFIX,
            labels=dict(labels) if labels else None,
            project=project,
            copy_outputs_to_gcp=copy_outputs_to_gcp,
            ar_guid_override=ar_guid_override,
        )
        old = _old_options(job)

        new = build_workflow_options(
            service_account_secret=f'{DATASET}-cromwell-{ACCESS_LEVEL}-key',
            secret_project=get_gcp_project(),
            google_project=project or get_gcp_project(),
            logs_dir=dataset_path(
                f'cromwell_logs/{PREFIX}',
                dataset=DATASET,
                category='analysis',
            ),
            gcs_root=join(
                dataset_path('', dataset=DATASET, category='tmp'),
                'cromwell',
            ),
            outputs_dir=(
                dataset_path(PREFIX, dataset=DATASET) if copy_outputs_to_gcp else None
            ),
            labels=labels,
            ar_guid=ar_guid_override or try_get_ar_guid(),
        )

    assert old_secret.call_args == new_secret.call_args
    assert new == old
    assert list(new) == list(old)
    assert list(new['google_labels']) == list(old['google_labels'])
    # round-tripping through JSON (as the client does) preserves the order too
    assert json.dumps(new) == json.dumps(old)


MINIMAL_ARGS = {
    'service_account_secret': 'my-secret',
    'secret_project': 'p',
    'google_project': 'p',
    'logs_dir': 'gs://l',
    'gcs_root': 'gs://r',
}


def test_missing_secret_raises():
    with (
        patch('cpg_utils.cromwell_client.gcp_batch.read_secret', return_value=None),
        pytest.raises(ValueError, match="Couldn't find secret: my-secret"),
    ):
        build_workflow_options(**MINIMAL_ARGS)


def test_secret_without_client_email_raises():
    with (
        patch('cpg_utils.cromwell_client.gcp_batch.read_secret', return_value='{}'),
        pytest.raises(ValueError, match='client_email'),
    ):
        build_workflow_options(**MINIMAL_ARGS)
