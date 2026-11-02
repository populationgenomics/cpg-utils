"""Tests for the Submission key in cpg_utils.cromwell_client.submission (spec §3.2)."""

import json
import re
from typing import Any

import pytest

from cpg_utils.cromwell_client.submission import (
    compute_submission_key,
    serialise_inputs,
)

WDL = b'version 1.0\nworkflow w {}\n'
DEPS = b'PK\x03\x04deps'
INPUTS = {'w.sample': 'sample-1', 'w.bam': 'gs://bucket/a.bam', 'w.n': 3}


def _options(**overrides: Any) -> dict[str, Any]:
    options = {
        'backend': 'batch',
        'user_service_account_json': '{"client_email": "a@b", "private_key": "1"}',
        'google_compute_service_account': 'a@b',
        'google_project': 'proj',
        'google_labels': {'compute-category': 'cromwell', 'ar-guid': 'guid-1'},
        'final_call_logs_dir': 'gs://logs',
        'final_workflow_log_dir': 'gs://logs',
        'gcp_batch_gcs_root': 'gs://tmp/cromwell',
        'final_workflow_outputs_dir': 'gs://out',
    }
    options.update(overrides)
    return options


def _key(
    *,
    workflow_source: bytes = WDL,
    dependencies: bytes | None = DEPS,
    input_documents: list[bytes] | None = None,
    options: dict[str, Any] | None = None,
) -> str:
    return compute_submission_key(
        workflow_source=workflow_source,
        dependencies=dependencies,
        input_documents=(
            input_documents
            if input_documents is not None
            else [b'{"w.file_input": 1}', serialise_inputs(INPUTS)]
        ),
        options=options if options is not None else _options(),
    )


BASE = _key()


def test_key_format():
    assert re.fullmatch(r'[0-9a-f]{63}', BASE)


@pytest.mark.parametrize(
    'kwargs',
    [
        pytest.param(
            {'options': _options(user_service_account_json='{}')},
            id='secret',
        ),
        pytest.param(
            {'options': _options(google_labels={'compute-category': 'cromwell'})},
            id='google_labels',
        ),
        pytest.param(
            {
                'options': _options(
                    google_labels={'compute-category': 'cromwell', 'ar-guid': 'guid-2'},
                ),
            },
            id='ar_guid',
        ),
        pytest.param(
            {'options': dict(reversed(list(_options().items())))},
            id='options_key_order',
        ),
        pytest.param(
            {
                'input_documents': [
                    b'{"w.file_input": 1}',
                    serialise_inputs(dict(reversed(list(INPUTS.items())))),
                ],
            },
            id='inputs_dict_key_order',
        ),
    ],
)
def test_key_unchanged(kwargs: dict[str, Any]):
    assert _key(**kwargs) == BASE


@pytest.mark.parametrize(
    'kwargs',
    [
        pytest.param({'workflow_source': WDL + b'\n'}, id='wdl'),
        pytest.param({'dependencies': DEPS + b'x'}, id='dependencies'),
        pytest.param({'dependencies': None}, id='no_dependencies'),
        pytest.param(
            {
                'input_documents': [
                    b'{"w.file_input": 1}',
                    serialise_inputs({**INPUTS, 'w.n': 4}),
                ],
            },
            id='input_value',
        ),
        pytest.param(
            {
                'input_documents': [
                    serialise_inputs(INPUTS),
                    b'{"w.file_input": 1}',
                ],
            },
            id='input_document_order',
        ),
        pytest.param({'options': _options(google_project='other')}, id='project'),
        pytest.param({'options': _options(final_call_logs_dir='gs://x')}, id='logs'),
        pytest.param({'options': _options(gcp_batch_gcs_root='gs://x')}, id='root'),
        pytest.param(
            {'options': _options(final_workflow_outputs_dir='gs://x')},
            id='outputs_dir',
        ),
    ],
)
def test_key_changes(kwargs: dict[str, Any]):
    assert _key(**kwargs) != BASE


def test_removing_outputs_dir_changes_key():
    options = _options()
    del options['final_workflow_outputs_dir']
    assert _key(options=options) != BASE


def test_serialise_inputs_is_canonical_json():
    data = serialise_inputs(INPUTS)
    assert json.loads(data) == INPUTS
    assert data == serialise_inputs(dict(sorted(INPUTS.items(), reverse=True)))
    assert b' ' not in data.replace(b'gs://bucket/a.bam', b'')
