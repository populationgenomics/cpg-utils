"""Tests for run_workflow and the Submission-key lookup (spec Phase 6)."""

import json
import pathlib
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import patch

import pytest

from cpg_utils.cromwell_client import collect as collect_module
from cpg_utils.cromwell_client import submission as submission_module
from cpg_utils.cromwell_client import watch as watch_module
from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.errors import CromwellError, CromwellUnavailable
from cpg_utils.cromwell_client.gcp_batch import build_workflow_options
from cpg_utils.cromwell_client.submission import (
    ABORTING_WAIT_SECONDS,
    SUBMISSION_KEY_LABEL,
    WorkflowResult,
    compute_submission_key,
    run_workflow,
    serialise_inputs,
)

OPTIONS = {'backend': 'batch', 'google_project': 'proj'}


class FakeCromwell:
    """A stateful fake: Workflows are stored with labels and scripted statuses."""

    def __init__(self) -> None:
        self.workflows: dict[str, dict[str, Any]] = {}
        self.submissions: list[dict[str, Any]] = []
        self.calls: list[str] = []
        self.query_error: Exception | None = None
        self.next_statuses: list[str] = ['Running', 'Succeeded']
        self.outputs_by_id: dict[str, dict[str, Any]] = {}

    def add(
        self,
        workflow_id: str,
        statuses: list[str],
        *,
        key: str | None = None,
        submitted: str = '2026-11-03T10:00:00.000Z',
        outputs: dict[str, Any] | None = None,
    ) -> None:
        self.workflows[workflow_id] = {
            'statuses': list(statuses),
            'labels': {SUBMISSION_KEY_LABEL: key} if key else {},
            'submission': submitted,
        }
        self.outputs_by_id[workflow_id] = outputs or {'w.out': f'out-{workflow_id}'}

    # --- the CromwellClient surface -------------------------------------------------

    def submit(self, workflow_source: bytes, **kwargs: Any) -> str:
        self.calls.append('submit')
        workflow_id = f'new-{len(self.submissions) + 1}'
        self.submissions.append({'workflow_source': workflow_source, **kwargs})
        self.add(
            workflow_id,
            self.next_statuses,
            key=(kwargs.get('labels') or {}).get(SUBMISSION_KEY_LABEL),
            submitted='2026-11-03T11:59:00.000Z',
        )
        return workflow_id

    def query_by_label(self, key: str, value: str) -> list[dict[str, Any]]:
        self.calls.append('query')
        if self.query_error:
            raise self.query_error
        return [
            {
                'id': workflow_id,
                'status': wf['statuses'][0],
                'submission': wf['submission'],
            }
            for workflow_id, wf in self.workflows.items()
            if wf['labels'].get(key) == value
        ]

    def status(self, workflow_id: str) -> str:
        self.calls.append(f'status:{workflow_id}')
        statuses = self.workflows[workflow_id]['statuses']
        if len(statuses) > 1:
            statuses.pop(0)
        return statuses[0]

    def metadata(self, workflow_id: str, *, include_keys: Any = ()) -> dict[str, Any]:
        self.calls.append(f'metadata:{workflow_id}')
        return {
            'id': workflow_id,
            'submission': self.workflows[workflow_id]['submission'],
            'status': self.workflows[workflow_id]['statuses'][0],
            'calls': {},
        }

    def outputs(self, workflow_id: str) -> dict[str, Any]:
        self.calls.append(f'outputs:{workflow_id}')
        return self.outputs_by_id[workflow_id]

    def abort(self, workflow_id: str) -> str:
        self.calls.append(f'abort:{workflow_id}')
        return 'Aborting'


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(watch_module, '_sleep', c.sleep)
    monkeypatch.setattr(watch_module, '_utcnow', lambda: c.now)
    monkeypatch.setattr(submission_module, '_sleep', c.sleep)
    return c


@pytest.fixture
def wdl(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / 'Main.wdl'
    path.write_text('version 1.0\nworkflow w {}\n')
    return path


@pytest.fixture
def fake() -> FakeCromwell:
    return FakeCromwell()


def _run(fake: FakeCromwell, wdl: pathlib.Path, **kwargs: Any) -> WorkflowResult:
    kwargs.setdefault('options', OPTIONS)
    kwargs.setdefault('inputs', {'w.x': 1})
    return run_workflow(cast(CromwellClient, fake), wdl, **kwargs)


def _key(wdl: pathlib.Path, inputs: dict[str, Any] | None = None) -> str:
    return compute_submission_key(
        workflow_source=wdl.read_bytes(),
        dependencies=None,
        input_documents=[serialise_inputs(inputs or {'w.x': 1})],
        options=OPTIONS,
    )


def test_nothing_found_submits(fake: FakeCromwell, wdl: pathlib.Path):
    result = _run(fake, wdl)
    assert fake.calls[:2] == ['query', 'submit']
    assert result == WorkflowResult(
        workflow_id='new-1',
        outputs={'w.out': 'out-new-1'},
        collected={},
        reused=False,
    )
    sub = fake.submissions[0]
    assert sub['labels'] == {SUBMISSION_KEY_LABEL: _key(wdl)}
    assert sub['workflow_filename'] == 'Main.wdl'
    assert sub['options'] == OPTIONS


@pytest.mark.parametrize('status', ['Submitted', 'Running', 'On Hold'])
def test_active_workflow_is_attached(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    status: str,
):
    fake.add('old', [status, 'Succeeded'], key=_key(wdl))
    result = _run(fake, wdl)
    assert 'submit' not in fake.calls
    assert result.workflow_id == 'old'
    assert result.reused is True
    assert result.outputs == {'w.out': 'out-old'}


def test_succeeded_workflow_is_attached_and_collected_again(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        collect_module,
        '_is_file_value',
        lambda v: isinstance(v, str) and v.startswith('/'),
    )
    src = tmp_path / 'result.txt'
    src.write_text('done\n')
    fake.add('old', ['Succeeded'], key=_key(wdl), outputs={'w.out': str(src)})
    dst = tmp_path / 'dest' / 'result.txt'
    for _ in range(2):
        dst.unlink(missing_ok=True)
        result = _run(fake, wdl, destinations={'w.out': dst})
        assert result.reused is True
        assert result.collected == {'w.out': str(dst)}
        assert dst.read_text() == 'done\n'
    assert 'submit' not in fake.calls
    # straight to outputs: one status poll per run
    assert fake.calls.count('status:old') == 2  # noqa: PLR2004


@pytest.mark.parametrize('status', ['Failed', 'Aborted'])
def test_failed_or_aborted_workflow_is_resubmitted(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    status: str,
):
    fake.add('old', [status], key=_key(wdl))
    result = _run(fake, wdl)
    assert fake.calls[:2] == ['query', 'submit']
    assert result.workflow_id == 'new-1'
    assert result.reused is False


def test_aborting_waits_then_submits(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    clock: Clock,
):
    fake.add('old', ['Aborting', 'Aborting', 'Aborting', 'Aborted'], key=_key(wdl))
    result = _run(fake, wdl, max_poll_interval=30)
    assert result.workflow_id == 'new-1'
    assert fake.calls.count('status:old') == 3  # noqa: PLR2004
    assert clock.sleeps[:3] == [30, 30, 30]


def test_aborting_too_long_raises(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    clock: Clock,
):
    fake.add('old', ['Aborting'], key=_key(wdl))
    with pytest.raises(CromwellError, match='still Aborting'):
        _run(fake, wdl)
    assert 'submit' not in fake.calls
    assert sum(clock.sleeps) == ABORTING_WAIT_SECONDS


def test_newest_result_wins(fake: FakeCromwell, wdl: pathlib.Path):
    key = _key(wdl)
    fake.add('oldest', ['Running', 'Succeeded'], key=key, submitted='2026-11-01T00:00Z')
    fake.add('newest', ['Failed'], key=key, submitted='2026-11-03T00:00:00.000Z')
    fake.add('middle', ['Running', 'Succeeded'], key=key, submitted='2026-11-02T00:00Z')
    result = _run(fake, wdl)
    # the newest one failed, so a new Workflow is submitted (not the older running one)
    assert result.workflow_id == 'new-1'

    fake2 = FakeCromwell()
    fake2.add('a', ['Failed'], key=key, submitted='2026-11-01T00:00:00.000Z')
    fake2.add('b', ['Running', 'Succeeded'], key=key, submitted='2026-11-02T00:00Z')
    assert _run(fake2, wdl).workflow_id == 'b'


def test_reuse_existing_false_skips_lookup_but_sends_label(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    fake.add('old', ['Running', 'Succeeded'], key=_key(wdl))
    result = _run(fake, wdl, reuse_existing=False)
    assert 'query' not in fake.calls
    assert result.workflow_id == 'new-1'
    assert fake.submissions[0]['labels'] == {SUBMISSION_KEY_LABEL: _key(wdl)}


def test_lookup_failure_propagates_without_submitting(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    fake.query_error = CromwellUnavailable('503')
    with pytest.raises(CromwellUnavailable):
        _run(fake, wdl)
    assert 'submit' not in fake.calls


def test_input_document_order_and_canonical_dict(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    tmp_path: pathlib.Path,
):
    first = tmp_path / 'first.json'
    second = tmp_path / 'second.json'
    first.write_text('{"w.a": 1}')
    second.write_text('{"w.b": 2}')
    _run(
        fake,
        wdl,
        input_files=[first, str(second)],
        inputs={'w.z': 1, 'w.a': {'y': 2, 'x': 1}},
    )
    docs = fake.submissions[0]['input_documents']
    assert docs == [
        b'{"w.a": 1}',
        b'{"w.b": 2}',
        b'{"w.a":{"x":1,"y":2},"w.z":1}',
    ]


def test_too_many_input_documents_raises_before_any_request(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    tmp_path: pathlib.Path,
):
    doc = tmp_path / 'in.json'
    doc.write_text('{}')
    with pytest.raises(ValueError, match='at most 5'):
        _run(fake, wdl, input_files=[doc] * 5)
    assert fake.calls == []


def test_bad_watch_kwarg_raises_before_any_request(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    with pytest.raises(TypeError, match='time_limit'):
        _run(fake, wdl, time_limit=60)
    assert fake.calls == []


def test_watch_kwargs_are_passed_through(fake: FakeCromwell, wdl: pathlib.Path):
    fake.next_statuses = ['Running']
    from cpg_utils.cromwell_client.errors import WorkflowTimedOut

    # submitted at 11:59, clock at 12:00: a 30s limit has already passed
    with pytest.raises(WorkflowTimedOut):
        _run(fake, wdl, time_limit_seconds=30)
    assert 'abort:new-1' in fake.calls


def test_round_trip_reattaches_despite_new_secret_and_ar_guid(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    def options(secret_email: str, ar_guid: str) -> dict[str, Any]:
        key_json = json.dumps({'client_email': secret_email, 'private_key': ar_guid})
        with patch(
            'cpg_utils.cromwell_client.gcp_batch.read_secret',
            return_value=key_json,
        ):
            return build_workflow_options(
                service_account_secret='ds-cromwell-test-key',
                secret_project='ds-project',
                google_project='ds-project',
                logs_dir='gs://ds-analysis/cromwell_logs/x',
                gcs_root='gs://ds-tmp/cromwell',
                outputs_dir='gs://ds-main/x',
                labels={'stage': 'align'},
                ar_guid=ar_guid,
            )

    fake.next_statuses = ['Running']  # the first watcher is "preempted" mid-run
    first_options = options('sa@one', 'guid-1')
    with pytest.raises(KeyboardInterrupt):
        with patch.object(
            watch_module,
            '_sleep',
            side_effect=KeyboardInterrupt,
        ):
            _run(fake, wdl, options=first_options)
    assert fake.calls.count('submit') == 1
    assert not any(c.startswith('abort') for c in fake.calls)

    fake.workflows['new-1']['statuses'] = ['Running', 'Succeeded']
    second = _run(fake, wdl, options=options('sa@two', 'guid-2'))
    assert fake.calls.count('submit') == 1
    assert second.workflow_id == 'new-1'
    assert second.reused is True
