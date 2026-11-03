"""Tests for cpg_utils.cromwell_client.watch (spec Phase 4)."""

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from cpg_utils.cromwell_client import common as common_module
from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.errors import (
    CromwellNotFound,
    CromwellUnavailable,
    WorkflowFailed,
    WorkflowTimedOut,
)
from cpg_utils.cromwell_client.watch import (
    METADATA_REPORT_KEYS,
    Polling,
    watch_workflow,
)
from cpg_utils.cromwell_model import WorkflowMetadataModel

WF = 'wf-123'
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
OUTPUTS = {'w.out': 'gs://bucket/out.txt'}
REPORT_META = {
    'id': WF,
    'workflowName': 'w',
    'status': 'Succeeded',
    'submission': '2026-11-03T11:59:00.000Z',
    'start': '2026-11-03T11:59:01.000Z',
    'end': '2026-11-03T11:59:30.000Z',
    'calls': {
        'w.task': [
            {
                'executionStatus': 'Done',
                'shardIndex': -1,
                'attempt': 1,
                'start': '2026-11-03T11:59:02.000Z',
                'end': '2026-11-03T11:59:29.000Z',
            },
        ],
    },
}


class FakeClient:
    """Scripted stand-in for CromwellClient. Each script entry is a value or an error."""

    def __init__(
        self,
        statuses: list[Any],
        *,
        submission: Any = '2026-11-03T11:59:00.000Z',
        submission_errors: list[Exception] | None = None,
        outputs: list[Any] | None = None,
        report: Any = None,
        abort: list[Any] | None = None,
    ) -> None:
        self.statuses = list(statuses)
        self.submission = submission
        self.submission_errors = list(submission_errors or [])
        self.outputs_script = list(outputs or [OUTPUTS])
        self.report = REPORT_META if report is None else report
        self.abort_script = list(abort or ['Aborting'])
        self.calls: list[tuple[str, Any]] = []

    @staticmethod
    def _next(script: list[Any]) -> Any:
        value = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(value, BaseException):
            raise value
        return value

    def metadata(self, workflow_id: str, *, include_keys: tuple[str, ...] = ()):
        self.calls.append(('metadata', include_keys))
        assert workflow_id == WF
        if include_keys == ('submission',):
            if self.submission_errors:
                raise self.submission_errors.pop(0)
            return {} if self.submission is None else {'submission': self.submission}
        if isinstance(self.report, BaseException):
            raise self.report
        return self.report

    def status(self, workflow_id: str) -> str:
        self.calls.append(('status', workflow_id))
        return self._next(self.statuses)

    def outputs(self, workflow_id: str) -> dict[str, Any]:
        self.calls.append(('outputs', workflow_id))
        return self._next(self.outputs_script)

    def abort(self, workflow_id: str) -> str:
        self.calls.append(('abort', workflow_id))
        return self._next(self.abort_script)

    def count(self, name: str) -> int:
        return sum(1 for call, _ in self.calls if call == name)


class Clock:
    def __init__(self) -> None:
        self.now = NOW
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)

    def utcnow(self) -> datetime:
        return self.now


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(common_module, 'sleep', c.sleep)
    monkeypatch.setattr(common_module, 'utcnow', c.utcnow)
    return c


def _watch(client: FakeClient, **kwargs: Any) -> dict[str, Any]:
    return watch_workflow(cast(CromwellClient, client), WF, **kwargs)


def _old_formula(elapsed: float, lo: int, hi: int, ramp: int) -> int:
    """Copied from cpg_utils.cromwell.watch_workflow._get_wait_interval."""
    factor = elapsed / ramp
    if factor > 1:
        return hi
    return max(lo, int((1 - math.cos(math.pi * factor)) * hi // 2))


@pytest.mark.parametrize('ramp', [1200, 600])
def test_poll_interval_matches_old_formula(ramp: int):
    polling = Polling(min_seconds=5, max_seconds=60, ramp_seconds=ramp)
    for elapsed in (0, 1, ramp / 4, ramp / 2, ramp, 2 * ramp):
        assert polling.interval(elapsed) == _old_formula(elapsed, 5, 60, ramp)


def test_polling_defaults_match_old_watcher():
    assert Polling() == Polling(
        min_seconds=5,
        max_seconds=60,
        ramp_seconds=1200,
        max_consecutive_errors=25,
    )


def test_happy_path(caplog: pytest.LogCaptureFixture, clock: Clock):
    client = FakeClient(['Submitted', 'Running', 'Succeeded'])
    with caplog.at_level(logging.INFO, logger='cpg_utils.cromwell_client.watch'):
        assert _watch(client) == OUTPUTS
    assert client.count('status') == 3  # noqa: PLR2004
    assert client.count('outputs') == 1
    assert ('metadata', METADATA_REPORT_KEYS) in client.calls
    assert len(clock.sleeps) == 2  # noqa: PLR2004
    assert any('report' in r.message and WF in r.message for r in caplog.records)


def test_failed_with_nested_caused_by():
    report = {
        **REPORT_META,
        'status': 'Failed',
        'failures': [
            {
                'message': 'Workflow failed',
                'causedBy': [
                    {'message': 'Task w.task failed', 'causedBy': []},
                    {
                        'message': 'Task w.other failed',
                        'causedBy': [{'message': 'Task w.task failed'}],
                    },
                ],
            },
        ],
    }
    client = FakeClient(['Running', 'Failed'], report=report)
    with pytest.raises(WorkflowFailed) as exc:
        _watch(client)
    assert exc.value.status == 'Failed'
    assert exc.value.workflow_id == WF
    assert exc.value.failures == [
        'Workflow failed',
        'Task w.task failed',
        'Task w.other failed',
    ]
    assert WF in str(exc.value)


def test_aborted_elsewhere_is_workflow_failed():
    client = FakeClient(['Running', 'Aborted'])
    with pytest.raises(WorkflowFailed) as exc:
        _watch(client)
    assert exc.value.status == 'Aborted'
    assert exc.value.failures == []


def test_transient_errors_recover_and_budget_resets():
    blip = CromwellUnavailable('503')
    statuses = [blip, blip, 'Running', blip, blip, 'Running', blip, blip, 'Succeeded']
    client = FakeClient(statuses)
    # three errors in a row would exhaust the budget; two never do
    assert _watch(client, polling=Polling(max_consecutive_errors=3)) == OUTPUTS


def test_exhausted_budget_raises_unavailable():
    client = FakeClient([CromwellUnavailable('503')])
    with pytest.raises(CromwellUnavailable, match='3 consecutive errors'):
        _watch(client, polling=Polling(max_consecutive_errors=3))
    assert client.count('status') == 3  # noqa: PLR2004


def test_submission_time_errors_count_against_budget():
    client = FakeClient(
        ['Succeeded'],
        submission_errors=[CromwellUnavailable('503')],
    )
    assert _watch(client) == OUTPUTS
    client = FakeClient(
        ['Succeeded'],
        submission_errors=[CromwellUnavailable('503')] * 2,
    )
    with pytest.raises(CromwellUnavailable):
        _watch(client, polling=Polling(max_consecutive_errors=2))


def test_submission_time_not_found_propagates():
    client = FakeClient(['Running'], submission_errors=[CromwellNotFound('404')])
    with pytest.raises(CromwellNotFound):
        _watch(client)


def test_missing_submission_times_from_now(caplog: pytest.LogCaptureFixture):
    client = FakeClient(['Running', 'Running'], submission=None)
    with caplog.at_level(logging.WARNING), pytest.raises(WorkflowTimedOut):
        _watch(client, time_limit_seconds=5)
    assert any('no usable submission time' in r.message for r in caplog.records)
    # timed from "now", so the limit only fires after the first sleep
    assert client.count('status') == 2  # noqa: PLR2004


def test_time_limit_counts_from_past_submission():
    client = FakeClient(['Running'], submission='2026-11-03T09:00:00.000Z')
    with pytest.raises(WorkflowTimedOut) as exc:
        _watch(client, time_limit_seconds=3600)
    assert exc.value.abort_confirmed is True
    assert exc.value.time_limit_seconds == 3600  # noqa: PLR2004
    assert client.count('status') == 1
    assert client.count('abort') == 1
    assert WF in str(exc.value)


def test_time_limit_does_not_abort_a_finished_workflow():
    client = FakeClient(['Succeeded'], submission='2026-11-03T09:00:00.000Z')
    assert _watch(client, time_limit_seconds=3600) == OUTPUTS
    assert client.count('abort') == 0


def test_abort_that_keeps_failing_is_unconfirmed():
    client = FakeClient(
        ['Running'],
        submission='2026-11-03T09:00:00.000Z',
        abort=[CromwellUnavailable('503')],
    )
    with pytest.raises(WorkflowTimedOut) as exc:
        _watch(client, time_limit_seconds=60, polling=Polling(max_consecutive_errors=4))
    assert exc.value.abort_confirmed is False
    assert client.count('abort') == 4  # noqa: PLR2004


def test_abort_that_recovers_is_confirmed():
    client = FakeClient(
        ['Running'],
        submission='2026-11-03T09:00:00.000Z',
        abort=[CromwellUnavailable('503'), 'Aborting'],
    )
    with pytest.raises(WorkflowTimedOut) as exc:
        _watch(client, time_limit_seconds=60)
    assert exc.value.abort_confirmed is True
    assert client.count('abort') == 2  # noqa: PLR2004


def test_outputs_transient_failure_recovers_and_reports_once(
    caplog: pytest.LogCaptureFixture,
):
    client = FakeClient(
        ['Running', 'Succeeded'],
        outputs=[CromwellUnavailable('500'), CromwellUnavailable('500'), OUTPUTS],
    )
    with caplog.at_level(logging.INFO, logger='cpg_utils.cromwell_client.watch'):
        assert _watch(client, polling=Polling(max_consecutive_errors=3)) == OUTPUTS
    assert client.count('outputs') == 3  # noqa: PLR2004
    assert sum(1 for _, keys in client.calls if keys == METADATA_REPORT_KEYS) == 1
    assert sum('report' in r.message for r in caplog.records) == 1


def test_outputs_that_never_recover_exhaust_budget():
    client = FakeClient(['Succeeded'], outputs=[CromwellUnavailable('500')])
    with pytest.raises(CromwellUnavailable):
        _watch(client, polling=Polling(max_consecutive_errors=3))


def test_report_failure_does_not_fail_watch(caplog: pytest.LogCaptureFixture):
    client = FakeClient(['Succeeded'], report=CromwellUnavailable('500'))
    with caplog.at_level(logging.WARNING):
        assert _watch(client) == OUTPUTS
    assert any('failed to collect run metadata' in r.message for r in caplog.records)


def test_unrenderable_report_does_not_fail_watch():
    client = FakeClient(['Succeeded'], report={'status': 'NotAStatus'})
    assert _watch(client) == OUTPUTS


@pytest.mark.parametrize('abort_on_interrupt', [True, False])
def test_keyboard_interrupt(abort_on_interrupt: bool):
    client = FakeClient(['Running', KeyboardInterrupt()])
    with pytest.raises(KeyboardInterrupt):
        _watch(client, abort_on_interrupt=abort_on_interrupt)
    assert client.count('abort') == (1 if abort_on_interrupt else 0)


def test_interrupt_abort_failure_still_reraises_original():
    client = FakeClient(
        [KeyboardInterrupt()],
        abort=[CromwellUnavailable('503')],
    )
    with pytest.raises(KeyboardInterrupt):
        _watch(client, abort_on_interrupt=True)
    assert client.count('abort') == 1


def test_workflow_failed_is_not_aborted_on_interrupt():
    client = FakeClient(['Failed'])
    with pytest.raises(WorkflowFailed):
        _watch(client, abort_on_interrupt=True)
    assert client.count('abort') == 0


def test_not_found_on_status_propagates():
    client = FakeClient([CromwellNotFound('404')])
    with pytest.raises(CromwellNotFound):
        _watch(client)
    assert client.count('status') == 1


def test_display_renders_from_trimmed_metadata():
    full = {
        **REPORT_META,
        'inputs': {'w.x': 1},
        'outputs': OUTPUTS,
        'workflowRoot': 'gs://root',
        'labels': {'cromwell-workflow-id': WF},
        'submittedFiles': {'workflow': 'version 1.0'},
    }
    trimmed = {k: v for k, v in full.items() if k in METADATA_REPORT_KEYS}
    assert WorkflowMetadataModel.parse(trimmed).display(
        monochrome=True,
    ) == WorkflowMetadataModel.parse(full).display(monochrome=True)
