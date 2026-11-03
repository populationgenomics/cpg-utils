"""Tests for run_workflows, and for the ``stop`` event it relies on (spec Phase 8)."""

import json
import pathlib
import signal
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from cpg_utils.cromwell_client import common as common_module
from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.errors import Stopped, WorkflowFailed
from cpg_utils.cromwell_client.many import (
    WorkflowOutcome,
    WorkflowRequest,
    _Throttle,
    run_workflows,
)
from cpg_utils.cromwell_client.submission import (
    SUBMISSION_KEY_LABEL,
    WorkflowResult,
    prepare_workflow,
    run_workflow,
)
from cpg_utils.cromwell_client.watch import watch_workflow

OPTIONS = {'backend': 'batch', 'google_project': 'proj'}
SUBMITTED = '2026-11-03T12:00:00.000Z'


class FakeCromwell:
    """
    A thread-safe fake. A submitted Workflow's ID is ``wf-<w.sample>``, taken from its
    inputs, so IDs don't depend on which thread submits first.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.calls: list[str] = []
        self.workflows: dict[str, dict[str, Any]] = {}
        #: statuses for the Workflow submitted for a sample; default: Running, Succeeded
        self.scripts: dict[str, list[str]] = {}

    def _record(self, call: str) -> None:
        with self.lock:
            self.calls.append(call)

    def add(self, workflow_id: str, statuses: Sequence[str], *, key: str) -> None:
        with self.lock:
            self.workflows[workflow_id] = {
                'statuses': list(statuses),
                'labels': {SUBMISSION_KEY_LABEL: key},
            }

    def submit(
        self,
        workflow_source: bytes,  # noqa: ARG002
        *,
        input_documents: Sequence[bytes],
        labels: dict[str, str],
        **kwargs: Any,  # noqa: ARG002
    ) -> str:
        sample = json.loads(input_documents[-1])['w.sample']
        self._record(f'submit:{sample}')
        workflow_id = f'wf-{sample}'
        statuses = self.scripts.get(sample, ['Running', 'Succeeded'])
        self.add(workflow_id, statuses, key=labels[SUBMISSION_KEY_LABEL])
        return workflow_id

    def query_by_label(self, key: str, value: str) -> list[dict[str, Any]]:
        self._record('query')
        with self.lock:
            return [
                {'id': wid, 'status': wf['statuses'][0], 'submission': SUBMITTED}
                for wid, wf in self.workflows.items()
                if wf['labels'].get(key) == value
            ]

    def status(self, workflow_id: str) -> str:
        self._record(f'status:{workflow_id}')
        with self.lock:
            statuses = self.workflows[workflow_id]['statuses']
            if len(statuses) > 1:
                statuses.pop(0)
            return statuses[0]

    def metadata(
        self,
        workflow_id: str,
        *,
        include_keys: Any = (),  # noqa: ARG002
    ) -> dict[str, Any]:
        with self.lock:
            status = self.workflows[workflow_id]['statuses'][0]
        return {
            'id': workflow_id,
            'submission': SUBMITTED,
            'status': status,
            'calls': {},
        }

    def outputs(self, workflow_id: str) -> dict[str, Any]:
        self._record(f'outputs:{workflow_id}')
        return {'w.out': f'out-{workflow_id}'}

    def abort(self, workflow_id: str) -> str:
        self._record(f'abort:{workflow_id}')
        return 'Aborting'


class Clock:
    """A clock that only moves when something sleeps; safe across threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.now = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)

    def advance(self, seconds: float) -> None:
        with self.lock:
            self.now += timedelta(seconds=seconds)

    def utcnow(self) -> datetime:
        with self.lock:
            return self.now

    def wait_for(self, event: threading.Event, seconds: float) -> bool:
        self.advance(seconds)
        return event.is_set()


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(common_module, 'sleep', c.advance)
    monkeypatch.setattr(common_module, 'utcnow', c.utcnow)
    monkeypatch.setattr(common_module, 'wait_for', c.wait_for)
    return c


@pytest.fixture
def wdl(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / 'Main.wdl'
    path.write_text('version 1.0\nworkflow w {}\n')
    return path


@pytest.fixture
def fake() -> FakeCromwell:
    return FakeCromwell()


def _request(wdl: pathlib.Path, sample: str, **kwargs: Any) -> WorkflowRequest:
    kwargs.setdefault('inputs', {'w.sample': sample})
    return WorkflowRequest(name=sample, workflow=wdl, options=OPTIONS, **kwargs)


def _run(
    fake: FakeCromwell,
    requests: Sequence[WorkflowRequest],
    **kwargs: Any,
) -> list[WorkflowOutcome]:
    kwargs.setdefault('submit_interval_seconds', 0)
    return run_workflows(requests, client=cast(CromwellClient, fake), **kwargs)


# --- outcomes ----------------------------------------------------------------------


def test_every_workflow_runs_and_outcomes_keep_request_order(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    seen: list[tuple[str, str]] = []
    lock = threading.Lock()

    def on_result(request: WorkflowRequest, result: WorkflowResult) -> None:
        with lock:
            seen.append((request.name, result.workflow_id))

    outcomes = _run(fake, [_request(wdl, s) for s in 'abc'], on_result=on_result)

    assert [o.name for o in outcomes] == ['a', 'b', 'c']
    assert all(o.ok for o in outcomes)
    assert outcomes[1] == WorkflowOutcome(
        'b',
        result=WorkflowResult(
            workflow_id='wf-b',
            outputs={'w.out': 'out-wf-b'},
            reused=False,
        ),
    )
    assert sorted(seen) == [('a', 'wf-a'), ('b', 'wf-b'), ('c', 'wf-c')]


def test_a_failure_does_not_stop_the_others(fake: FakeCromwell, wdl: pathlib.Path):
    fake.scripts['b'] = ['Running', 'Failed']
    called: list[str] = []

    outcomes = _run(
        fake,
        [_request(wdl, s) for s in 'abc'],
        on_result=lambda request, _: called.append(request.name),
    )

    assert [o.ok for o in outcomes] == [True, False, True]
    assert isinstance(outcomes[1].error, WorkflowFailed)
    assert outcomes[1].result is None
    assert sorted(called) == ['a', 'c']


def test_on_result_error_is_recorded_with_the_result(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    def on_result(request: WorkflowRequest, _: WorkflowResult) -> None:
        if request.name == 'b':
            raise RuntimeError('metamist is down')

    outcomes = _run(fake, [_request(wdl, s) for s in 'abc'], on_result=on_result)

    assert [o.ok for o in outcomes] == [True, False, True]
    assert isinstance(outcomes[1].error, RuntimeError)
    assert outcomes[1].result is not None
    assert outcomes[1].result.workflow_id == 'wf-b'


def test_copy_file_is_shared_by_every_workflow(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(fake, 'outputs', lambda wid: {'w.out': f'gs://exec/{wid}.txt'})
    copied: list[tuple[str, str]] = []
    lock = threading.Lock()

    def copy_file(source: str, destination: str) -> None:
        with lock:
            copied.append((source, destination))

    requests = [
        _request(wdl, s, destinations={'w.out': f'gs://dest/{s}.txt'}) for s in 'ab'
    ]
    outcomes = _run(fake, requests, copy_file=copy_file)
    assert all(o.ok for o in outcomes)
    assert sorted(copied) == [
        ('gs://exec/wf-a.txt', 'gs://dest/a.txt'),
        ('gs://exec/wf-b.txt', 'gs://dest/b.txt'),
    ]


def test_existing_workflow_is_reused(fake: FakeCromwell, wdl: pathlib.Path):
    key = prepare_workflow(
        wdl,
        options=OPTIONS,
        inputs={'w.sample': 'a'},
    ).submission_key
    fake.add('old-a', ['Running', 'Succeeded'], key=key)

    outcomes = _run(fake, [_request(wdl, 'a'), _request(wdl, 'b')])

    assert outcomes[0].result is not None
    assert outcomes[0].result.workflow_id == 'old-a'
    assert outcomes[0].result.reused is True
    assert 'submit:a' not in fake.calls
    assert 'submit:b' in fake.calls


def test_no_requests(fake: FakeCromwell):
    assert _run(fake, []) == []
    assert fake.calls == []


# --- checked before anything is sent -----------------------------------------------


def test_repeated_names_raise_before_any_request(fake: FakeCromwell, wdl: pathlib.Path):
    requests = [_request(wdl, 'a'), _request(wdl, 'a', inputs={'w.sample': 'z'})]
    with pytest.raises(ValueError, match=r"unique: \['a'\]"):
        _run(fake, requests)
    assert fake.calls == []


def test_identical_requests_raise_before_any_request(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    same = {'w.sample': 'a'}
    requests = [_request(wdl, 'a', inputs=same), _request(wdl, 'b', inputs=same)]
    with pytest.raises(ValueError, match='identical Workflows'):
        _run(fake, requests)
    assert fake.calls == []


def test_unreadable_request_raises_before_any_request(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    tmp_path: pathlib.Path,
):
    requests = [
        _request(wdl, 'a'),
        WorkflowRequest(name='b', workflow=tmp_path / 'missing.wdl', options=OPTIONS),
    ]
    with pytest.raises(FileNotFoundError):
        _run(fake, requests)
    assert fake.calls == []


def test_max_in_flight_must_be_positive(fake: FakeCromwell, wdl: pathlib.Path):
    with pytest.raises(ValueError, match='max_in_flight'):
        _run(fake, [_request(wdl, 'a')], max_in_flight=0)


# --- pacing ------------------------------------------------------------------------


def test_max_in_flight_one_runs_them_one_after_another(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    _run(fake, [_request(wdl, 'a'), _request(wdl, 'b')], max_in_flight=1)
    assert fake.calls.index('outputs:wf-a') < fake.calls.index('submit:b')


def test_throttle_spaces_out_starts(monkeypatch: pytest.MonkeyPatch, clock: Clock):
    waits: list[float] = []

    def record(event: threading.Event, seconds: float) -> bool:
        waits.append(seconds)
        return event.is_set()

    monkeypatch.setattr(common_module, 'wait_for', record)
    throttle = _Throttle(10, threading.Event())
    for _ in range(3):
        throttle.wait()  # the clock doesn't move, so each one waits a slot longer
    assert waits == [10, 20]

    clock.advance(60)  # long after the last slot: no wait
    throttle.wait()
    assert waits == [10, 20]


# --- stop and interrupts -----------------------------------------------------------


@pytest.mark.parametrize('abort', [True, False])
def test_watch_workflow_stops_when_stop_is_set(fake: FakeCromwell, abort: bool):
    fake.add('wf-x', ['Running'], key='k')
    stop = threading.Event()
    stop.set()
    with pytest.raises(Stopped):
        watch_workflow(
            cast(CromwellClient, fake),
            'wf-x',
            abort_on_interrupt=abort,
            stop=stop,
        )
    assert ('abort:wf-x' in fake.calls) is abort


def test_run_workflow_submits_nothing_once_stopped(
    fake: FakeCromwell,
    wdl: pathlib.Path,
):
    stop = threading.Event()
    stop.set()
    with pytest.raises(Stopped):
        run_workflow(
            wdl,
            options=OPTIONS,
            inputs={'w.sample': 'a'},
            client=cast(CromwellClient, fake),
            stop=stop,
        )
    assert fake.calls == ['query']


def _interrupt_main_when(condition: Any) -> threading.Thread:
    """Send SIGINT to the main thread once ``condition()`` holds."""
    main = threading.main_thread().ident
    assert main is not None

    def watch() -> None:
        while not condition():
            threading.Event().wait(0.005)
        signal.pthread_kill(main, signal.SIGINT)

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    return thread


@pytest.mark.skipif(not hasattr(signal, 'pthread_kill'), reason='needs POSIX')
@pytest.mark.parametrize('abort', [True, False])
def test_interrupt_stops_every_workflow(
    fake: FakeCromwell,
    wdl: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    abort: bool,
):
    # real (short) waits, so a stop wakes the watchers
    monkeypatch.setattr(
        common_module,
        'wait_for',
        lambda event, _seconds: event.wait(0.005),
    )
    for sample in 'abc':
        fake.scripts[sample] = ['Running']  # never finishes

    def both_watched() -> bool:
        return {'status:wf-a', 'status:wf-b'} <= set(fake.calls)

    _interrupt_main_when(both_watched)
    with pytest.raises(KeyboardInterrupt):
        _run(
            fake,
            [_request(wdl, s) for s in 'abc'],
            max_in_flight=2,
            abort_on_interrupt=abort,
        )

    aborted = {c for c in fake.calls if c.startswith('abort:')}
    assert aborted == ({'abort:wf-a', 'abort:wf-b'} if abort else set())
    assert 'submit:c' not in fake.calls  # it was still queued
