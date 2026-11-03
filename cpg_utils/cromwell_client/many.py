"""
Run many Workflows from one process: ``run_workflows``.

Each Workflow runs through ``run_workflow`` in its own thread, so it gets the same
Submission-key lookup, watching, time limit and collecting as a single Workflow. A
Workflow failing never stops the others. The threads mostly sleep between polls, so
one process can comfortably watch dozens of Workflows.
"""

import logging
import threading
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from cpg_utils import Path
from cpg_utils.cromwell_client import common
from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.collect import CopyFile, Destination
from cpg_utils.cromwell_client.errors import Stopped
from cpg_utils.cromwell_client.submission import (
    DEFAULT_REUSE_SUCCEEDED_WITHIN,
    PreparedWorkflow,
    WorkflowResult,
    prepare_workflow,
    run_prepared_workflow,
)
from cpg_utils.cromwell_client.watch import DEFAULT_POLLING, Polling

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkflowRequest:
    """
    One Workflow for ``run_workflows``: ``run_workflow``'s per-Workflow arguments,
    plus a ``name`` that identifies it in logs and in its ``WorkflowOutcome``.
    """

    #: unique within one ``run_workflows`` call, e.g. a sample ID
    name: str
    workflow: str | Path
    options: Mapping[str, Any]
    inputs: Mapping[str, Any] | None = None
    input_files: Sequence[str | Path] = ()
    dependencies: bytes | None = None
    destinations: Mapping[str, Destination | Sequence[Destination]] | None = None
    time_limit_seconds: int | None = None
    polling: Polling = DEFAULT_POLLING


@dataclass(frozen=True)
class WorkflowOutcome:
    """
    What happened to one ``WorkflowRequest``.

    ``result`` is set once the Workflow succeeded and its outputs were collected.
    ``error`` is set if anything failed. Both are set when only ``on_result`` raised.

    >>> WorkflowOutcome('sample-1', error=ValueError('bad')).ok
    False
    """

    name: str
    result: WorkflowResult | None = None
    error: Exception | None = None

    @property
    def ok(self) -> bool:
        """True if the Workflow succeeded, was collected, and ``on_result`` returned."""
        return self.error is None


class _Throttle:
    """
    Spaces out calls to ``wait`` by at least ``interval_seconds``, across threads.

    Each caller reserves the next free slot under the lock, then sleeps outside it.
    """

    def __init__(self, interval_seconds: float, stop: threading.Event) -> None:
        self.interval = timedelta(seconds=interval_seconds)
        self.stop = stop
        self._lock = threading.Lock()
        self._next: datetime | None = None

    def wait(self) -> None:
        if self.interval <= timedelta(0):
            return
        with self._lock:
            now = common.utcnow()
            slot = now if self._next is None else max(now, self._next)
            self._next = slot + self.interval
        delay = (slot - now).total_seconds()
        if delay > 0:
            common.pause(delay, self.stop)


def _check_unique(
    requests: Sequence[WorkflowRequest],
    prepared: Sequence[PreparedWorkflow],
) -> None:
    """Raise ``ValueError`` for repeated names, or for two requests with one key."""
    counts = Counter(r.name for r in requests)
    repeated_names = sorted(name for name, count in counts.items() if count > 1)
    if repeated_names:
        raise ValueError(f'WorkflowRequest names must be unique: {repeated_names}')

    by_key: dict[str, list[str]] = {}
    for request, ready in zip(requests, prepared, strict=True):
        by_key.setdefault(ready.submission_key, []).append(request.name)
    same = [names for names in by_key.values() if len(names) > 1]
    if same:
        # both would find nothing and submit, giving two identical Workflows
        raise ValueError(
            f'These WorkflowRequests would submit identical Workflows: {same}',
        )


@dataclass(frozen=True)
class _Runner:
    """What every request's thread shares: ``run_workflows``' common arguments."""

    client: CromwellClient
    throttle: _Throttle
    stop: threading.Event
    on_result: Callable[[WorkflowRequest, WorkflowResult], None] | None
    abort_on_interrupt: bool
    reuse_existing: bool
    reuse_succeeded_within: timedelta | None
    collect_threads: int
    copy_file: CopyFile | None

    def run(self, request: WorkflowRequest, ready: PreparedWorkflow) -> WorkflowOutcome:
        """Run one request in this thread. Only ``Stopped`` escapes."""
        try:
            self.throttle.wait()
            result = run_prepared_workflow(
                ready,
                client=self.client,
                destinations=request.destinations,
                time_limit_seconds=request.time_limit_seconds,
                abort_on_interrupt=self.abort_on_interrupt,
                reuse_existing=self.reuse_existing,
                reuse_succeeded_within=self.reuse_succeeded_within,
                polling=request.polling,
                collect_threads=self.collect_threads,
                copy_file=self.copy_file,
                stop=self.stop,
            )
        except Stopped:
            raise
        except Exception as e:  # noqa: BLE001 - recorded in the outcome
            logger.error(f'{request.name}: {type(e).__name__}: {e}')
            return WorkflowOutcome(request.name, error=e)
        if self.on_result is not None:
            try:
                self.on_result(request, result)
            except Exception as e:  # recorded in the outcome
                logger.exception(f'{request.name}: on_result failed')
                return WorkflowOutcome(request.name, result=result, error=e)
        return WorkflowOutcome(request.name, result=result)


def run_workflows(
    requests: Sequence[WorkflowRequest],
    *,
    max_in_flight: int | None = None,
    submit_interval_seconds: float = 1.0,
    on_result: Callable[[WorkflowRequest, WorkflowResult], None] | None = None,
    abort_on_interrupt: bool = False,
    reuse_existing: bool = True,
    reuse_succeeded_within: timedelta | None = DEFAULT_REUSE_SUCCEEDED_WITHIN,
    collect_threads: int = 8,
    copy_file: CopyFile | None = None,
    client: CromwellClient | None = None,
) -> list[WorkflowOutcome]:
    """
    Run many Workflows at once, each as ``run_workflow`` would, and return one
    ``WorkflowOutcome`` per request, in request order.

    Every request is read and checked before anything is sent to Cromwell. Then each
    one runs in its own thread: look up or submit, watch, collect, then
    ``on_result``. A failure is recorded in that request's outcome; it never stops
    the others.

    **Interrupts.** If the calling thread is interrupted while waiting (any
    ``BaseException``, such as ``KeyboardInterrupt``, or ``SystemExit`` raised by the
    caller's own SIGTERM handler), every thread is told to stop. Threads that are
    waiting stop at once; nothing new is submitted; queued requests never start.
    With ``abort_on_interrupt``, each thread aborts the Workflow it was watching. This
    function waits for the threads to finish, then re-raises the interrupt. No
    outcomes are returned in that case: ``on_result`` has already run for every
    Workflow that finished. This function installs no signal handlers.

    Args:
        requests: the Workflows to run. Names must be unique, and no two requests may
            have the same Submission key.
        max_in_flight: at most this many requests run at once (the rest wait their
            turn). Defaults to all of them.
        submit_interval_seconds: start each request's lookup-or-submit at least this
            long after the previous one's, so a large batch doesn't arrive at
            Cromwell all at once.
        on_result: called with the request and its result once a Workflow has
            succeeded and been collected, from that Workflow's thread, so it must be
            thread-safe. If it raises, the outcome records the error.
        abort_on_interrupt: on an interrupt, abort the Workflows being watched. Off
            by default, as in ``run_workflow``: a restarted process re-attaches to
            them instead.
        reuse_existing: see ``run_workflow``; applies to every request.
        reuse_succeeded_within: see ``run_workflow``; applies to every request.
        collect_threads: parallel copies per Workflow when collecting.
        copy_file: how to copy one file when collecting (see ``collect_outputs``),
            shared by every thread.
        client: the Cromwell client, shared by every thread. Defaults to
            ``CromwellClient()``.

    Raises:
        ValueError: repeated names or Submission keys, a request with more than five
            input documents, or ``max_in_flight`` below 1. Nothing has been sent.
        OSError: a request's WDL or input file can't be read. Nothing has been sent.

    Example::

        def register(request: WorkflowRequest, result: WorkflowResult) -> None:
            logger.info(f'{request.name}: outputs at {result.outputs}')

        outcomes = run_workflows(
            [
                WorkflowRequest(
                    name=sample,
                    workflow='wdl/Align.wdl',
                    options=options,
                    inputs={'Align.sample': sample},
                    dependencies=deps,
                    destinations={'Align.cram': f'{prefix}/{sample}.cram'},
                )
                for sample in samples
            ],
            max_in_flight=50,
            on_result=register,
            abort_on_interrupt=True,
        )
        failed = [o.name for o in outcomes if not o.ok]
    """
    if max_in_flight is not None and max_in_flight < 1:
        raise ValueError(f'max_in_flight must be at least 1, got {max_in_flight}')
    prepared = [
        prepare_workflow(
            r.workflow,
            options=r.options,
            inputs=r.inputs,
            input_files=r.input_files,
            dependencies=r.dependencies,
        )
        for r in requests
    ]
    _check_unique(requests, prepared)
    if not requests:
        return []
    if client is None:
        client = CromwellClient()

    stop = threading.Event()
    runner = _Runner(
        client=client,
        throttle=_Throttle(submit_interval_seconds, stop),
        stop=stop,
        on_result=on_result,
        abort_on_interrupt=abort_on_interrupt,
        reuse_existing=reuse_existing,
        reuse_succeeded_within=reuse_succeeded_within,
        collect_threads=collect_threads,
        copy_file=copy_file,
    )

    workers = min(max_in_flight or len(requests), len(requests))
    pool = ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix='cromwell-workflow',
    )
    futures = [
        pool.submit(runner.run, r, p) for r, p in zip(requests, prepared, strict=True)
    ]
    try:
        outcomes = [future.result() for future in futures]
    except BaseException as e:
        action = 'aborting' if abort_on_interrupt else 'leaving running'
        logger.warning(
            f'Interrupted ({type(e).__name__}); stopping, and {action}, the Workflows '
            'still being watched',
        )
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    pool.shutdown()

    failed = [o.name for o in outcomes if not o.ok]
    if failed:
        logger.error(f'{len(failed)} of {len(outcomes)} Workflow(s) failed: {failed}')
    else:
        logger.info(f'All {len(outcomes)} Workflow(s) succeeded')
    return outcomes
