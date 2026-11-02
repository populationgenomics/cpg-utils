"""
Watch a Cromwell Workflow until it finishes, and return its Workflow outputs.
"""

import logging
import math
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.errors import (
    CromwellRequestError,
    CromwellUnavailable,
    WorkflowFailed,
    WorkflowTimedOut,
)
from cpg_utils.cromwell_model import WorkflowMetadataModel

logger = logging.getLogger(__name__)

SUCCEEDED = 'Succeeded'
FAILED_STATUSES = frozenset({'Failed', 'Aborted'})
TERMINAL_STATUSES = FAILED_STATUSES | {SUCCEEDED}

METADATA_REPORT_KEYS = (
    'id',
    'workflowName',
    'status',
    'submission',
    'start',
    'end',
    'failures',
    'calls',
    'executionStatus',
    'shardIndex',
    'attempt',
    'jobId',
    'returnCode',
    'stdout',
    'stderr',
    'callCaching',
)


def _sleep(seconds: float) -> None:
    """Indirection so tests can replace sleeping."""
    time.sleep(seconds)


def _utcnow() -> datetime:
    """Indirection so tests can control the clock."""
    return datetime.now(UTC)


def poll_interval(
    elapsed_seconds: float,
    min_seconds: int,
    max_seconds: int,
    ramp_seconds: int,
) -> int:
    """
    Seconds to wait before the next poll: a cosine ramp from ``min_seconds`` up to
    ``max_seconds`` over the first ``ramp_seconds``, then flat.

    >>> poll_interval(0, 5, 60, 1200), poll_interval(600, 5, 60, 1200)
    (5, 29)
    >>> poll_interval(5000, 5, 60, 1200)
    60
    """
    factor = elapsed_seconds / ramp_seconds
    if factor > 1:
        return max_seconds
    return max(min_seconds, int((1 - math.cos(math.pi * factor)) * max_seconds // 2))


class _ErrorBudget:
    """Counts consecutive errors, raising once ``limit`` of them have happened."""

    def __init__(self, limit: int, workflow_id: str) -> None:
        self.limit = limit
        self.remaining = limit
        self.workflow_id = workflow_id

    def reset(self) -> None:
        self.remaining = self.limit

    def spend(self, error: Exception, what: str) -> None:
        self.remaining -= 1
        if self.remaining <= 0:
            raise CromwellUnavailable(
                f'Giving up on Workflow {self.workflow_id} after {self.limit} '
                f'consecutive errors; last one while {what}: {error}',
            ) from error
        logger.warning(
            f'Error while {what} for Workflow {self.workflow_id} '
            f'({self.remaining} retries left): {error}',
        )


def _parse_cromwell_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _flatten_failures(failures: Iterable[Any]) -> list[str]:
    """Collect ``message`` strings through nested ``causedBy``, without duplicates."""
    messages: list[str] = []

    def visit(items: Iterable[Any]) -> None:
        for failure in items or ():
            if not isinstance(failure, dict):
                continue
            message = failure.get('message')
            if message and message not in messages:
                messages.append(message)
            visit(failure.get('causedBy') or ())

    visit(failures)
    return messages


def _submission_time(
    client: CromwellClient,
    workflow_id: str,
    budget: _ErrorBudget,
    retry_seconds: int,
) -> datetime:
    """When Cromwell received the Workflow, so re-attaching doesn't restart the clock."""
    while True:
        try:
            meta = client.metadata(workflow_id, include_keys=('submission',))
            break
        except CromwellUnavailable as e:
            budget.spend(e, 'reading the submission time')
            _sleep(retry_seconds)
    submitted = _parse_cromwell_datetime(meta.get('submission'))
    if submitted is None:
        logger.warning(
            f'Workflow {workflow_id} has no usable submission time '
            f'({meta.get("submission")!r}); timing from now',
        )
        return _utcnow()
    return submitted


def _report(client: CromwellClient, workflow_id: str) -> dict[str, Any] | None:
    """Log the finish report. Never raises; returns the metadata if it was fetched."""
    try:
        meta = client.metadata(workflow_id, include_keys=METADATA_REPORT_KEYS)
    except Exception as e:  # noqa: BLE001 - the report must never fail the watch
        logger.warning(f'Workflow {workflow_id}: failed to collect run metadata: {e}')
        return None
    try:
        model = WorkflowMetadataModel.parse({'calls': {}, **meta})
        logger.info(
            f'Workflow {workflow_id} report:\n{model.display(monochrome=True)}',
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f'Workflow {workflow_id}: failed to render run metadata: {e}')
    return meta


def _abort_for_time_limit(
    client: CromwellClient,
    workflow_id: str,
    time_limit_seconds: int,
    budget: _ErrorBudget,
    retry_seconds: int,
) -> WorkflowTimedOut:
    """Abort the Workflow, retrying within the budget; return the error to raise."""
    logger.warning(
        f'Workflow {workflow_id} exceeded its time limit of {time_limit_seconds}s; '
        'aborting',
    )
    while True:
        try:
            status = client.abort(workflow_id)
        except (CromwellUnavailable, CromwellRequestError) as e:
            try:
                budget.spend(e, 'aborting')
            except CromwellUnavailable:
                return WorkflowTimedOut(
                    workflow_id,
                    time_limit_seconds,
                    abort_confirmed=False,
                )
            _sleep(retry_seconds)
            continue
        logger.info(f'Abort requested for Workflow {workflow_id}: {status}')
        return WorkflowTimedOut(workflow_id, time_limit_seconds, abort_confirmed=True)


def watch_workflow(
    client: CromwellClient,
    workflow_id: str,
    *,
    min_poll_interval: int = 5,
    max_poll_interval: int = 60,
    exponential_decrease_seconds: int = 1200,
    max_sequential_exception_count: int = 25,
    time_limit_seconds: int | None = None,
    abort_on_interrupt: bool = False,
) -> dict[str, Any]:
    """
    Poll a Workflow until it finishes, and return its Workflow outputs.

    Works for any Workflow ID, including one submitted by an earlier process: the
    polling curve and the time limit both count from the Workflow's own
    ``submission`` time. When the Workflow finishes, a report built from its metadata
    is logged at INFO.

    Args:
        client: the Cromwell client.
        workflow_id: the Workflow to watch.
        min_poll_interval: shortest wait between polls, in seconds.
        max_poll_interval: longest wait between polls, in seconds.
        exponential_decrease_seconds: how long the wait takes to ramp from min to max.
        max_sequential_exception_count: give up after this many consecutive errors
            talking to Cromwell. A successful status poll resets the count.
        time_limit_seconds: abort the Workflow if it is still running this long after
            submission.
        abort_on_interrupt: abort the Workflow if watching stops for any reason other
            than the Workflow failing or timing out (e.g. ``KeyboardInterrupt``). Off
            by default, so that preempting a watcher doesn't kill its Workflow.

    Raises:
        WorkflowFailed: the Workflow finished as ``Failed`` or ``Aborted``.
        WorkflowTimedOut: the time limit passed and an abort was requested.
        CromwellUnavailable: the error budget ran out.
        CromwellNotFound: Cromwell doesn't know the Workflow ID.

    Example::

        outputs = watch_workflow(client, workflow_id, time_limit_seconds=6 * 3600)
    """
    try:
        return _watch(
            client,
            workflow_id,
            min_poll_interval=min_poll_interval,
            max_poll_interval=max_poll_interval,
            exponential_decrease_seconds=exponential_decrease_seconds,
            max_sequential_exception_count=max_sequential_exception_count,
            time_limit_seconds=time_limit_seconds,
        )
    except (WorkflowFailed, WorkflowTimedOut):
        raise
    except BaseException as e:
        if abort_on_interrupt:
            logger.warning(
                f'Watching Workflow {workflow_id} stopped ({type(e).__name__}); '
                'aborting it because abort_on_interrupt is set',
            )
            try:
                status = client.abort(workflow_id)
                logger.warning(f'Abort requested for Workflow {workflow_id}: {status}')
            except Exception as abort_error:  # noqa: BLE001 - best effort
                logger.warning(
                    f'Could not abort Workflow {workflow_id}: {abort_error}',
                )
        raise


def _watch(
    client: CromwellClient,
    workflow_id: str,
    *,
    min_poll_interval: int,
    max_poll_interval: int,
    exponential_decrease_seconds: int,
    max_sequential_exception_count: int,
    time_limit_seconds: int | None,
) -> dict[str, Any]:
    budget = _ErrorBudget(max_sequential_exception_count, workflow_id)
    submitted = _submission_time(client, workflow_id, budget, min_poll_interval)
    logger.info(f'Watching Workflow {workflow_id}, submitted {submitted.isoformat()}')
    report: dict[str, Any] | None = None
    reported = False

    while True:
        elapsed = (_utcnow() - submitted).total_seconds()
        wait = poll_interval(
            elapsed,
            min_poll_interval,
            max_poll_interval,
            exponential_decrease_seconds,
        )
        try:
            status = client.status(workflow_id)
        except CromwellUnavailable as e:
            budget.spend(e, 'polling status')
            _sleep(wait)
            continue
        budget.reset()

        if status in TERMINAL_STATUSES and not reported:
            reported = True
            report = _report(client, workflow_id)

        if status == SUCCEEDED:
            while True:
                try:
                    outputs = client.outputs(workflow_id)
                except CromwellUnavailable as e:
                    budget.spend(e, 'fetching outputs')
                    _sleep(wait)
                    continue
                logger.info(f'Workflow {workflow_id} succeeded')
                return outputs

        if status in FAILED_STATUSES:
            failures = _flatten_failures((report or {}).get('failures') or ())
            logger.error(f'Workflow {workflow_id} finished with status {status}')
            raise WorkflowFailed(workflow_id, status, failures)

        # The limit only applies to an unfinished Workflow: re-attaching to one that
        # already succeeded must not try to abort it.
        if time_limit_seconds is not None and elapsed >= time_limit_seconds:
            raise _abort_for_time_limit(
                client,
                workflow_id,
                time_limit_seconds,
                budget,
                wait,
            )

        logger.info(f'Workflow {workflow_id} is {status} (next poll in {wait}s)')
        _sleep(wait)
