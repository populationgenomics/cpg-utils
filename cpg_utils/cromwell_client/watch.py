"""
Watch a Cromwell Workflow until it finishes, and return its Workflow outputs.
"""

import logging
import math
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cpg_utils.cromwell_client import common
from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.common import (
    SUCCEEDED,
    TERMINAL_STATUSES,
    UNSUCCESSFUL_STATUSES,
    ErrorBudget,
    parse_cromwell_datetime,
)
from cpg_utils.cromwell_client.errors import (
    CromwellRequestError,
    CromwellUnavailable,
    WorkflowFailed,
    WorkflowTimedOut,
)
from cpg_utils.cromwell_model import WorkflowMetadataModel

logger = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class Polling:
    """
    How often a watcher polls Cromwell, and how many consecutive errors it tolerates.

    The wait between polls follows a cosine ramp from ``min_seconds`` up to
    ``max_seconds`` over the first ``ramp_seconds`` after the Workflow's submission,
    then stays at ``max_seconds``. The defaults match ``cpg_utils.cromwell``.

    ``Polling(min_seconds=0, max_seconds=0)`` never waits: between polls, between
    retries, or while an ``Aborting`` Workflow finishes. Tests against a fake
    ``CromwellClient`` use it (with ``submit_interval_seconds=0`` for
    ``run_workflows``) instead of patching out sleeping.

    >>> Polling(min_seconds=0, max_seconds=0).interval(600)
    0
    >>> polling = Polling()
    >>> polling.interval(0), polling.interval(600), polling.interval(5000)
    (5, 29, 60)
    """

    #: shortest wait between polls, in seconds
    min_seconds: int = 5
    #: longest wait between polls, in seconds
    max_seconds: int = 60
    #: how long the wait takes to ramp from ``min_seconds`` to ``max_seconds``
    ramp_seconds: int = 1200
    #: give up after this many consecutive errors talking to Cromwell; a successful
    #: status poll resets the count
    max_consecutive_errors: int = 25

    def interval(self, elapsed_seconds: float) -> int:
        """Seconds to wait before the next poll, ``elapsed_seconds`` after submission."""
        factor = elapsed_seconds / self.ramp_seconds
        if factor > 1:
            return self.max_seconds
        return max(
            self.min_seconds,
            int((1 - math.cos(math.pi * factor)) * self.max_seconds // 2),
        )


DEFAULT_POLLING = Polling()


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
    budget: ErrorBudget,
    retry_seconds: int,
) -> datetime:
    """When Cromwell received the Workflow, so re-attaching doesn't restart the clock."""
    meta = budget.retry(
        lambda: client.metadata(workflow_id, include_keys=('submission',)),
        'reading the submission time',
        wait_seconds=retry_seconds,
    )
    submitted = parse_cromwell_datetime(meta.get('submission'))
    if submitted is None:
        logger.warning(
            f'Workflow {workflow_id} has no usable submission time '
            f'({meta.get("submission")!r}); timing from now',
        )
        return common.utcnow()
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
    budget: ErrorBudget,
    retry_seconds: int,
) -> WorkflowTimedOut:
    """Abort the Workflow, retrying within the budget; return the error to raise."""
    logger.warning(
        f'Workflow {workflow_id} exceeded its time limit of {time_limit_seconds}s; '
        'aborting',
    )
    try:
        status = budget.retry(
            lambda: client.abort(workflow_id),
            'aborting',
            wait_seconds=retry_seconds,
            retry_on=(CromwellUnavailable, CromwellRequestError),
        )
    except CromwellUnavailable:  # the budget ran out
        return WorkflowTimedOut(workflow_id, time_limit_seconds, abort_confirmed=False)
    logger.info(f'Abort requested for Workflow {workflow_id}: {status}')
    return WorkflowTimedOut(workflow_id, time_limit_seconds, abort_confirmed=True)


def watch_workflow(
    client: CromwellClient,
    workflow_id: str,
    *,
    time_limit_seconds: int | None = None,
    abort_on_interrupt: bool = False,
    polling: Polling = DEFAULT_POLLING,
    stop: threading.Event | None = None,
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
        time_limit_seconds: abort the Workflow if it is still running this long after
            submission.
        abort_on_interrupt: abort the Workflow if watching stops for any reason other
            than the Workflow failing or timing out (e.g. ``KeyboardInterrupt``, or
            ``stop`` being set). Off by default, so that preempting a watcher doesn't
            kill its Workflow.
        polling: the polling curve and the consecutive-error budget.
        stop: if given, setting it from another thread ends the watch at once,
            rather than at the end of the current wait between polls. The watch then
            raises ``Stopped``.

    Raises:
        WorkflowFailed: the Workflow finished as ``Failed`` or ``Aborted``.
        WorkflowTimedOut: the time limit passed and an abort was requested.
        CromwellUnavailable: ``polling.max_consecutive_errors`` errors in a row.
        CromwellNotFound: Cromwell doesn't know the Workflow ID.
        Stopped: ``stop`` was set.

    Example::

        outputs = watch_workflow(client, workflow_id, time_limit_seconds=6 * 3600)
    """
    try:
        return _watch(
            client,
            workflow_id,
            time_limit_seconds=time_limit_seconds,
            polling=polling,
            stop=stop,
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
    time_limit_seconds: int | None,
    polling: Polling,
    stop: threading.Event | None,
) -> dict[str, Any]:
    budget = ErrorBudget(polling.max_consecutive_errors, workflow_id, stop=stop)
    submitted = _submission_time(client, workflow_id, budget, polling.min_seconds)
    logger.info(f'Watching Workflow {workflow_id}, submitted {submitted.isoformat()}')
    report: dict[str, Any] | None = None
    reported = False

    while True:
        elapsed = (common.utcnow() - submitted).total_seconds()
        wait = polling.interval(elapsed)
        status = budget.retry(
            lambda: client.status(workflow_id),
            'polling status',
            wait_seconds=wait,
        )
        budget.reset()

        if status in TERMINAL_STATUSES and not reported:
            reported = True
            report = _report(client, workflow_id)

        if status == SUCCEEDED:
            outputs = budget.retry(
                lambda: client.outputs(workflow_id),
                'fetching outputs',
                wait_seconds=wait,
            )
            logger.info(f'Workflow {workflow_id} succeeded')
            return outputs

        if status in UNSUCCESSFUL_STATUSES:
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
        common.pause(wait, stop)
