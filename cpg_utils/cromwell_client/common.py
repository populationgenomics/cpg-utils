"""
Pieces shared by more than one module in this package: Cromwell status names, the
input-document limit, shared defaults, the clock and sleep indirections, timestamp
parsing, and the consecutive-error budget used when retrying Cromwell calls.

Internal to the package: nothing here is re-exported from ``cpg_utils.cromwell_client``.
"""

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar

from cpg_utils.cromwell_client.errors import CromwellUnavailable

logger = logging.getLogger(__name__)

_T = TypeVar('_T')

# Cromwell Workflow statuses.
SUCCEEDED = 'Succeeded'
FAILED = 'Failed'
ABORTED = 'Aborted'
ABORTING = 'Aborting'
ACTIVE_STATUSES = frozenset({'Submitted', 'Running', 'On Hold'})
UNSUCCESSFUL_STATUSES = frozenset({FAILED, ABORTED})
TERMINAL_STATUSES = UNSUCCESSFUL_STATUSES | {SUCCEEDED}

# watch_workflow defaults that run_workflow also needs before it starts watching.
DEFAULT_MAX_POLL_INTERVAL = 60
DEFAULT_MAX_SEQUENTIAL_EXCEPTION_COUNT = 25

MAX_INPUT_DOCUMENTS = 5


def check_input_document_count(count: int) -> None:
    """
    Raise ``ValueError`` if Cromwell wouldn't accept this many input documents.

    >>> check_input_document_count(5)
    >>> check_input_document_count(6)
    Traceback (most recent call last):
    ...
    ValueError: Cromwell accepts at most 5 input documents, got 6
    """
    if count > MAX_INPUT_DOCUMENTS:
        raise ValueError(
            f'Cromwell accepts at most {MAX_INPUT_DOCUMENTS} input documents, '
            f'got {count}',
        )


def sleep(seconds: float) -> None:
    """Indirection so tests can replace sleeping (patch ``common.sleep``)."""
    time.sleep(seconds)


def utcnow() -> datetime:
    """Indirection so tests can control the clock (patch ``common.utcnow``)."""
    return datetime.now(UTC)


def parse_cromwell_datetime(value: Any) -> datetime | None:
    """
    Parse a Cromwell timestamp as an aware UTC datetime, or return None.

    >>> parse_cromwell_datetime('2026-11-03T10:00:00.000Z')
    datetime.datetime(2026, 11, 3, 10, 0, tzinfo=datetime.timezone.utc)
    >>> parse_cromwell_datetime('not a date') is None
    True
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class ErrorBudget:
    """Counts consecutive errors, raising ``CromwellUnavailable`` once ``limit`` happen."""

    def __init__(self, limit: int, workflow_id: str) -> None:
        self.limit = limit
        self.remaining = limit
        self.workflow_id = workflow_id

    def reset(self) -> None:
        self.remaining = self.limit

    def spend(self, error: Exception, what: str) -> None:
        """Record one error; raise ``CromwellUnavailable`` if the budget is used up."""
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

    def retry(
        self,
        call: Callable[[], _T],
        what: str,
        *,
        wait_seconds: float,
        retry_on: tuple[type[Exception], ...] = (CromwellUnavailable,),
    ) -> _T:
        """
        Return ``call()``, retrying after ``wait_seconds`` on any ``retry_on`` error
        until the budget runs out. Success doesn't reset the budget; the caller decides
        which successes count as recovery.
        """
        while True:
            try:
                return call()
            except retry_on as e:
                self.spend(e, what)
                sleep(wait_seconds)
