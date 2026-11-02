"""
Exceptions raised by ``cpg_utils.cromwell_client``.

Everything derives from :class:`CromwellError`, so callers can catch the whole family
with one ``except`` clause, or pick out the cases they can act on.
"""


class CromwellError(Exception):
    """Base class for every error raised by the Cromwell client."""


class CromwellUnavailable(CromwellError):  # noqa: N818
    """
    The Cromwell server couldn't be reached or gave an unusable answer.

    Raised for a connection error, timeout, HTTP 5xx/408/429/401, a non-JSON body, or
    when a watcher's consecutive-error budget is exhausted. Retrying later is sensible.
    """


class CromwellRequestError(CromwellError):
    """Cromwell rejected the request with a 4xx other than 401/408/429."""

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class CromwellNotFound(CromwellRequestError):  # noqa: N818
    """Cromwell answered 404, e.g. for an unknown Workflow ID."""

    def __init__(self, message: str, *, status_code: int = 404) -> None:
        super().__init__(message, status_code=status_code)


class WorkflowFailed(CromwellError):  # noqa: N818
    """The Workflow finished with status ``Failed`` or ``Aborted``."""

    def __init__(
        self,
        workflow_id: str,
        status: str,
        failures: list[str] | None = None,
    ) -> None:
        self.workflow_id = workflow_id
        self.status = status
        self.failures = list(failures or [])
        message = f'Workflow {workflow_id} finished with status {status}'
        if self.failures:
            message += ': ' + '; '.join(self.failures)
        super().__init__(message)


class WorkflowTimedOut(CromwellError):  # noqa: N818
    """The Workflow ran past its time limit, and an abort was requested."""

    def __init__(
        self,
        workflow_id: str,
        time_limit_seconds: int,
        *,
        abort_confirmed: bool,
    ) -> None:
        self.workflow_id = workflow_id
        self.time_limit_seconds = time_limit_seconds
        self.abort_confirmed = abort_confirmed
        outcome = (
            'abort requested'
            if abort_confirmed
            else 'the abort request did not succeed'
        )
        super().__init__(
            f'Workflow {workflow_id} exceeded its time limit of '
            f'{time_limit_seconds}s ({outcome})',
        )


class CollectError(CromwellError):
    """One or more Workflow outputs couldn't be planned or copied."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        super().__init__(
            f'{len(self.problems)} problem(s) collecting outputs:\n'
            + '\n'.join(f'  - {p}' for p in self.problems),
        )
