"""
Submission keys, and ``run_workflow``: look up or submit, then watch, then collect.

A Submission key is a content hash of everything that decides what a Workflow does.
It's recorded on each submitted Workflow as the Cromwell label
``cpg-submission-key``, so a watcher that is restarted (e.g. after preemption) finds
the Workflow it already submitted instead of submitting a duplicate.
"""

import hashlib
import json
import logging
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from cpg_utils import Path, to_path
from cpg_utils.cromwell_client import common
from cpg_utils.cromwell_client.client import CromwellClient
from cpg_utils.cromwell_client.collect import CopyFile, Destination, collect_outputs
from cpg_utils.cromwell_client.common import (
    ABORTING,
    ACTIVE_STATUSES,
    SUCCEEDED,
    UNSUCCESSFUL_STATUSES,
    ErrorBudget,
    check_input_document_count,
    parse_cromwell_datetime,
)
from cpg_utils.cromwell_client.errors import CromwellError, Stopped
from cpg_utils.cromwell_client.watch import DEFAULT_POLLING, Polling, watch_workflow

logger = logging.getLogger(__name__)

SUBMISSION_KEY_LABEL = 'cpg-submission-key'
# Options that don't change what the Workflow computes: the credentials rotate, and
# google_labels carry per-run values such as the AR GUID.
KEY_EXCLUDED_OPTIONS = ('user_service_account_json', 'google_labels')
_KEY_LENGTH = 63  # the maximum length of a Cromwell label value


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def serialise_inputs(inputs: Mapping[str, Any]) -> bytes:
    """
    Serialise an inputs dict canonically, so its key order never changes the bytes.

    >>> serialise_inputs({'b': 1, 'a': [1, 2]})
    b'{"a":[1,2],"b":1}'
    """
    return json.dumps(inputs, sort_keys=True, separators=(',', ':')).encode()


def compute_submission_key(
    *,
    workflow_source: bytes,
    dependencies: bytes | None,
    input_documents: Sequence[bytes],
    options: Mapping[str, Any],
) -> str:
    """
    Return the Submission key: 63 hex characters identifying this Workflow's content.

    The key covers the WDL, the dependencies zip, each input document (in the order it
    is sent, since later documents override earlier ones), and the Workflow options
    except ``user_service_account_json`` and ``google_labels``.

    >>> key = compute_submission_key(
    ...     workflow_source=b'version 1.0',
    ...     dependencies=None,
    ...     input_documents=[],
    ...     options={'backend': 'batch'},
    ... )
    >>> len(key)
    63
    """
    payload = {
        'workflow': _sha256(workflow_source),
        'dependencies': _sha256(dependencies) if dependencies is not None else None,
        'inputs': [_sha256(document) for document in input_documents],
        'options': {
            key: value
            for key, value in options.items()
            if key not in KEY_EXCLUDED_OPTIONS
        },
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return _sha256(canonical.encode())[:_KEY_LENGTH]


ABORTING_WAIT_SECONDS = 900
# How long a Succeeded Workflow stays reusable. Its outputs may have been deleted by a
# bucket lifecycle rule since, or the files its inputs point to rewritten in place.
DEFAULT_REUSE_SUCCEEDED_WITHIN = timedelta(weeks=1)


@dataclass(frozen=True)
class WorkflowResult:
    """What ``run_workflow`` returns."""

    workflow_id: str
    #: every Workflow output where it now lives: a collected output's destination
    #: path(s), otherwise the raw value Cromwell reported. The original locations of
    #: collected outputs are available from ``client.outputs(workflow_id)``.
    outputs: dict[str, Any]
    #: True if an existing Workflow was found via the Submission key
    reused: bool


def _submission_sort_key(result: Mapping[str, Any]) -> datetime:
    """Newest first under ``max``; results without a parsable time sort oldest."""
    return parse_cromwell_datetime(result.get('submission')) or datetime.min.replace(
        tzinfo=UTC,
    )


def _wait_for_abort(
    client: CromwellClient,
    workflow_id: str,
    *,
    interval_seconds: int,
    max_sequential_exception_count: int,
    stop: threading.Event | None,
) -> str:
    """
    Poll until the Workflow stops ``Aborting``; return its final status.

    Transient ``CromwellUnavailable`` errors are retried within an error budget, as
    ``watch_workflow`` does; time spent retrying counts towards the wait limit.
    """
    budget = ErrorBudget(max_sequential_exception_count, workflow_id, stop=stop)
    started = common.utcnow()
    status = ABORTING
    while status == ABORTING:
        if (common.utcnow() - started).total_seconds() >= ABORTING_WAIT_SECONDS:
            raise CromwellError(
                f'Workflow {workflow_id} with the same Submission key is still '
                f'Aborting after {ABORTING_WAIT_SECONDS}s; not submitting a new one',
            )
        common.pause(interval_seconds, stop)
        status = budget.retry(
            lambda: client.status(workflow_id),
            'waiting for an Aborting Workflow',
            wait_seconds=interval_seconds,
        )
        budget.reset()
    return status


def _finished_at(result: Mapping[str, Any]) -> datetime | None:
    """When a query result finished (``end``), falling back to its ``submission``."""
    return parse_cromwell_datetime(result.get('end')) or parse_cromwell_datetime(
        result.get('submission'),
    )


def _find_existing(
    client: CromwellClient,
    key: str,
    *,
    polling: Polling,
    reuse_succeeded_within: timedelta | None,
    stop: threading.Event | None,
) -> str | None:
    """Return the ID of a Workflow to attach to, or None if a new one is needed."""
    results = client.query_by_label(SUBMISSION_KEY_LABEL, key)
    if not results:
        return None
    newest = max(results, key=_submission_sort_key)
    workflow_id, status = newest['id'], newest.get('status', '')
    newest_of = f' (newest of {len(results)})' if len(results) > 1 else ''
    logger.info(
        f'Found Workflow {workflow_id} ({status}) with Submission key {key}{newest_of}',
    )
    if status == ABORTING:
        status = _wait_for_abort(
            client,
            workflow_id,
            interval_seconds=polling.max_seconds,
            max_sequential_exception_count=polling.max_consecutive_errors,
            stop=stop,
        )
    if status in UNSUCCESSFUL_STATUSES:
        logger.info(f'Workflow {workflow_id} is {status}; submitting a new one')
        return None
    if status == SUCCEEDED and reuse_succeeded_within is not None:
        finished = _finished_at(newest)
        if finished is None or common.utcnow() - finished > reuse_succeeded_within:
            logger.info(
                f'Workflow {workflow_id} succeeded more than {reuse_succeeded_within} '
                f'ago ({finished.isoformat() if finished else "at an unknown time"}); '
                'submitting a new one',
            )
            return None
    if status not in ACTIVE_STATUSES and status != SUCCEEDED:
        logger.warning(
            f'Workflow {workflow_id} has unexpected status {status!r}; attaching to it',
        )
    return workflow_id


@dataclass(frozen=True)
class PreparedWorkflow:
    """Everything a submission sends, read up front, and its Submission key."""

    workflow_source: bytes
    workflow_filename: str
    input_documents: tuple[bytes, ...]
    dependencies: bytes | None
    options: Mapping[str, Any]
    submission_key: str


def prepare_workflow(
    workflow: str | Path,
    *,
    options: Mapping[str, Any],
    inputs: Mapping[str, Any] | None = None,
    input_files: Sequence[str | Path] = (),
    dependencies: bytes | None = None,
) -> PreparedWorkflow:
    """
    Read the WDL and inputs, and compute the Submission key. Makes no requests.

    Input documents are ``input_files`` in order, then ``inputs`` serialised with
    ``serialise_inputs``, so ``inputs`` wins where they overlap.

    Raises:
        ValueError: more than five input documents.
        OSError: the WDL or an input file can't be read.
    """
    workflow_path = to_path(workflow)
    workflow_source = workflow_path.read_bytes()
    input_documents = [to_path(f).read_bytes() for f in input_files]
    if inputs is not None:
        input_documents.append(serialise_inputs(inputs))
    check_input_document_count(len(input_documents))

    key = compute_submission_key(
        workflow_source=workflow_source,
        dependencies=dependencies,
        input_documents=input_documents,
        options=options,
    )
    return PreparedWorkflow(
        workflow_source=workflow_source,
        workflow_filename=workflow_path.name,
        input_documents=tuple(input_documents),
        dependencies=dependencies,
        options=options,
        submission_key=key,
    )


def run_prepared_workflow(
    prepared: PreparedWorkflow,
    *,
    client: CromwellClient,
    destinations: Mapping[str, Destination | Sequence[Destination]] | None = None,
    time_limit_seconds: int | None = None,
    abort_on_interrupt: bool = False,
    reuse_existing: bool = True,
    reuse_succeeded_within: timedelta | None = DEFAULT_REUSE_SUCCEEDED_WITHIN,
    polling: Polling = DEFAULT_POLLING,
    collect_threads: int = 8,
    copy_file: CopyFile | None = None,
    stop: threading.Event | None = None,
) -> WorkflowResult:
    """``run_workflow`` for a ``PreparedWorkflow``; the arguments are the same."""
    key = prepared.submission_key
    workflow_id = None
    if reuse_existing:
        workflow_id = _find_existing(
            client,
            key,
            polling=polling,
            reuse_succeeded_within=reuse_succeeded_within,
            stop=stop,
        )
    reused = workflow_id is not None
    if workflow_id is None:
        if stop is not None and stop.is_set():
            raise Stopped('stop requested before submitting')
        workflow_id = client.submit(
            prepared.workflow_source,
            workflow_filename=prepared.workflow_filename,
            input_documents=list(prepared.input_documents),
            options=prepared.options,
            dependencies=prepared.dependencies,
            labels={SUBMISSION_KEY_LABEL: key},
        )
        logger.info(f'Submitted Workflow {workflow_id} with Submission key {key}')
    else:
        logger.info(f'Attaching to Workflow {workflow_id} (Submission key {key})')

    raw_outputs = watch_workflow(
        client,
        workflow_id,
        time_limit_seconds=time_limit_seconds,
        abort_on_interrupt=abort_on_interrupt,
        polling=polling,
        stop=stop,
    )
    outputs = (
        collect_outputs(
            raw_outputs,
            destinations,
            threads=collect_threads,
            copy_file=copy_file,
        )
        if destinations is not None
        else raw_outputs
    )
    return WorkflowResult(workflow_id=workflow_id, outputs=outputs, reused=reused)


def run_workflow(
    workflow: str | Path,
    *,
    options: Mapping[str, Any],
    inputs: Mapping[str, Any] | None = None,
    input_files: Sequence[str | Path] = (),
    dependencies: bytes | None = None,
    destinations: Mapping[str, Destination | Sequence[Destination]] | None = None,
    time_limit_seconds: int | None = None,
    abort_on_interrupt: bool = False,
    reuse_existing: bool = True,
    reuse_succeeded_within: timedelta | None = DEFAULT_REUSE_SUCCEEDED_WITHIN,
    polling: Polling = DEFAULT_POLLING,
    collect_threads: int = 8,
    copy_file: CopyFile | None = None,
    client: CromwellClient | None = None,
    stop: threading.Event | None = None,
) -> WorkflowResult:
    """
    Run a Workflow to completion: find or submit it, watch it, and collect its outputs.

    The Submission key (see :func:`compute_submission_key`) is attached to every
    submission as the Cromwell label ``cpg-submission-key``. With ``reuse_existing``,
    the newest Workflow carrying the same key is looked up first:

    - ``Submitted``/``Running``/``On Hold``: attach to it.
    - ``Succeeded`` within ``reuse_succeeded_within``: attach to it, which returns its
      outputs at once. Older than that: submit.
    - ``Aborting``: wait (up to 15 minutes) for it to become ``Aborted``, then submit.
    - ``Failed``/``Aborted``, or nothing found: submit.

    Args:
        workflow: path to the main WDL file.
        options: Workflow options, e.g. from ``build_workflow_options``.
        inputs: an inputs dict, sent as the last input document (so it overrides
            values from ``input_files``).
        input_files: inputs JSON files, sent first, in order.
        dependencies: a dependencies zip, e.g. from ``zip_dependencies``.
        destinations: if given, passed to ``collect_outputs`` after success, and
            the result's ``outputs`` point at the copies.
        time_limit_seconds: abort the Workflow if it is still running this long after
            its submission (see ``watch_workflow``).
        abort_on_interrupt: abort the Workflow if watching stops for a reason other
            than the Workflow failing or timing out (see ``watch_workflow``).
        reuse_existing: look up an existing Workflow by Submission key first.
        reuse_succeeded_within: only reuse a ``Succeeded`` Workflow that finished at
            most this long ago; ``None`` reuses one of any age. Active Workflows are
            always reused.
        polling: the polling curve and consecutive-error budget.
        collect_threads: parallel copies when collecting.
        copy_file: how to copy one file when collecting (see ``collect_outputs``).
        client: the Cromwell client. Defaults to ``CromwellClient()``.
        stop: if given, setting it from another thread ends any wait at once and
            raises ``Stopped``, and nothing new is submitted (see ``watch_workflow``).

    Raises:
        ValueError: more than five input documents.
        CromwellError: see ``watch_workflow`` and ``collect_outputs``. A lookup that
            fails raises rather than risking a duplicate submission.

    Example::

        result = run_workflow(
            'wdl/Align.wdl',
            options=options,
            inputs={'Align.sample': 'sample-1'},
            dependencies=zip_dependencies('wdl', ['tasks']),
            destinations={'Align.cram': f'{prefix}/sample-1.cram'},
            time_limit_seconds=12 * 3600,
        )
    """
    if client is None:
        client = CromwellClient()
    prepared = prepare_workflow(
        workflow,
        options=options,
        inputs=inputs,
        input_files=input_files,
        dependencies=dependencies,
    )
    return run_prepared_workflow(
        prepared,
        client=client,
        destinations=destinations,
        time_limit_seconds=time_limit_seconds,
        abort_on_interrupt=abort_on_interrupt,
        reuse_existing=reuse_existing,
        reuse_succeeded_within=reuse_succeeded_within,
        polling=polling,
        collect_threads=collect_threads,
        copy_file=copy_file,
        stop=stop,
    )
