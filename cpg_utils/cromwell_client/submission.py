"""
Submission keys, and ``run_workflow``: look up or submit, then watch, then collect.

A Submission key is a content hash of everything that decides what a Workflow does.
It's recorded on each submitted Workflow as the Cromwell label
``cpg-submission-key``, so a watcher that is restarted (e.g. after preemption) finds
the Workflow it already submitted instead of submitting a duplicate.
"""

import hashlib
import inspect
import json
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from cpg_utils import Path, to_path
from cpg_utils.cromwell_client.client import MAX_INPUT_DOCUMENTS, CromwellClient
from cpg_utils.cromwell_client.collect import Destination, collect_outputs
from cpg_utils.cromwell_client.errors import CromwellError
from cpg_utils.cromwell_client.watch import watch_workflow

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


ACTIVE_STATUSES = frozenset({'Submitted', 'Running', 'On Hold'})
RESUBMIT_STATUSES = frozenset({'Failed', 'Aborted'})
ABORTING = 'Aborting'
SUCCEEDED = 'Succeeded'
ABORTING_WAIT_SECONDS = 900

_WATCH_KWARGS = frozenset(
    name
    for name, parameter in inspect.signature(watch_workflow).parameters.items()
    if parameter.kind is inspect.Parameter.KEYWORD_ONLY
)


def _sleep(seconds: float) -> None:
    """Indirection so tests can replace sleeping."""
    time.sleep(seconds)


@dataclass(frozen=True)
class WorkflowResult:
    """What ``run_workflow`` returns."""

    workflow_id: str
    #: the raw Workflow outputs, as Cromwell reports them
    outputs: dict[str, Any]
    #: the ``collect_outputs`` result; ``{}`` when no destinations were given
    collected: dict[str, Any]
    #: True if an existing Workflow was found via the Submission key
    reused: bool


def _submission_sort_key(result: Mapping[str, Any]) -> datetime:
    value = result.get('submission')
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            pass
        else:
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.min.replace(tzinfo=UTC)


def _wait_for_abort(
    client: CromwellClient,
    workflow_id: str,
    *,
    interval_seconds: int,
) -> str:
    """Poll until the Workflow stops ``Aborting``; return its final status."""
    waited = 0
    status = ABORTING
    while status == ABORTING:
        if waited >= ABORTING_WAIT_SECONDS:
            raise CromwellError(
                f'Workflow {workflow_id} with the same Submission key is still '
                f'Aborting after {ABORTING_WAIT_SECONDS}s; not submitting a new one',
            )
        _sleep(interval_seconds)
        waited += interval_seconds
        status = client.status(workflow_id)
    return status


def _find_existing(
    client: CromwellClient,
    key: str,
    *,
    poll_seconds: int,
) -> str | None:
    """Return the ID of a Workflow to attach to, or None if a new one is needed."""
    results = client.query_by_label(SUBMISSION_KEY_LABEL, key)
    if not results:
        return None
    newest = max(results, key=_submission_sort_key)
    workflow_id, status = newest['id'], newest.get('status', '')
    logger.info(
        f'Found Workflow {workflow_id} ({status}) with Submission key {key}'
        + (f' (newest of {len(results)})' if len(results) > 1 else ''),
    )
    if status == ABORTING:
        status = _wait_for_abort(client, workflow_id, interval_seconds=poll_seconds)
    if status in RESUBMIT_STATUSES:
        logger.info(f'Workflow {workflow_id} is {status}; submitting a new one')
        return None
    if status not in ACTIVE_STATUSES and status != SUCCEEDED:
        logger.warning(
            f'Workflow {workflow_id} has unexpected status {status!r}; attaching to it',
        )
    return workflow_id


def run_workflow(
    client: CromwellClient,
    workflow: str | Path,
    *,
    options: Mapping[str, Any],
    inputs: Mapping[str, Any] | None = None,
    input_files: Sequence[str | Path] = (),
    dependencies: bytes | None = None,
    destinations: Mapping[str, Destination | Sequence[Destination]] | None = None,
    reuse_existing: bool = True,
    collect_threads: int = 8,
    **watch_kwargs: Any,
) -> WorkflowResult:
    """
    Run a Workflow to completion: find or submit it, watch it, and collect its outputs.

    The Submission key (see :func:`compute_submission_key`) is attached to every
    submission as the Cromwell label ``cpg-submission-key``. With ``reuse_existing``,
    a Workflow carrying the same key is looked up first:

    - ``Submitted``/``Running``/``On Hold``/``Succeeded``: attach to it.
    - ``Aborting``: wait (up to 15 minutes) for it to become ``Aborted``, then submit.
    - ``Failed``/``Aborted``, or nothing found: submit.

    Args:
        client: the Cromwell client.
        workflow: path to the main WDL file.
        options: Workflow options, e.g. from ``build_workflow_options``.
        inputs: an inputs dict, sent as the last input document (so it overrides
            values from ``input_files``).
        input_files: inputs JSON files, sent first, in order.
        dependencies: a dependencies zip, e.g. from ``zip_dependencies``.
        destinations: if given, passed to ``collect_outputs`` after success.
        reuse_existing: look up an existing Workflow by Submission key first.
        collect_threads: parallel copies when collecting.
        **watch_kwargs: passed to ``watch_workflow``, e.g. ``time_limit_seconds``.

    Raises:
        TypeError: an unknown ``watch_kwargs`` key, before any request is made.
        ValueError: more than five input documents.
        CromwellError: see ``watch_workflow`` and ``collect_outputs``. A lookup that
            fails raises rather than risking a duplicate submission.

    Example::

        result = run_workflow(
            client,
            'wdl/Align.wdl',
            options=options,
            inputs={'Align.sample': 'sample-1'},
            dependencies=zip_dependencies('wdl', ['tasks']),
            destinations={'Align.cram': f'{prefix}/sample-1.cram'},
            time_limit_seconds=12 * 3600,
        )
    """
    unknown = sorted(set(watch_kwargs) - _WATCH_KWARGS)
    if unknown:
        raise TypeError(
            f'run_workflow() got unexpected watch_workflow argument(s): {unknown}',
        )

    workflow_path = to_path(workflow)
    workflow_source = workflow_path.read_bytes()
    input_documents = [to_path(f).read_bytes() for f in input_files]
    if inputs is not None:
        input_documents.append(serialise_inputs(inputs))
    if len(input_documents) > MAX_INPUT_DOCUMENTS:
        raise ValueError(
            f'Cromwell accepts at most {MAX_INPUT_DOCUMENTS} input documents, '
            f'got {len(input_documents)}',
        )

    key = compute_submission_key(
        workflow_source=workflow_source,
        dependencies=dependencies,
        input_documents=input_documents,
        options=options,
    )

    workflow_id = None
    if reuse_existing:
        workflow_id = _find_existing(
            client,
            key,
            poll_seconds=watch_kwargs.get('max_poll_interval', 60),
        )
    reused = workflow_id is not None
    if workflow_id is None:
        workflow_id = client.submit(
            workflow_source,
            workflow_filename=workflow_path.name,
            input_documents=input_documents,
            options=options,
            dependencies=dependencies,
            labels={SUBMISSION_KEY_LABEL: key},
        )
        logger.info(f'Submitted Workflow {workflow_id} with Submission key {key}')
    else:
        logger.info(f'Attaching to Workflow {workflow_id} (Submission key {key})')

    outputs = watch_workflow(client, workflow_id, **watch_kwargs)
    collected = (
        collect_outputs(outputs, destinations, threads=collect_threads)
        if destinations is not None
        else {}
    )
    return WorkflowResult(
        workflow_id=workflow_id,
        outputs=outputs,
        collected=collected,
        reused=reused,
    )
