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
from collections.abc import Mapping, Sequence
from typing import Any

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
