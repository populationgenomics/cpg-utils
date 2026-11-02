"""
A Cromwell client that works under any workflow manager. GCP only.

Nothing in this package imports ``hail``, ``hailtop``, ``cpg_utils.config``,
``cpg_utils.hail_batch`` or ``cpg_utils.cromwell``; every value is passed in
explicitly. See ``cpg_utils/cromwell_client/README.md`` for a user guide.
"""

from cpg_utils.cromwell_client.client import CachedTokenProvider, CromwellClient
from cpg_utils.cromwell_client.collect import Destination, collect_outputs
from cpg_utils.cromwell_client.deps import zip_dependencies
from cpg_utils.cromwell_client.errors import (
    CollectError,
    CromwellError,
    CromwellNotFound,
    CromwellRequestError,
    CromwellUnavailable,
    WorkflowFailed,
    WorkflowTimedOut,
)
from cpg_utils.cromwell_client.gcp_batch import build_workflow_options
from cpg_utils.cromwell_client.submission import (
    KEY_EXCLUDED_OPTIONS,
    SUBMISSION_KEY_LABEL,
    WorkflowResult,
    compute_submission_key,
    run_workflow,
)
from cpg_utils.cromwell_client.watch import (
    METADATA_REPORT_KEYS,
    poll_interval,
    watch_workflow,
)

__all__ = [
    'KEY_EXCLUDED_OPTIONS',
    'METADATA_REPORT_KEYS',
    'SUBMISSION_KEY_LABEL',
    'CachedTokenProvider',
    'CollectError',
    'CromwellClient',
    'CromwellError',
    'CromwellNotFound',
    'CromwellRequestError',
    'CromwellUnavailable',
    'Destination',
    'WorkflowFailed',
    'WorkflowResult',
    'WorkflowTimedOut',
    'build_workflow_options',
    'collect_outputs',
    'compute_submission_key',
    'poll_interval',
    'run_workflow',
    'watch_workflow',
    'zip_dependencies',
]
