"""
A Cromwell client that works under any workflow manager. GCP only.

Nothing in this package imports ``hail``, ``hailtop``, ``cpg_utils.config``,
``cpg_utils.hail_batch`` or ``cpg_utils.cromwell``; every value is passed in
explicitly. See ``cpg_utils/cromwell_client/README.md`` for a user guide.

Only the names below are the public API. Anything else (such as the Submission key
helpers in ``submission``) is importable from its submodule but may change.
"""

from cpg_utils.cromwell_client.client import CachedTokenProvider, CromwellClient
from cpg_utils.cromwell_client.collect import collect_outputs
from cpg_utils.cromwell_client.deps import zip_dependencies
from cpg_utils.cromwell_client.errors import (
    CollectError,
    CromwellError,
    CromwellNotFound,
    CromwellRequestError,
    CromwellUnavailable,
    Stopped,
    WorkflowFailed,
    WorkflowTimedOut,
)
from cpg_utils.cromwell_client.gcp_batch import build_workflow_options
from cpg_utils.cromwell_client.many import (
    WorkflowOutcome,
    WorkflowRequest,
    run_workflows,
)
from cpg_utils.cromwell_client.submission import WorkflowResult, run_workflow
from cpg_utils.cromwell_client.watch import Polling, watch_workflow

__all__ = [
    'CachedTokenProvider',
    'CollectError',
    'CromwellClient',
    'CromwellError',
    'CromwellNotFound',
    'CromwellRequestError',
    'CromwellUnavailable',
    'Polling',
    'Stopped',
    'WorkflowFailed',
    'WorkflowOutcome',
    'WorkflowRequest',
    'WorkflowResult',
    'WorkflowTimedOut',
    'build_workflow_options',
    'collect_outputs',
    'run_workflow',
    'run_workflows',
    'watch_workflow',
    'zip_dependencies',
]
