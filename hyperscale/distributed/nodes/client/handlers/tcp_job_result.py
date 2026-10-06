"""
TCP handlers for job result notifications.

Handles JobFinalResult (single DC) and GlobalJobResult (multi-DC aggregated).

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from hyperscale.distributed.models import (
    DatacenterSubstitution,
    GlobalJobResult,
    JobFinalResult,
    JobStatus,
    WorkflowDCResult,
    WorkflowResult,
    WorkflowResultPush,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerWarning
from hyperscale.reporting.results import Results

from .tcp_workflow_result import WorkflowResultPushHandler
from .global_job_result_handler import GlobalJobResultHandler
from .job_final_result_handler import JobFinalResultHandler

_REHOMED = (
    JobFinalResultHandler,
    GlobalJobResultHandler,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
