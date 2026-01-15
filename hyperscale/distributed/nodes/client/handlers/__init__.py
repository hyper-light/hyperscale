"""
TCP message handlers for HyperscaleClient.

Each handler class processes a specific message type from gates/managers.
"""

from .tcp_job_status_push import JobStatusPushHandler, JobBatchPushHandler
from .tcp_job_result import JobFinalResultHandler, GlobalJobResultHandler
from .tcp_reporter_result import ReporterResultPushHandler
from .tcp_workflow_result import WorkflowResultPushHandler
from .tcp_windowed_stats import WindowedStatsPushHandler
from .tcp_cancellation_complete import CancellationCompleteHandler
from .tcp_leadership_transfer import (
    GateLeaderTransferHandler,
    ManagerLeaderTransferHandler,
)

__all__ = [
    "JobStatusPushHandler",
    "JobBatchPushHandler",
    "JobFinalResultHandler",
    "GlobalJobResultHandler",
    "ReporterResultPushHandler",
    "WorkflowResultPushHandler",
    "WindowedStatsPushHandler",
    "CancellationCompleteHandler",
    "GateLeaderTransferHandler",
    "ManagerLeaderTransferHandler",
]
