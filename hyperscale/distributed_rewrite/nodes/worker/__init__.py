"""
Worker server module.

This module provides the WorkerServer class for executing workflows
in the distributed Hyperscale system.

During the refactoring (Phase 15.2), the original worker.py implementation
remains the source of truth. The new module structure (config.py, state.py,
handlers/, models/) provides the foundation for the eventual composition
root refactoring in Phase 15.2.7.
"""

# Import from original worker.py file (parent directory)
# This preserves backward compatibility during incremental refactoring
from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer

# Also export the new modular components
from .config import WorkerConfig, create_worker_config_from_env
from .state import WorkerState
from .models import (
    ManagerPeerState,
    WorkflowRuntimeState,
    CancelState,
    ExecutionMetrics,
    CompletionTimeTracker,
    TransferMetrics,
    PendingTransferState,
)
from .handlers import (
    WorkflowDispatchHandler,
    WorkflowCancelHandler,
    StateSyncHandler,
    JobLeaderTransferHandler,
    WorkflowStatusQueryHandler,
    WorkflowProgressHandler,
)

# Core modules (Phase 15.2.6)
from .execution import WorkerExecutor
from .registry import WorkerRegistry
from .sync import WorkerStateSync
from .cancellation import WorkerCancellationHandler
from .health import WorkerHealthIntegration
from .backpressure import WorkerBackpressureManager
from .discovery import WorkerDiscoveryManager

__all__ = [
    # Main server class
    "WorkerServer",
    # Configuration
    "WorkerConfig",
    "create_worker_config_from_env",
    # State
    "WorkerState",
    # Models
    "ManagerPeerState",
    "WorkflowRuntimeState",
    "CancelState",
    "ExecutionMetrics",
    "CompletionTimeTracker",
    "TransferMetrics",
    "PendingTransferState",
    # Handlers
    "WorkflowDispatchHandler",
    "WorkflowCancelHandler",
    "StateSyncHandler",
    "JobLeaderTransferHandler",
    "WorkflowStatusQueryHandler",
    "WorkflowProgressHandler",
    # Core modules
    "WorkerExecutor",
    "WorkerRegistry",
    "WorkerStateSync",
    "WorkerCancellationHandler",
    "WorkerHealthIntegration",
    "WorkerBackpressureManager",
    "WorkerDiscoveryManager",
]
