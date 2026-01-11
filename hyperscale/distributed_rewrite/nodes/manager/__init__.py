"""
Manager node module.

Provides ManagerServer and related components for workflow orchestration.
The manager coordinates job execution within a datacenter, dispatching workflows
to workers and reporting status to gates.
"""

from .config import ManagerConfig, create_manager_config_from_env
from .state import ManagerState
from .registry import ManagerRegistry
from .cancellation import ManagerCancellationCoordinator
from .leases import ManagerLeaseCoordinator
from .workflow_lifecycle import ManagerWorkflowLifecycle
from .dispatch import ManagerDispatchCoordinator
from .sync import ManagerStateSync
from .health import (
    ManagerHealthMonitor,
    NodeStatus,
    JobSuspicion,
    ExtensionTracker,
    HealthcheckExtensionManager,
)
from .leadership import ManagerLeadershipCoordinator
from .stats import ManagerStatsCoordinator, ProgressState, BackpressureLevel
from .discovery import ManagerDiscoveryCoordinator
from .load_shedding import ManagerLoadShedder, RequestPriority, OverloadState
from .in_flight import InFlightTracker, BoundedRequestExecutor

__all__ = [
    # Configuration and State
    "ManagerConfig",
    "create_manager_config_from_env",
    "ManagerState",
    # Core Modules
    "ManagerRegistry",
    "ManagerCancellationCoordinator",
    "ManagerLeaseCoordinator",
    "ManagerWorkflowLifecycle",
    "ManagerDispatchCoordinator",
    "ManagerStateSync",
    "ManagerHealthMonitor",
    "ManagerLeadershipCoordinator",
    "ManagerStatsCoordinator",
    "ManagerDiscoveryCoordinator",
    # AD-19 Progress State (Three-Signal Health)
    "ProgressState",
    # AD-22 Load Shedding with Priority Queues
    "ManagerLoadShedder",
    "RequestPriority",
    "OverloadState",
    # AD-23 Backpressure
    "BackpressureLevel",
    # AD-26 Adaptive Healthcheck Extensions
    "ExtensionTracker",
    "HealthcheckExtensionManager",
    # AD-30 Hierarchical Failure Detection
    "NodeStatus",
    "JobSuspicion",
    # AD-32 Bounded Execution
    "InFlightTracker",
    "BoundedRequestExecutor",
]
