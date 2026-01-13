"""
Manager node module.

Provides ManagerServer and related components for workflow orchestration.
The manager coordinates job execution within a datacenter, dispatching workflows
to workers and reporting status to gates.
"""

# Export ManagerServer from new modular server implementation
from .server import ManagerServer

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
from .load_shedding import ManagerLoadShedder, RequestPriority, OverloadStateTracker

# Backwards compatibility alias
OverloadState = OverloadStateTracker
from .rate_limiting import ManagerRateLimitingCoordinator
from .version_skew import ManagerVersionSkewHandler

__all__ = [
    # Main Server Class
    "ManagerServer",
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
    # AD-24 Rate Limiting
    "ManagerRateLimitingCoordinator",
    # AD-25 Version Skew Handling
    "ManagerVersionSkewHandler",
]
