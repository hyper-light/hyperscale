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
    JobSuspicion,
)
from .leadership import ManagerLeadershipCoordinator
from .raft_integration import ManagerRaftIntegration
from .stats import ManagerStatsCoordinator, ProgressState, BackpressureLevel
from .discovery import ManagerDiscoveryCoordinator
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
    "ManagerRaftIntegration",
    "ManagerStatsCoordinator",
    "ManagerDiscoveryCoordinator",
    # AD-19 Progress State (Three-Signal Health)
    "ProgressState",
    # AD-23 Backpressure
    "BackpressureLevel",
    # AD-26 Adaptive Healthcheck Extensions
    # AD-30 Hierarchical Failure Detection
    "JobSuspicion",
    # AD-24 Rate Limiting
    # AD-25 Version Skew Handling
    "ManagerVersionSkewHandler",
]
