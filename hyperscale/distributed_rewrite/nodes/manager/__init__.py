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

__all__ = [
    "ManagerConfig",
    "create_manager_config_from_env",
    "ManagerState",
    "ManagerRegistry",
    "ManagerCancellationCoordinator",
    "ManagerLeaseCoordinator",
]
