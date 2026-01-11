"""
Manager node module.

Provides ManagerServer and related components for workflow orchestration.
The manager coordinates job execution within a datacenter, dispatching workflows
to workers and reporting status to gates.
"""

# Re-export ManagerServer from the original location for backward compatibility
from hyperscale.distributed_rewrite.nodes.manager_server import ManagerServer

__all__ = [
    "ManagerServer",
]
