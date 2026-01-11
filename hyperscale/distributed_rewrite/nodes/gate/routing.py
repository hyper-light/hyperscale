"""
Gate job routing module (AD-36).

Provides Vivaldi-based multi-factor routing for optimal datacenter selection.

Classes:
- GateJobRouter: Multi-factor scoring (RTT UCB x load x quality) with hysteresis
- GateJobRouterConfig: Router configuration
- DatacenterHealthManager: Centralized DC health classification (AD-16)

These are re-exported from the routing and datacenters packages.
"""

from hyperscale.distributed_rewrite.routing import (
    GateJobRouter,
    GateJobRouterConfig,
    RoutingDecision,
    DatacenterCandidate,
)
from hyperscale.distributed_rewrite.datacenters import DatacenterHealthManager

__all__ = [
    "GateJobRouter",
    "GateJobRouterConfig",
    "RoutingDecision",
    "DatacenterCandidate",
    "DatacenterHealthManager",
]
