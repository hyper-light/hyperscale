"""
Gate node modular implementation.

This module provides a fully modular implementation of the GateServer
following the one-class-per-file pattern.

Structure:
- config: settings derived from Env
- state: GateRuntimeState for mutable runtime state
- server: GateServer composition root
- models/: Gate-specific dataclasses (slots=True)
- handlers/: TCP handler classes for message processing
- *_coordinator: Business logic coordinators

Coordinators:
- leadership_coordinator: Job leadership and gate elections
- dispatch_coordinator: Job submission and DC routing
- stats_coordinator: Statistics collection and aggregation
- peer_coordinator: Gate peer management
- health_coordinator: Datacenter health monitoring
- orphan_job_coordinator: Orphaned job detection and takeover
"""

from .state import GateRuntimeState
from .server import GateServer

# Coordinators
from .leadership_coordinator import GateLeadershipCoordinator
from .dispatch_coordinator import GateDispatchCoordinator
from .stats_coordinator import GateStatsCoordinator
from .peer_coordinator import GatePeerCoordinator
from .health_coordinator import GateHealthCoordinator
from .orphan_job_coordinator import GateOrphanJobCoordinator
from .raft_integration import GateRaftIntegration

# Handlers
from .handlers import (
    GatePingHandler,
    GateJobHandler,
    GateManagerHandler,
    GateCancellationHandler,
    GateStateSyncHandler,
)

__all__ = [
    # Core
    "GateServer",
    "GateRuntimeState",
    # Coordinators
    "GateLeadershipCoordinator",
    "GateDispatchCoordinator",
    "GateStatsCoordinator",
    "GatePeerCoordinator",
    "GateHealthCoordinator",
    "GateOrphanJobCoordinator",
    "GateRaftIntegration",
    # Handlers
    "GatePingHandler",
    "GateJobHandler",
    "GateManagerHandler",
    "GateCancellationHandler",
    "GateStateSyncHandler",
]
