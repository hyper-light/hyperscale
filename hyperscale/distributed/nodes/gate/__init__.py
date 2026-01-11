"""
Gate node modular implementation.

This module provides a fully modular implementation of the GateServer
following the one-class-per-file pattern.

Structure:
- config: GateConfig dataclass for immutable configuration
- state: GateRuntimeState for mutable runtime state
- server: GateServer composition root
- models/: Gate-specific dataclasses (slots=True)
- handlers/: TCP handler classes for message processing
- *_coordinator: Business logic coordinators

Coordinators:
- leadership_coordinator: Job leadership and gate elections
- dispatch_coordinator: Job submission and DC routing
- stats_coordinator: Statistics collection and aggregation
- cancellation_coordinator: Job/workflow cancellation
- peer_coordinator: Gate peer management
- health_coordinator: Datacenter health monitoring
"""

from .config import GateConfig, create_gate_config
from .state import GateRuntimeState
from .server import GateServer

# Coordinators
from .leadership_coordinator import GateLeadershipCoordinator
from .dispatch_coordinator import GateDispatchCoordinator
from .stats_coordinator import GateStatsCoordinator
from .cancellation_coordinator import GateCancellationCoordinator
from .peer_coordinator import GatePeerCoordinator
from .health_coordinator import GateHealthCoordinator

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
    "GateConfig",
    "create_gate_config",
    "GateRuntimeState",
    # Coordinators
    "GateLeadershipCoordinator",
    "GateDispatchCoordinator",
    "GateStatsCoordinator",
    "GateCancellationCoordinator",
    "GatePeerCoordinator",
    "GateHealthCoordinator",
    # Handlers
    "GatePingHandler",
    "GateJobHandler",
    "GateManagerHandler",
    "GateCancellationHandler",
    "GateStateSyncHandler",
]
