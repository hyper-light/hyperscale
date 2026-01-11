"""
Gate node refactored module structure.

This module provides a modular implementation of the GateServer
following the one-class-per-file pattern from REFACTOR.md.

Until refactoring is complete, the canonical GateServer remains
in nodes/gate.py (the monolithic implementation).

Submodules:
- config: GateConfig dataclass
- state: GateRuntimeState class
- models/: Gate-specific dataclasses (slots=True)
- handlers/: TCP handler stubs with dependency protocols

Core Modules (re-exports from infrastructure packages):
- registry: GateJobManager, ConsistentHashRing
- routing: GateJobRouter (AD-36), DatacenterHealthManager
- dispatch: ManagerDispatcher
- sync: VersionedStateClock
- health: CircuitBreakerManager, LatencyTracker, health states (AD-19)
- leadership: JobLeadershipTracker
- stats: WindowedStatsCollector
- cancellation: Cancellation messages (AD-20)
- leases: JobLeaseManager, DatacenterLeaseManager
- discovery: DiscoveryService, RoleValidator (AD-28)
"""

from .config import GateConfig, create_gate_config
from .state import GateRuntimeState

__all__ = [
    "GateConfig",
    "create_gate_config",
    "GateRuntimeState",
]
