"""
Gate health monitoring module.

Provides health tracking infrastructure for managers and peer gates.

Classes:
- CircuitBreakerManager: Per-manager circuit breakers for dispatch failures
- LatencyTracker: Latency sample collection and analysis
- ManagerHealthState: Three-signal health state for managers (AD-19)
- GateHealthState: Three-signal health state for peer gates (AD-19)

These are re-exported from the health package.
"""

from hyperscale.distributed_rewrite.health import (
    CircuitBreakerManager,
    LatencyTracker,
    ManagerHealthState,
    ManagerHealthConfig,
    GateHealthState,
    GateHealthConfig,
)

__all__ = [
    "CircuitBreakerManager",
    "LatencyTracker",
    "ManagerHealthState",
    "ManagerHealthConfig",
    "GateHealthState",
    "GateHealthConfig",
]
