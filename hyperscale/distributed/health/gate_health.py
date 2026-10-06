"""
Gate Health State (AD-19).

Three-signal health model for gates, monitored by peer gates.

Signals:
1. Liveness: Is the gate process alive and responsive?
2. Readiness: Can the gate forward jobs? (has DC connectivity, not overloaded)
3. Progress: Is job forwarding happening at expected rate?

Routing decisions and leader election integration:
- route: All signals healthy, forward jobs
- drain: Not ready but alive, stop forwarding
- investigate: Progress issues, check gate
- evict: Dead or stuck, remove from peer list

Leader Election:
- Unhealthy gates should not participate in leader election
- Gates with overload_state == "overloaded" should yield leadership

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from enum import Enum
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.health.worker_health import ProgressState, RoutingDecision

from .gate_health_state import _DEFAULT_CLOCK
from .gate_health_config import GateHealthConfig
from .gate_health_state import GateHealthState

_REHOMED = (
    GateHealthConfig,
    GateHealthState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
