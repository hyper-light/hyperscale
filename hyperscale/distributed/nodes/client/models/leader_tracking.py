"""
Leadership tracking state for client.

Tracks gate and manager leaders and fence tokens.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass
from hyperscale.distributed.models import GateLeaderInfo, ManagerLeaderInfo

from .gate_leader_tracking import GateLeaderTracking
from .manager_leader_tracking import ManagerLeaderTracking

_REHOMED = (
    GateLeaderTracking,
    ManagerLeaderTracking,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
