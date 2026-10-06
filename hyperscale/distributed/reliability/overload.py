"""
Hybrid Overload Detection (AD-18).

Combines delta-based detection with absolute safety bounds for robust
overload detection that is self-calibrating yet protected against drift.

Three-tier detection:
1. Primary: Delta-based (% above EMA baseline + trend slope)
2. Secondary: Absolute safety bounds (hard limits)
3. Tertiary: Resource signals (CPU, memory, queue depth)

Final state = max(delta_state, absolute_state, resource_state)

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from collections import deque
from dataclasses import dataclass, field
from enum import Enum

from .overload_state import _STATE_ORDER
from .hybrid_overload_detector import HybridOverloadDetector
from .overload_config import OverloadConfig
from .overload_state import OverloadState

_REHOMED = (
    OverloadState,
    OverloadConfig,
    HybridOverloadDetector,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
