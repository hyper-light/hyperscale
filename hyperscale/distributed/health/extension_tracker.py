"""
Adaptive Healthcheck Extension Tracker (AD-26).

This module provides deadline extension tracking for workers that need
additional time to complete long-running operations. Extensions use
logarithmic decay to prevent indefinite extension grants.

Key concepts:
- Workers can request deadline extensions when busy with legitimate work
- Extensions are granted with logarithmic decay: max(min_grant, base / 2^n)
- Extensions require demonstrable progress to be granted
- Maximum extension count prevents infinite extension

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from hyperscale.distributed.runtime import Clock, RealClock

from .extension_tracker_impl import _DEFAULT_CLOCK
from .extension_tracker_impl import ExtensionTracker
from .extension_tracker_config import ExtensionTrackerConfig

_REHOMED = (
    ExtensionTracker,
    ExtensionTrackerConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
