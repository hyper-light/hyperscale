"""
Load Shedding with Priority Queues (AD-22, AD-37).

Provides graceful degradation under load by shedding low-priority
requests based on current overload state.

Uses unified MessageClass classification from AD-37:
- CONTROL (CRITICAL): SWIM probes/acks, cancellation, leadership - never shed
- DISPATCH (HIGH): Job submissions, workflow dispatch, state sync
- DATA (NORMAL): Progress updates, stats queries
- TELEMETRY (LOW): Debug stats, detailed metrics - shed first

Shedding Behavior by State:
- healthy: Accept all requests
- busy: Shed TELEMETRY (LOW) only
- stressed: Shed DATA (NORMAL) and TELEMETRY (LOW)
- overloaded: Shed all except CONTROL (CRITICAL)

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.reliability.message_class import MessageClass, classify_handler

from .load_shedder import MESSAGE_CLASS_TO_REQUEST_PRIORITY
from .load_shedder import classify_handler_to_priority
from .load_shedder import LoadShedder
from .load_shedder_config import LoadShedderConfig

_REHOMED = (
    LoadShedderConfig,
    LoadShedder,
    classify_handler_to_priority,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
