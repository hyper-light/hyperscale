"""
Audit trail for SWIM membership and leadership changes.

Provides a bounded event log for debugging and compliance.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.logging.hyperscale_logging_models import ServerDebug

from .protocols import LoggerProtocol
from .audit_log import _DEFAULT_CLOCK
from .audit_event import AuditEvent
from .audit_event_type import AuditEventType
from .audit_log import AuditLog

_REHOMED = (
    AuditEventType,
    AuditEvent,
    AuditLog,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
