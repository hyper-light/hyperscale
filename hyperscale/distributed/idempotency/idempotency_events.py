"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass

from .idempotency_committed_event import IdempotencyCommittedEvent
from .idempotency_reserved_event import IdempotencyReservedEvent

_REHOMED = (
    IdempotencyReservedEvent,
    IdempotencyCommittedEvent,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
