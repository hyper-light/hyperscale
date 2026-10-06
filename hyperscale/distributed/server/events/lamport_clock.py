"""
Lamport Clock Implementation.

A Lamport clock provides logical timestamps for ordering events in a
distributed system. It guarantees that if event A happens-before event B,
then the timestamp of A is less than the timestamp of B.

Operations:
- increment/tick: Local event, advance clock
- update: Receive event, sync with sender's clock and advance
- ack: Acknowledge, sync without advancing (for responses)

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import TypeVar, Generic
from hyperscale.distributed.runtime import Clock, RealClock

from .versioned_state_clock import _DEFAULT_CLOCK
from .versioned_state import EntityT
from .lamport_clock_impl import LamportClock
from .versioned_state import VersionedState
from .versioned_state_clock import VersionedStateClock

_REHOMED = (
    LamportClock,
    VersionedState,
    VersionedStateClock,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
