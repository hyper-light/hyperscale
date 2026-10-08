"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from enum import Enum, IntEnum

from .transition_result import TransitionResult
from .wal_entry_state import WALEntryState

_REHOMED = (
    WALEntryState,
    TransitionResult,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
