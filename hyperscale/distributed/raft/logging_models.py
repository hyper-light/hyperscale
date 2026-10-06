"""
Structured logging models for Raft consensus operations.

Follows the Entry-based pattern from hyperscale/logging/models.
Each level variant carries contextual fields identifying the
node, job, and Raft-specific details.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from hyperscale.logging.models import Entry, LogLevel

from .raft_critical import RaftCritical
from .raft_debug import RaftDebug
from .raft_error import RaftError
from .raft_info import RaftInfo
from .raft_trace import RaftTrace
from .raft_warning import RaftWarning

_REHOMED = (
    RaftTrace,
    RaftDebug,
    RaftInfo,
    RaftWarning,
    RaftError,
    RaftCritical,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
