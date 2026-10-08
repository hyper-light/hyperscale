"""
Role-aware confirmation manager for unconfirmed peers (AD-35 Task 12.5.3-12.5.6).

Manages the confirmation lifecycle for peers discovered via gossip but not yet
confirmed via bidirectional communication (ping/ack).

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable
from hyperscale.distributed.models.distributed import NodeRole
from hyperscale.distributed.swim.roles.confirmation_strategy import RoleBasedConfirmationStrategy, get_strategy_for_role
from hyperscale.distributed.runtime import Clock, RealClock

from .role_aware_confirmation_manager import _DEFAULT_CLOCK
from .confirmation_result import ConfirmationResult
from .role_aware_confirmation_manager import RoleAwareConfirmationManager
from .unconfirmed_peer_state import UnconfirmedPeerState

_REHOMED = (
    UnconfirmedPeerState,
    ConfirmationResult,
    RoleAwareConfirmationManager,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
