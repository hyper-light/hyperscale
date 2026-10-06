"""
Adaptive peer selector using Power of Two Choices with EWMA.

Combines deterministic rendezvous hashing with load-aware selection
for optimal traffic distribution.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import random
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.discovery.selection.rendezvous_hash import WeightedRendezvousHash
from hyperscale.distributed.discovery.selection.ewma_tracker import EWMATracker, EWMAConfig
from hyperscale.distributed.discovery.models.peer_info import PeerInfo
from hyperscale.distributed.runtime import Random, RealRandom

from .adaptive_ewma_selector import _DEFAULT_RANDOM
from .adaptive_ewma_selector import AdaptiveEWMASelector
from .power_of_two_config import PowerOfTwoConfig
from .selection_result import SelectionResult

_REHOMED = (
    PowerOfTwoConfig,
    SelectionResult,
    AdaptiveEWMASelector,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
