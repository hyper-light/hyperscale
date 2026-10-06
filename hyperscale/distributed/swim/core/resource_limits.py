"""
Resource limits and bounded collections for SWIM protocol.

Provides bounded data structures that prevent unbounded memory growth
in high-churn distributed environments.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from typing import TypeVar, Generic, Callable
from collections import OrderedDict
from hyperscale.distributed.runtime import Clock, RealClock

from .protocols import LoggerProtocol
from .bounded_dict import _DEFAULT_CLOCK
from .bounded_dict import K
from .bounded_dict import V
from .bounded_dict import BoundedDict
from .cleanup_config import CleanupConfig
from .cleanup_context_settings import CleanupContextSettings


def create_cleanup_config_from_context(context: CleanupContextSettings) -> CleanupConfig:
    """Create CleanupConfig from server context with sensible defaults."""
    return CleanupConfig(
        max_node_states=context.get('max_node_states', 10000),
        dead_node_retention_seconds=context.get('dead_node_retention', 3600.0),
        max_suspicions=context.get('max_suspicions', 1000),
        max_gossip_updates=context.get('max_gossip_updates', 1000),
        max_pending_probes=context.get('max_pending_probes', 100),
        cleanup_interval_seconds=context.get('cleanup_interval', 30.0),
    )

_REHOMED = (
    BoundedDict,
    CleanupConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
