"""
Lease Manager - At-most-once job delivery guarantees via leases.

This class manages leases for job dispatches to datacenters, ensuring
at-most-once delivery semantics through fencing tokens.

Key concepts:
- Lease: A time-limited grant for a gate to dispatch to a specific DC
- Fence Token: Monotonic counter to reject stale operations
- Lease Transfer: Handoff of lease from one gate to another

Leases provide:
- At-most-once semantics: Only the lease holder can dispatch
- Partition tolerance: Leases expire if holder becomes unresponsive
- Ordered operations: Fence tokens reject out-of-order requests

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.models import DatacenterLease, LeaseTransfer
from hyperscale.distributed.runtime import Clock, RealClock

from .datacenter_lease_manager import _DEFAULT_CLOCK
from .datacenter_lease_manager import DatacenterLeaseManager
from .lease_stats import LeaseStats

_REHOMED = (
    LeaseStats,
    DatacenterLeaseManager,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
