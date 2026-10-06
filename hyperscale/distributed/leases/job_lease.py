"""

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.logging.hyperscale_logging_models import JobLeaseExpiryCallbackFailed

from .job_lease_shared import _DEFAULT_CLOCK
from .job_lease_model import JobLease
from .job_lease_manager import JobLeaseManager
from .lease_acquisition_result import LeaseAcquisitionResult
from .lease_state import LeaseState

_REHOMED = (
    LeaseState,
    JobLease,
    LeaseAcquisitionResult,
    JobLeaseManager,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
