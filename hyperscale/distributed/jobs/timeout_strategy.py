"""
Job timeout strategies with multi-DC coordination (AD-34).

Provides adaptive timeout detection that auto-detects deployment topology:
- LocalAuthorityTimeout: Single-DC deployments (manager has full authority)
- GateCoordinatedTimeout: Multi-DC deployments (gate coordinates globally)

Integrates with AD-26 healthcheck extensions to respect legitimate long-running work.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerInfo, ServerWarning
from hyperscale.distributed.models.distributed import JobFinalStatus, JobProgressReport, JobStatus, JobTimeoutReport
from hyperscale.distributed.models.jobs import TimeoutTrackingState
from hyperscale.distributed.workflow import WorkflowState
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.models.distributed import JobLeaderTransfer

from .timeout_strategy_shared import _DEFAULT_CLOCK
from .gate_coordinated_timeout import GateCoordinatedTimeout
from .local_authority_timeout import LocalAuthorityTimeout
from .timeout_strategy_base import TimeoutStrategy

_REHOMED = (
    TimeoutStrategy,
    LocalAuthorityTimeout,
    GateCoordinatedTimeout,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
