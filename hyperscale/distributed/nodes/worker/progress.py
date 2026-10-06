"""
Worker progress reporting module.

Handles sending workflow progress updates and final results to managers.
Implements job leader routing and backpressure-aware delivery.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING
from hyperscale.distributed.models import (
    RateLimitResponse,
    WorkflowFinalResult,
    WorkflowFinalResultAck,
    WorkflowProgress,
    WorkflowProgressAck,
    WorkflowCancellationComplete,
)
from hyperscale.distributed.reliability import (
    BackpressureLevel,
    BackpressureSignal,
    RetryConfig,
    RetryExecutor,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerError, ServerInfo, ServerWarning
from hyperscale.distributed.runtime import Clock, RealClock, SendTcp, RunTask

from .worker_progress_reporter import _DEFAULT_CLOCK
from .worker_progress_reporter import _TRANSIENT_SEND_ERRORS
from .worker_progress_reporter import _LOCAL_BUG_ERRORS
from .worker_progress_reporter import _classify_send_error
from .pending_result import PendingResult
from .worker_progress_reporter import WorkerProgressReporter

_REHOMED = (
    PendingResult,
    WorkerProgressReporter,
    _classify_send_error,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
