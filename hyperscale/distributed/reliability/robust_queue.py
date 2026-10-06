"""
Robust Message Queue with Backpressure Support.

Provides a bounded async queue with overflow handling, backpressure signaling,
and comprehensive metrics. Designed for distributed systems where message loss
must be minimized while preventing OOM under load.

Features:
- Primary bounded queue with configurable size
- Overflow buffer behind it, FIFO across both (when full: drop the oldest, or refuse the newest)
- Backpressure signals aligned with AD-23
- Per-message priority support
- Comprehensive metrics for observability
- Thread-safe for asyncio concurrent access

Usage:
    queue = RobustMessageQueue(maxsize=1000, overflow_size=100)

    # Producer side
    result = queue.put_nowait(message)
    if result.in_overflow:
        # Signal backpressure to sender
        return BackpressureResponse(retry_after_ms=result.suggested_delay_ms)

    # Consumer side
    message = await queue.get()

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TypeVar, Generic
from hyperscale.distributed.reliability.backpressure import BackpressureLevel, BackpressureSignal

from .robust_message_queue import T
from .queue_full_error import QueueFullError
from .queue_metrics import QueueMetrics
from .queue_put_result import QueuePutResult
from .queue_state import QueueState
from .robust_message_queue import RobustMessageQueue
from .robust_queue_config import RobustQueueConfig

_REHOMED = (
    QueueState,
    QueueFullError,
    QueuePutResult,
    RobustQueueConfig,
    QueueMetrics,
    RobustMessageQueue,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
