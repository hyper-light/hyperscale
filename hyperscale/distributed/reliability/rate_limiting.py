"""
Rate Limiting (AD-24).

Provides adaptive rate limiting that integrates with the HybridOverloadDetector
to avoid false positives during legitimate traffic bursts.

Components:
- SlidingWindowCounter: Deterministic counting without time-division edge cases
- AdaptiveRateLimiter: Health-gated limiting that only activates under stress
- ServerRateLimiter: Per-client rate limiting using adaptive approach

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadConfig, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.runtime import Clock, RealClock

from .server_rate_limiter import HANDLER_RATE_LIMIT_OPERATIONS
from .adaptive_rate_limit_config import AdaptiveRateLimitConfig
from .adaptive_rate_limiter import AdaptiveRateLimiter
from .rate_limit_result import RateLimitResult
from .server_rate_limiter import ServerRateLimiter
from .sliding_window_counter import SlidingWindowCounter


_REHOMED = (
    SlidingWindowCounter,
    AdaptiveRateLimitConfig,
    AdaptiveRateLimiter,
    RateLimitResult,
    ServerRateLimiter,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
