"""
Unified Retry Framework with Jitter (AD-21).

Provides a consistent retry mechanism with exponential backoff and jitter
for all network operations. Different jitter strategies suit different scenarios.

Jitter prevents thundering herd when multiple clients retry simultaneously.

Phase 5 DI: ``RetryExecutor``, ``calculate_jittered_delay`` and
``add_jitter`` all accept ``Clock`` / ``Random`` instances so Phase 6
SIM mode can drive deterministic retry timing and jitter. Default
fall-back is the shared ``RealClock`` / ``RealRandom`` defined at
module scope; behavior is byte-equivalent to the prior ``time`` /
``asyncio`` / ``random`` calls.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from enum import Enum
from itertools import count
from typing import Awaitable, Callable, TypeVar
from hyperscale.distributed.runtime import Clock, Random, RealClock, RealRandom

from .retry_executor import T
from .retry_executor import _DEFAULT_CLOCK
from .retry_shared import _DEFAULT_RANDOM
from .jitter_strategy import JitterStrategy
from .retry_config import RetryConfig
from .retry_executor import RetryExecutor


def calculate_jittered_delay(
    attempt: int,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    jitter: JitterStrategy = JitterStrategy.FULL,
    *,
    random_source: Random | None = None,
) -> float:
    """
    Standalone function to calculate a jittered delay.

    Useful when you need jitter calculation without the full executor.

    Args:
        attempt: Zero-based attempt number
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap in seconds
        jitter: Jitter strategy to use
        random_source: Optional ``Random`` injection; defaults to the
            module-level ``RealRandom`` so existing callers see
            byte-identical behavior under Phase 5.

    Returns:
        Delay in seconds
    """
    rng = random_source if random_source is not None else _DEFAULT_RANDOM

    if jitter == JitterStrategy.FULL:
        temp = min(max_delay, base_delay * (2**attempt))
        return rng.uniform(0, temp)

    elif jitter == JitterStrategy.EQUAL:
        temp = min(max_delay, base_delay * (2**attempt))
        return temp / 2 + rng.uniform(0, temp / 2)

    elif jitter == JitterStrategy.DECORRELATED:
        # For standalone use, treat as full jitter since we don't track state
        temp = min(max_delay, base_delay * (2**attempt))
        return rng.uniform(0, temp)

    else:  # NONE
        return min(max_delay, base_delay * (2**attempt))


def add_jitter(
    interval: float,
    jitter_factor: float = 0.1,
    *,
    random_source: Random | None = None,
) -> float:
    """
    Add jitter to a fixed interval.

    Useful for heartbeats, health checks, and other periodic operations
    where you want some variation to prevent synchronization.

    Args:
        interval: Base interval in seconds
        jitter_factor: Maximum jitter as fraction of interval (default 10%)
        random_source: Optional ``Random`` injection; defaults to the
            module-level ``RealRandom``.

    Returns:
        Interval with random jitter applied

    Example:
        # 30 second heartbeat with 10% jitter (27-33 seconds)
        delay = add_jitter(30.0, jitter_factor=0.1)
    """
    rng = random_source if random_source is not None else _DEFAULT_RANDOM
    jitter_amount = interval * jitter_factor
    return interval + rng.uniform(-jitter_amount, jitter_amount)

_REHOMED = (
    JitterStrategy,
    RetryConfig,
    RetryExecutor,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
