"""
Silent drop counter for tracking and periodically logging dropped messages.

Tracks various categories of dropped messages (rate limited, too large, etc.)
and provides periodic logging summaries for security monitoring.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Literal
from hyperscale.distributed.runtime import Clock, RealClock

from .drop_counter_snapshot import DropCounterSnapshot

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class DropCounter:
    """
    Thread-safe counter for tracking silently dropped messages.

    Designed for use in asyncio contexts where synchronous counter increments
    are atomic within a single event loop iteration.
    """

    rate_limited: int = 0
    message_too_large: int = 0
    decompression_too_large: int = 0
    decryption_failed: int = 0
    malformed_message: int = 0
    replay_detected: int = 0
    load_shed: int = 0  # AD-32: Messages dropped due to backpressure
    # Log records lost because the logger's write itself failed: with the
    # logger broken there is nowhere else to report them.
    log_write_failed: int = 0
    _last_reset: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())

    def increment_rate_limited(self) -> None:
        self.rate_limited += 1

    def increment_message_too_large(self) -> None:
        self.message_too_large += 1

    def increment_decompression_too_large(self) -> None:
        self.decompression_too_large += 1

    def increment_decryption_failed(self) -> None:
        self.decryption_failed += 1

    def increment_malformed_message(self) -> None:
        self.malformed_message += 1

    def increment_replay_detected(self) -> None:
        self.replay_detected += 1

    def increment_load_shed(self) -> None:
        """AD-32: Increment when message dropped due to priority-based load shedding."""
        self.load_shed += 1

    @property
    def total(self) -> int:
        return (
            self.rate_limited
            + self.message_too_large
            + self.decompression_too_large
            + self.decryption_failed
            + self.malformed_message
            + self.replay_detected
            + self.load_shed
            + self.log_write_failed
        )

    @property
    def interval_seconds(self) -> float:
        return _DEFAULT_CLOCK.monotonic() - self._last_reset

    def reset(self) -> "DropCounterSnapshot":
        """
        Reset all counters and return a snapshot of the values before reset.

        Returns:
            DropCounterSnapshot with the pre-reset values and interval duration
        """
        snapshot = DropCounterSnapshot(
            rate_limited=self.rate_limited,
            message_too_large=self.message_too_large,
            decompression_too_large=self.decompression_too_large,
            decryption_failed=self.decryption_failed,
            malformed_message=self.malformed_message,
            replay_detected=self.replay_detected,
            load_shed=self.load_shed,
            log_write_failed=self.log_write_failed,
            interval_seconds=self.interval_seconds,
        )

        self.rate_limited = 0
        self.message_too_large = 0
        self.decompression_too_large = 0
        self.decryption_failed = 0
        self.malformed_message = 0
        self.replay_detected = 0
        self.load_shed = 0
        self.log_write_failed = 0
        self._last_reset = _DEFAULT_CLOCK.monotonic()

        return snapshot

_REHOMED = (
    DropCounterSnapshot,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
