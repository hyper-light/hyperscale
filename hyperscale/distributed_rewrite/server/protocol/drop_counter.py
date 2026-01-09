"""
Silent drop counter for tracking and periodically logging dropped messages.

Tracks various categories of dropped messages (rate limited, too large, etc.)
and provides periodic logging summaries for security monitoring.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Literal


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
    _last_reset: float = field(default_factory=time.monotonic)

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
        )

    @property
    def interval_seconds(self) -> float:
        return time.monotonic() - self._last_reset

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
            interval_seconds=self.interval_seconds,
        )

        self.rate_limited = 0
        self.message_too_large = 0
        self.decompression_too_large = 0
        self.decryption_failed = 0
        self.malformed_message = 0
        self.replay_detected = 0
        self.load_shed = 0
        self._last_reset = time.monotonic()

        return snapshot


@dataclass(frozen=True)
class DropCounterSnapshot:
    """Immutable snapshot of drop counter values."""

    rate_limited: int
    message_too_large: int
    decompression_too_large: int
    decryption_failed: int
    malformed_message: int
    replay_detected: int
    load_shed: int  # AD-32: Messages dropped due to backpressure
    interval_seconds: float

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
        )

    @property
    def has_drops(self) -> bool:
        return self.total > 0
