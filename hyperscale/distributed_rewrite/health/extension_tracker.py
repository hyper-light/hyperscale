"""
Adaptive Healthcheck Extension Tracker (AD-26).

This module provides deadline extension tracking for workers that need
additional time to complete long-running operations. Extensions use
logarithmic decay to prevent indefinite extension grants.

Key concepts:
- Workers can request deadline extensions when busy with legitimate work
- Extensions are granted with logarithmic decay: max(min_grant, base / 2^n)
- Extensions require demonstrable progress to be granted
- Maximum extension count prevents infinite extension
"""

from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class ExtensionTracker:
    """
    Tracks deadline extension requests for a single worker.

    Implements logarithmic decay for extension grants:
    - First extension: base_deadline / 2 = 15s (with base=30s)
    - Second extension: base_deadline / 4 = 7.5s
    - Third extension: base_deadline / 8 = 3.75s
    - ...continues until min_grant is reached

    Extensions require progress since the last extension to be granted.
    This prevents stuck workers from getting unlimited extensions.

    Graceful Exhaustion:
    - When remaining extensions hit warning_threshold, sends warning
    - After exhaustion, grace_period gives final time before eviction
    - Allows workflows to checkpoint/save before being killed

    Attributes:
        worker_id: Unique identifier for the worker being tracked.
        base_deadline: Base deadline in seconds (default 30.0).
        min_grant: Minimum extension grant in seconds (default 1.0).
        max_extensions: Maximum number of extensions allowed (default 5).
        warning_threshold: Remaining extensions count to trigger warning (default 1).
        grace_period: Seconds of grace after exhaustion before kill (default 10.0).
        extension_count: Number of extensions granted so far.
        last_progress: Progress value at last extension (for comparison).
        total_extended: Total seconds extended so far.
        last_extension_time: Timestamp of last extension grant.
        exhaustion_time: Timestamp when extensions were exhausted (None if not exhausted).
        warning_sent: Whether exhaustion warning has been sent.
    """

    worker_id: str
    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    warning_threshold: int = 1
    grace_period: float = 10.0
    extension_count: int = 0
    last_progress: float = 0.0
    total_extended: float = 0.0
    last_extension_time: float = field(default_factory=time.monotonic)
    exhaustion_time: float | None = None
    warning_sent: bool = False

    def request_extension(
        self,
        reason: str,
        current_progress: float,
    ) -> tuple[bool, float, str | None, bool]:
        """
        Request a deadline extension.

        Extensions are granted if:
        1. max_extensions has not been reached
        2. Progress has been made since the last extension

        The extension amount uses logarithmic decay:
        grant = max(min_grant, base_deadline / 2^(extension_count + 1))

        Args:
            reason: Reason for requesting extension (for logging).
            current_progress: Current progress metric (must increase to show progress).

        Returns:
            Tuple of (granted, extension_seconds, denial_reason, is_warning).
            - granted: True if extension was granted
            - extension_seconds: Amount of time granted (0 if denied)
            - denial_reason: Reason for denial, or None if granted
            - is_warning: True if this is a warning about impending exhaustion
        """
        # Check max extensions
        if self.extension_count >= self.max_extensions:
            # Track exhaustion time for grace period
            if self.exhaustion_time is None:
                self.exhaustion_time = time.monotonic()
            return (
                False,
                0.0,
                f"Maximum extensions ({self.max_extensions}) exceeded",
                False,
            )

        # Check for progress since last extension
        # Progress must strictly increase to demonstrate the worker is not stuck
        if self.extension_count > 0 and current_progress <= self.last_progress:
            return (
                False,
                0.0,
                f"No progress since last extension (current={current_progress}, last={self.last_progress})",
                False,
            )

        # Calculate extension grant with logarithmic decay
        # grant = base / 2^(n+1) where n = extension_count
        divisor = 2 ** (self.extension_count + 1)
        grant = max(self.min_grant, self.base_deadline / divisor)

        # Update state
        self.extension_count += 1
        self.last_progress = current_progress
        self.total_extended += grant
        self.last_extension_time = time.monotonic()

        # Check if we should send a warning about impending exhaustion
        remaining = self.get_remaining_extensions()
        is_warning = remaining <= self.warning_threshold and not self.warning_sent
        if is_warning:
            self.warning_sent = True

        return (True, grant, None, is_warning)

    def reset(self) -> None:
        """
        Reset the tracker for a new health check cycle.

        Call this when a worker becomes healthy again or when
        a new workflow starts.
        """
        self.extension_count = 0
        self.last_progress = 0.0
        self.total_extended = 0.0
        self.last_extension_time = time.monotonic()
        self.exhaustion_time = None
        self.warning_sent = False

    def get_remaining_extensions(self) -> int:
        """Get the number of remaining extension requests allowed."""
        return max(0, self.max_extensions - self.extension_count)

    def get_new_deadline(self, current_deadline: float, grant: float) -> float:
        """
        Calculate the new deadline after an extension grant.

        Args:
            current_deadline: The current deadline timestamp.
            grant: The extension grant in seconds.

        Returns:
            The new deadline timestamp.
        """
        return current_deadline + grant

    @property
    def is_exhausted(self) -> bool:
        """Check if all extensions have been used."""
        return self.extension_count >= self.max_extensions

    @property
    def is_in_grace_period(self) -> bool:
        """Check if currently in grace period after exhaustion."""
        if self.exhaustion_time is None:
            return False
        elapsed = time.monotonic() - self.exhaustion_time
        return elapsed < self.grace_period

    @property
    def grace_period_remaining(self) -> float:
        """Get seconds remaining in grace period (0 if not in grace period or expired)."""
        if self.exhaustion_time is None:
            return 0.0
        elapsed = time.monotonic() - self.exhaustion_time
        remaining = self.grace_period - elapsed
        return max(0.0, remaining)

    @property
    def should_evict(self) -> bool:
        """
        Check if worker should be evicted.

        Returns True if:
        - Extensions are exhausted AND
        - Grace period has expired
        """
        if not self.is_exhausted:
            return False
        if self.exhaustion_time is None:
            return False
        elapsed = time.monotonic() - self.exhaustion_time
        return elapsed >= self.grace_period


@dataclass(slots=True)
class ExtensionTrackerConfig:
    """
    Configuration for ExtensionTracker instances.

    Attributes:
        base_deadline: Base deadline in seconds.
        min_grant: Minimum extension grant in seconds.
        max_extensions: Maximum number of extensions allowed.
        warning_threshold: Remaining extensions to trigger warning.
        grace_period: Seconds of grace after exhaustion before kill.
    """

    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    warning_threshold: int = 1
    grace_period: float = 10.0

    def create_tracker(self, worker_id: str) -> ExtensionTracker:
        """Create an ExtensionTracker with this configuration."""
        return ExtensionTracker(
            worker_id=worker_id,
            base_deadline=self.base_deadline,
            min_grant=self.min_grant,
            max_extensions=self.max_extensions,
            warning_threshold=self.warning_threshold,
            grace_period=self.grace_period,
        )
