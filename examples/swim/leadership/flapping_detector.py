"""
Leadership flapping detection for SWIM clusters.

Detects rapid leadership changes that indicate cluster instability,
network issues, or misconfiguration.
"""

import time
from dataclasses import dataclass, field
from collections import deque
from typing import Callable, Any

from hyperscale.logging.hyperscale_logging_models import ServerDebug


from ..core.protocols import LoggerProtocol


@dataclass
class LeadershipChange:
    """Record of a leadership change event."""
    timestamp: float
    old_leader: tuple[str, int] | None
    new_leader: tuple[str, int] | None
    term: int
    reason: str  # e.g., 'election', 'stepdown', 'timeout', 'conflict'
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.monotonic()


@dataclass
class FlappingDetector:
    """
    Detects leadership flapping (rapid leadership changes).
    
    When leadership changes too frequently, it indicates:
    - Network instability (asymmetric partitions)
    - Overloaded nodes (LHM issues)
    - Misconfigured timeouts
    - Split-brain scenarios
    
    Actions on flapping:
    - Log warnings for visibility
    - Increase election cooldown
    - Voluntary step-down if we're contributing
    - Alert external monitoring
    
    Example:
        detector = FlappingDetector(
            max_changes_per_window=5,
            window_seconds=60.0,
            on_flapping_detected=handle_flapping,
        )
        await detector.record_change(old_leader, new_leader, term, 'election')
    """
    
    # Thresholds
    max_changes_per_window: int = 5
    """Maximum leadership changes before flapping is detected."""
    
    window_seconds: float = 60.0
    """Time window for counting changes."""
    
    # Severity levels
    warning_threshold: int = 3
    """Changes to trigger a warning (before full flapping)."""
    
    critical_threshold: int = 10
    """Changes to trigger critical alert."""
    
    # Cooldown escalation
    base_cooldown: float = 5.0
    """Base cooldown between elections."""
    
    max_cooldown: float = 60.0
    """Maximum cooldown during flapping."""
    
    cooldown_multiplier: float = 1.5
    """How much to increase cooldown per detection."""
    
    # State
    _changes: deque[LeadershipChange] = field(default_factory=lambda: deque(maxlen=100))
    _is_flapping: bool = False
    _current_cooldown: float = 5.0
    _flapping_start: float | None = None
    _last_detection_time: float = 0.0
    
    # Callbacks
    _on_flapping_detected: Callable[[int, float], Any] | None = None
    _on_flapping_resolved: Callable[[], Any] | None = None
    _on_warning: Callable[[int], Any] | None = None
    
    # Stats
    _total_changes: int = 0
    _flapping_episodes: int = 0
    _total_flapping_duration: float = 0.0
    
    # Maximum counter values to prevent overflow
    MAX_COUNTER_VALUE: int = 2**31 - 1
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    def __post_init__(self):
        self._changes = deque(maxlen=100)
        self._current_cooldown = self.base_cooldown
    
    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
    
    async def _log_debug(self, message: str) -> None:
        """Log a debug message."""
        if self._logger:
            try:
                await self._logger.log(ServerDebug(
                    message=f"[FlappingDetector] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
    
    def set_callbacks(
        self,
        on_flapping_detected: Callable[[int, float], Any] | None = None,
        on_flapping_resolved: Callable[[], Any] | None = None,
        on_warning: Callable[[int], Any] | None = None,
    ) -> None:
        """
        Set callback functions for flapping events.
        
        Args:
            on_flapping_detected: Called with (change_count, duration) when flapping starts.
            on_flapping_resolved: Called when flapping stops.
            on_warning: Called with change_count when approaching threshold.
        """
        self._on_flapping_detected = on_flapping_detected
        self._on_flapping_resolved = on_flapping_resolved
        self._on_warning = on_warning
    
    async def record_change(
        self,
        old_leader: tuple[str, int] | None,
        new_leader: tuple[str, int] | None,
        term: int,
        reason: str = 'unknown',
    ) -> bool:
        """
        Record a leadership change event.
        
        Args:
            old_leader: Previous leader address.
            new_leader: New leader address.
            term: Current term number.
            reason: Why the change happened.
        
        Returns:
            True if this change triggered flapping detection.
        """
        now = time.monotonic()
        
        change = LeadershipChange(
            timestamp=now,
            old_leader=old_leader,
            new_leader=new_leader,
            term=term,
            reason=reason,
        )
        self._changes.append(change)
        if self._total_changes < self.MAX_COUNTER_VALUE:
            self._total_changes += 1
        
        # Count changes in window
        changes_in_window = self._count_changes_in_window(now)
        
        # Check thresholds
        triggered_flapping = False
        
        if changes_in_window >= self.critical_threshold:
            await self._handle_critical(changes_in_window)
        elif changes_in_window >= self.max_changes_per_window:
            triggered_flapping = await self._handle_flapping_detected(changes_in_window, now)
        elif changes_in_window >= self.warning_threshold:
            await self._handle_warning(changes_in_window)
        elif self._is_flapping:
            # Check if flapping has resolved
            await self._check_flapping_resolved(now)
        
        return triggered_flapping
    
    def _count_changes_in_window(self, now: float) -> int:
        """Count leadership changes within the window."""
        cutoff = now - self.window_seconds
        return sum(1 for c in self._changes if c.timestamp >= cutoff)
    
    async def _handle_warning(self, count: int) -> None:
        """Handle approaching the flapping threshold."""
        if self._on_warning:
            try:
                self._on_warning(count)
            except Exception as e:
                await self._log_debug(f"Warning callback failed: {type(e).__name__}: {e}")
    
    async def _handle_flapping_detected(self, count: int, now: float) -> bool:
        """Handle flapping detection."""
        if self._is_flapping:
            # Already flapping, just update cooldown
            self._escalate_cooldown()
            return False
        
        self._is_flapping = True
        self._flapping_start = now
        if self._flapping_episodes < self.MAX_COUNTER_VALUE:
            self._flapping_episodes += 1
        self._last_detection_time = now
        
        # Escalate cooldown
        self._escalate_cooldown()
        
        if self._on_flapping_detected:
            try:
                self._on_flapping_detected(count, self._current_cooldown)
            except Exception as e:
                await self._log_debug(f"Flapping detected callback failed: {type(e).__name__}: {e}")
        
        return True
    
    async def _handle_critical(self, count: int) -> None:
        """Handle critical flapping level."""
        # Ensure we're in flapping state
        if not self._is_flapping:
            await self._handle_flapping_detected(count, time.monotonic())
        
        # Max out cooldown
        self._current_cooldown = self.max_cooldown
    
    async def _check_flapping_resolved(self, now: float) -> None:
        """Check if flapping has resolved."""
        # Require a full quiet window before resolving
        changes_in_window = self._count_changes_in_window(now)
        
        if changes_in_window < self.warning_threshold:
            self._is_flapping = False
            
            if self._flapping_start is not None:
                duration = now - self._flapping_start
                self._total_flapping_duration += duration
            
            self._flapping_start = None
            
            # Gradually reduce cooldown
            self._current_cooldown = max(
                self.base_cooldown,
                self._current_cooldown / self.cooldown_multiplier,
            )
            
            if self._on_flapping_resolved:
                try:
                    self._on_flapping_resolved()
                except Exception as e:
                    await self._log_debug(f"Flapping resolved callback failed: {type(e).__name__}: {e}")
    
    def _escalate_cooldown(self) -> None:
        """Increase the cooldown period."""
        self._current_cooldown = min(
            self.max_cooldown,
            self._current_cooldown * self.cooldown_multiplier,
        )
    
    @property
    def is_flapping(self) -> bool:
        """True if currently in a flapping state."""
        return self._is_flapping
    
    @property
    def current_cooldown(self) -> float:
        """Get the current election cooldown (escalated during flapping)."""
        return self._current_cooldown
    
    @property
    def changes_in_window(self) -> int:
        """Get current count of changes in the window."""
        return self._count_changes_in_window(time.monotonic())
    
    def get_recent_changes(self, count: int = 10) -> list[LeadershipChange]:
        """
        Get the most recent leadership changes.
        
        Args:
            count: Maximum number of changes to return (capped at 100).
        
        Returns:
            List of recent changes, most recent last.
        """
        # Bound count to prevent excessive memory allocation
        bounded_count = min(count, 100)
        return list(self._changes)[-bounded_count:]
    
    def get_change_rate(self) -> float:
        """Get the average changes per minute over the window."""
        changes = self.changes_in_window
        return (changes / self.window_seconds) * 60.0
    
    def should_delay_election(self) -> tuple[bool, float]:
        """
        Check if an election should be delayed due to flapping.
        
        Returns:
            (should_delay, delay_seconds)
        """
        if not self._is_flapping:
            return (False, 0.0)
        
        # Check time since last detection
        now = time.monotonic()
        time_since_detection = now - self._last_detection_time
        
        if time_since_detection < self._current_cooldown:
            return (True, self._current_cooldown - time_since_detection)
        
        return (False, 0.0)
    
    def reset(self) -> None:
        """Reset the detector state."""
        self._changes.clear()
        self._is_flapping = False
        self._current_cooldown = self.base_cooldown
        self._flapping_start = None
    
    def get_stats(self) -> dict[str, Any]:
        """Get detector statistics."""
        return {
            'is_flapping': self._is_flapping,
            'current_cooldown': self._current_cooldown,
            'changes_in_window': self.changes_in_window,
            'change_rate_per_min': self.get_change_rate(),
            'total_changes': self._total_changes,
            'flapping_episodes': self._flapping_episodes,
            'total_flapping_duration': self._total_flapping_duration,
            'window_seconds': self.window_seconds,
            'max_changes_per_window': self.max_changes_per_window,
        }

