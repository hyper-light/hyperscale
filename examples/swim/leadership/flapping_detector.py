"""
Leadership flapping detection for SWIM clusters.

Detects rapid leadership changes that indicate cluster instability,
network issues, or misconfiguration.
"""

import time
from dataclasses import dataclass, field
from collections import deque
from typing import Callable, Any


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
        detector.record_change(old_leader, new_leader, term, 'election')
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
    
    def __post_init__(self):
        self._changes = deque(maxlen=100)
        self._current_cooldown = self.base_cooldown
    
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
    
    def record_change(
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
        self._total_changes += 1
        
        # Count changes in window
        changes_in_window = self._count_changes_in_window(now)
        
        # Check thresholds
        triggered_flapping = False
        
        if changes_in_window >= self.critical_threshold:
            self._handle_critical(changes_in_window)
        elif changes_in_window >= self.max_changes_per_window:
            triggered_flapping = self._handle_flapping_detected(changes_in_window, now)
        elif changes_in_window >= self.warning_threshold:
            self._handle_warning(changes_in_window)
        elif self._is_flapping:
            # Check if flapping has resolved
            self._check_flapping_resolved(now)
        
        return triggered_flapping
    
    def _count_changes_in_window(self, now: float) -> int:
        """Count leadership changes within the window."""
        cutoff = now - self.window_seconds
        return sum(1 for c in self._changes if c.timestamp >= cutoff)
    
    def _handle_warning(self, count: int) -> None:
        """Handle approaching the flapping threshold."""
        if self._on_warning:
            try:
                self._on_warning(count)
            except Exception as e:
                import sys
                print(f"[FlappingDetector] Warning callback failed: {e}", file=sys.stderr)
    
    def _handle_flapping_detected(self, count: int, now: float) -> bool:
        """Handle flapping detection."""
        if self._is_flapping:
            # Already flapping, just update cooldown
            self._escalate_cooldown()
            return False
        
        self._is_flapping = True
        self._flapping_start = now
        self._flapping_episodes += 1
        self._last_detection_time = now
        
        # Escalate cooldown
        self._escalate_cooldown()
        
        if self._on_flapping_detected:
            try:
                self._on_flapping_detected(count, self._current_cooldown)
            except Exception as e:
                import sys
                print(f"[FlappingDetector] Flapping detected callback failed: {e}", file=sys.stderr)
        
        return True
    
    def _handle_critical(self, count: int) -> None:
        """Handle critical flapping level."""
        # Ensure we're in flapping state
        if not self._is_flapping:
            self._handle_flapping_detected(count, time.monotonic())
        
        # Max out cooldown
        self._current_cooldown = self.max_cooldown
    
    def _check_flapping_resolved(self, now: float) -> None:
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
                    import sys
                    print(f"[FlappingDetector] Flapping resolved callback failed: {e}", file=sys.stderr)
    
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
        """Get the most recent leadership changes."""
        return list(self._changes)[-count:]
    
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

