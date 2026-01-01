"""
Graceful degradation for SWIM protocol under high load.

When a node is overloaded (high LHM, event loop lag, etc.), it should:
1. Reduce outgoing traffic (skip probes, reduce gossip)
2. Step down from leadership
3. Extend timeouts to avoid false positives
4. Shed load progressively
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Any, Protocol

from hyperscale.logging.hyperscale_logging_models import ServerDebug


class LoggerProtocol(Protocol):
    """Protocol for logger to avoid circular imports."""
    async def log(self, entry: Any) -> None: ...


class DegradationLevel(Enum):
    """Levels of graceful degradation."""
    NORMAL = 0       # Normal operation
    LIGHT = 1        # Minor load shedding
    MODERATE = 2     # Significant load shedding
    HEAVY = 3        # Major load shedding
    CRITICAL = 4     # Emergency mode - minimal operation


@dataclass
class DegradationPolicy:
    """
    Policy for graceful degradation behavior at each level.
    
    Higher degradation levels progressively shed more load while
    maintaining core functionality (responding to probes, gossip).
    """
    
    # Probe rate multiplier (1.0 = normal, 0.5 = half rate)
    probe_rate: float = 1.0
    
    # Gossip rate multiplier
    gossip_rate: float = 1.0
    
    # Max piggyback updates per message
    max_piggyback_updates: int = 5
    
    # Timeout multiplier (extends all timeouts)
    timeout_multiplier: float = 1.0
    
    # Should step down from leadership
    should_step_down: bool = False
    
    # Should refuse leadership candidacy
    refuse_leadership: bool = False
    
    # Skip indirect probing when overloaded
    skip_indirect_probing: bool = False
    
    # Description for logging
    description: str = ""


# Pre-defined policies for each degradation level
DEGRADATION_POLICIES: dict[DegradationLevel, DegradationPolicy] = {
    DegradationLevel.NORMAL: DegradationPolicy(
        probe_rate=1.0,
        gossip_rate=1.0,
        max_piggyback_updates=5,
        timeout_multiplier=1.0,
        should_step_down=False,
        refuse_leadership=False,
        skip_indirect_probing=False,
        description="Normal operation",
    ),
    DegradationLevel.LIGHT: DegradationPolicy(
        probe_rate=0.9,
        gossip_rate=0.8,
        max_piggyback_updates=4,
        timeout_multiplier=1.2,
        should_step_down=False,
        refuse_leadership=False,
        skip_indirect_probing=False,
        description="Light load shedding: reduced gossip, extended timeouts",
    ),
    DegradationLevel.MODERATE: DegradationPolicy(
        probe_rate=0.7,
        gossip_rate=0.6,
        max_piggyback_updates=3,
        timeout_multiplier=1.5,
        should_step_down=False,
        refuse_leadership=True,  # Won't become new leader
        skip_indirect_probing=False,
        description="Moderate load shedding: refusing new leadership",
    ),
    DegradationLevel.HEAVY: DegradationPolicy(
        probe_rate=0.5,
        gossip_rate=0.4,
        max_piggyback_updates=2,
        timeout_multiplier=2.0,
        should_step_down=True,  # Will step down if leader
        refuse_leadership=True,
        skip_indirect_probing=True,  # Skip helping others probe
        description="Heavy load shedding: stepping down, minimal participation",
    ),
    DegradationLevel.CRITICAL: DegradationPolicy(
        probe_rate=0.25,
        gossip_rate=0.2,
        max_piggyback_updates=1,
        timeout_multiplier=3.0,
        should_step_down=True,
        refuse_leadership=True,
        skip_indirect_probing=True,
        description="Critical: emergency mode, survival operations only",
    ),
}


@dataclass
class GracefulDegradation:
    """
    Manages graceful degradation based on node health metrics.
    
    Integrates with:
    - LocalHealthMultiplier (LHM score)
    - EventLoopHealthMonitor (lag detection)
    - Leadership election (step-down, eligibility)
    - Probe scheduler (rate limiting)
    - Gossip buffer (reduced updates)
    
    Example:
        degradation = GracefulDegradation()
        degradation.set_health_callbacks(
            get_lhm=lambda: lhm.score,
            get_event_loop_lag=lambda: health_monitor.average_lag_ratio,
        )
        
        # In probe loop:
        if degradation.should_skip_probe():
            continue
        
        policy = degradation.get_current_policy()
        timeout = base_timeout * policy.timeout_multiplier
    """
    
    # Thresholds for transitioning between levels
    lhm_thresholds: tuple[int, ...] = (2, 4, 6, 7)
    """LHM scores for transitioning to LIGHT, MODERATE, HEAVY, CRITICAL."""
    
    lag_thresholds: tuple[float, ...] = (0.5, 1.0, 1.5, 2.0)
    """Lag ratios for transitioning to LIGHT, MODERATE, HEAVY, CRITICAL."""
    
    # Hysteresis to prevent rapid oscillation
    hysteresis_factor: float = 0.8
    """Threshold multiplier for transitioning down (e.g., 0.8 = 80% of threshold)."""
    
    # Current state
    _current_level: DegradationLevel = DegradationLevel.NORMAL
    _level_entered_at: float = field(default_factory=time.monotonic)
    _min_level_duration: float = 5.0  # Min seconds at a level before changing
    
    # Callbacks
    _get_lhm: Callable[[], int] | None = None
    _get_event_loop_lag: Callable[[], float] | None = None
    _on_level_change: Callable[[DegradationLevel, DegradationLevel], None] | None = None
    
    # Skip tracking for probabilistic rate limiting
    _probe_skip_counter: int = 0
    _gossip_skip_counter: int = 0
    
    # Stats
    _level_changes: int = 0
    _probes_skipped: int = 0
    _gossips_skipped: int = 0
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
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
                    message=f"[GracefulDegradation] {message}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
    
    def __post_init__(self):
        self._level_entered_at = time.monotonic()
    
    def set_health_callbacks(
        self,
        get_lhm: Callable[[], int] | None = None,
        get_event_loop_lag: Callable[[], float] | None = None,
        on_level_change: Callable[[DegradationLevel, DegradationLevel], None] | None = None,
    ) -> None:
        """Set callbacks for health metrics."""
        self._get_lhm = get_lhm
        self._get_event_loop_lag = get_event_loop_lag
        self._on_level_change = on_level_change
    
    async def update(self) -> DegradationLevel:
        """
        Update degradation level based on current health metrics.
        
        Returns:
            The current (possibly updated) degradation level.
        """
        new_level = self._calculate_level()
        
        # Check if we can change level (hysteresis)
        now = time.monotonic()
        time_at_level = now - self._level_entered_at
        
        if new_level != self._current_level and time_at_level >= self._min_level_duration:
            old_level = self._current_level
            self._current_level = new_level
            self._level_entered_at = now
            self._level_changes += 1
            
            if self._on_level_change:
                try:
                    self._on_level_change(old_level, new_level)
                except Exception as e:
                    await self._log_debug(
                        f"Level change callback error "
                        f"({old_level.name} -> {new_level.name}): "
                        f"{type(e).__name__}: {e}"
                    )
        
        return self._current_level
    
    def _calculate_level(self) -> DegradationLevel:
        """Calculate what degradation level we should be at."""
        lhm_level = DegradationLevel.NORMAL
        lag_level = DegradationLevel.NORMAL
        
        # Check LHM
        if self._get_lhm:
            lhm = self._get_lhm()
            effective_thresholds = self._get_effective_thresholds(
                self.lhm_thresholds, self._current_level
            )
            lhm_level = self._threshold_to_level(lhm, effective_thresholds)
        
        # Check event loop lag
        if self._get_event_loop_lag:
            lag = self._get_event_loop_lag()
            effective_thresholds = self._get_effective_thresholds(
                self.lag_thresholds, self._current_level
            )
            lag_level = self._threshold_to_level(lag, effective_thresholds)
        
        # Use the worse of the two
        return max(lhm_level, lag_level, key=lambda l: l.value)
    
    def _get_effective_thresholds(
        self,
        thresholds: tuple[int | float, ...],
        current_level: DegradationLevel,
    ) -> tuple[float, ...]:
        """
        Get effective thresholds with hysteresis applied.
        
        When at a higher level, thresholds for moving down are lower
        to prevent rapid oscillation.
        """
        result = []
        for i, t in enumerate(thresholds):
            # If we're at or above this level, apply hysteresis
            if i < current_level.value:
                result.append(t * self.hysteresis_factor)
            else:
                result.append(float(t))
        return tuple(result)
    
    def _threshold_to_level(
        self,
        value: float | int,
        thresholds: tuple[float, ...],
    ) -> DegradationLevel:
        """Convert a metric value to a degradation level."""
        for i, threshold in enumerate(thresholds):
            if value < threshold:
                return DegradationLevel(i)
        return DegradationLevel.CRITICAL
    
    @property
    def current_level(self) -> DegradationLevel:
        """Get the current degradation level."""
        return self._current_level
    
    def get_current_policy(self) -> DegradationPolicy:
        """Get the policy for the current degradation level."""
        return DEGRADATION_POLICIES[self._current_level]
    
    def should_skip_probe(self) -> bool:
        """
        Check if this probe round should be skipped.
        
        Uses probabilistic skipping based on probe_rate.
        """
        policy = self.get_current_policy()
        if policy.probe_rate >= 1.0:
            return False
        
        # Deterministic skip pattern for fairness
        self._probe_skip_counter += 1
        skip_interval = int(1.0 / (1.0 - policy.probe_rate))
        
        if self._probe_skip_counter >= skip_interval:
            self._probe_skip_counter = 0
            self._probes_skipped += 1
            return True
        
        return False
    
    def should_skip_gossip(self) -> bool:
        """
        Check if gossip should be skipped this round.
        
        Uses probabilistic skipping based on gossip_rate.
        """
        policy = self.get_current_policy()
        if policy.gossip_rate >= 1.0:
            return False
        
        self._gossip_skip_counter += 1
        skip_interval = int(1.0 / (1.0 - policy.gossip_rate))
        
        if self._gossip_skip_counter >= skip_interval:
            self._gossip_skip_counter = 0
            self._gossips_skipped += 1
            return True
        
        return False
    
    def should_step_down(self) -> bool:
        """Check if we should step down from leadership."""
        return self.get_current_policy().should_step_down
    
    def should_refuse_leadership(self) -> bool:
        """Check if we should refuse leadership candidacy."""
        return self.get_current_policy().refuse_leadership
    
    def should_skip_indirect_probe(self) -> bool:
        """Check if we should skip helping with indirect probes."""
        return self.get_current_policy().skip_indirect_probing
    
    def get_timeout_multiplier(self) -> float:
        """Get the current timeout multiplier."""
        return self.get_current_policy().timeout_multiplier
    
    def get_max_piggyback_updates(self) -> int:
        """Get the maximum piggyback updates for current level."""
        return self.get_current_policy().max_piggyback_updates
    
    def is_degraded(self) -> bool:
        """Check if we're in any degraded state."""
        return self._current_level != DegradationLevel.NORMAL
    
    async def force_level(self, level: DegradationLevel) -> None:
        """Force a specific degradation level (for testing)."""
        if level != self._current_level:
            old_level = self._current_level
            self._current_level = level
            self._level_entered_at = time.monotonic()
            if self._on_level_change:
                try:
                    self._on_level_change(old_level, level)
                except Exception as e:
                    await self._log_debug(
                        f"Force level callback error "
                        f"({old_level.name} -> {level.name}): "
                        f"{type(e).__name__}: {e}"
                    )
    
    async def reset(self) -> None:
        """Reset to normal operation."""
        await self.force_level(DegradationLevel.NORMAL)
        self._probe_skip_counter = 0
        self._gossip_skip_counter = 0
    
    def get_stats(self) -> dict[str, Any]:
        """Get degradation statistics."""
        policy = self.get_current_policy()
        return {
            'level': self._current_level.name,
            'level_value': self._current_level.value,
            'description': policy.description,
            'probe_rate': policy.probe_rate,
            'gossip_rate': policy.gossip_rate,
            'timeout_multiplier': policy.timeout_multiplier,
            'level_changes': self._level_changes,
            'probes_skipped': self._probes_skipped,
            'gossips_skipped': self._gossips_skipped,
            'time_at_level': time.monotonic() - self._level_entered_at,
        }

