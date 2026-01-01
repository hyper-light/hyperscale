"""
Simple metrics collection for SWIM protocol.

Provides counters and gauges for monitoring key events.
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Any

from .protocols import LoggerProtocol


@dataclass
class Metrics:
    """
    Simple metrics collector for SWIM protocol events.
    
    Tracks:
    - Counters: Increment-only values (probes, joins, etc.)
    - Gauges: Current state values (active members, suspicions, etc.)
    - Timing: Track operation durations
    
    Thread-safe for concurrent updates using threading.Lock.
    This protects the read-modify-write pattern in increment() and
    compound operations in reset().
    """
    
    # Probe metrics
    probes_sent: int = 0
    probes_received: int = 0
    probes_failed: int = 0
    probes_timeout: int = 0
    indirect_probes_sent: int = 0
    indirect_probes_success: int = 0
    
    # Membership metrics
    joins_received: int = 0
    joins_propagated: int = 0
    leaves_received: int = 0
    leaves_propagated: int = 0
    
    # Suspicion metrics
    suspicions_started: int = 0
    suspicions_confirmed: int = 0
    suspicions_refuted: int = 0
    suspicions_expired: int = 0
    
    # Election metrics
    elections_started: int = 0
    elections_won: int = 0
    elections_lost: int = 0
    pre_votes_started: int = 0
    pre_votes_granted: int = 0
    
    # Leadership metrics
    heartbeats_sent: int = 0
    heartbeats_received: int = 0
    leadership_changes: int = 0
    split_brain_events: int = 0
    
    # Error metrics
    network_errors: int = 0
    protocol_errors: int = 0
    resource_errors: int = 0
    
    # Gossip metrics
    gossip_updates_sent: int = 0
    gossip_updates_received: int = 0
    gossip_buffer_overflows: int = 0
    
    # Rate limiting and dedup
    messages_rate_limited: int = 0
    messages_deduplicated: int = 0
    
    # Start time for uptime calculation
    _start_time: float = field(default_factory=time.monotonic)
    
    # Lock for thread-safe updates
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    # Track saturated counters for monitoring
    _saturated_counters: set[str] = field(default_factory=set, repr=False)
    
    # Maximum counter value to prevent overflow
    MAX_COUNTER_VALUE: int = 2**31 - 1
    
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
    
    def increment(self, metric: str, amount: int = 1) -> None:
        """Increment a counter metric with overflow protection."""
        with self._lock:
            if hasattr(self, metric):
                current = getattr(self, metric)
                if current < self.MAX_COUNTER_VALUE:
                    new_value = min(current + amount, self.MAX_COUNTER_VALUE)
                    setattr(self, metric, new_value)
                    # Track saturation for monitoring
                    if new_value >= self.MAX_COUNTER_VALUE:
                        self._saturated_counters.add(metric)
    
    def get(self, metric: str) -> int:
        """Get current value of a metric."""
        return getattr(self, metric, 0)
    
    def get_saturated_counters(self) -> set[str]:
        """Get set of counters that have reached MAX_COUNTER_VALUE."""
        with self._lock:
            return self._saturated_counters.copy()
    
    async def log_saturation_warnings(self) -> int:
        """
        Log warnings for any saturated counters.
        
        Returns the number of saturated counters.
        Should be called periodically (e.g., from cleanup loop).
        """
        if not self._logger or not self._saturated_counters:
            return 0
        
        with self._lock:
            saturated = list(self._saturated_counters)
        
        if saturated:
            try:
                from hyperscale.logging.hyperscale_logging_models import ServerDebug
                await self._logger.log(ServerDebug(
                    message=f"[Metrics] Counters saturated at MAX_COUNTER_VALUE: {', '.join(saturated)}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
        
        return len(saturated)
    
    def uptime(self) -> float:
        """Get uptime in seconds."""
        return time.monotonic() - self._start_time
    
    def to_dict(self) -> dict[str, Any]:
        """Export all metrics as a dictionary."""
        return {
            'uptime_seconds': self.uptime(),
            'probes': {
                'sent': self.probes_sent,
                'received': self.probes_received,
                'failed': self.probes_failed,
                'timeout': self.probes_timeout,
                'indirect_sent': self.indirect_probes_sent,
                'indirect_success': self.indirect_probes_success,
            },
            'membership': {
                'joins_received': self.joins_received,
                'joins_propagated': self.joins_propagated,
                'leaves_received': self.leaves_received,
                'leaves_propagated': self.leaves_propagated,
            },
            'suspicions': {
                'started': self.suspicions_started,
                'confirmed': self.suspicions_confirmed,
                'refuted': self.suspicions_refuted,
                'expired': self.suspicions_expired,
            },
            'elections': {
                'started': self.elections_started,
                'won': self.elections_won,
                'lost': self.elections_lost,
                'pre_votes_started': self.pre_votes_started,
                'pre_votes_granted': self.pre_votes_granted,
            },
            'leadership': {
                'heartbeats_sent': self.heartbeats_sent,
                'heartbeats_received': self.heartbeats_received,
                'changes': self.leadership_changes,
                'split_brain_events': self.split_brain_events,
            },
            'errors': {
                'network': self.network_errors,
                'protocol': self.protocol_errors,
                'resource': self.resource_errors,
            },
            'gossip': {
                'updates_sent': self.gossip_updates_sent,
                'updates_received': self.gossip_updates_received,
                'buffer_overflows': self.gossip_buffer_overflows,
            },
            'rate_limiting': {
                'rate_limited': self.messages_rate_limited,
                'deduplicated': self.messages_deduplicated,
            },
        }
    
    def reset(self) -> None:
        """Reset all counters to zero."""
        with self._lock:
            for name in dir(self):
                if not name.startswith('_') and isinstance(getattr(self, name), int):
                    setattr(self, name, 0)
            self._start_time = time.monotonic()

