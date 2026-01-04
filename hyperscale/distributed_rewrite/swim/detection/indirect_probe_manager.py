"""
Indirect probe management for SWIM protocol.
"""

import time
from dataclasses import dataclass, field

from .pending_indirect_probe import PendingIndirectProbe
from ..core.protocols import LoggerProtocol


@dataclass
class IndirectProbeManager:
    """
    Manages indirect probe requests for SWIM protocol.
    
    When a direct probe to node B fails, node A asks k random other
    nodes to probe B on A's behalf. If any proxy gets a response from B,
    it forwards the ack back to A.
    
    This helps distinguish between:
    - B being actually failed
    - Network issues between A and B specifically
    
    Resource limits:
    - max_pending: Maximum concurrent pending probes
    - probe_ttl: Maximum time to keep probe records
    """
    pending_probes: dict[tuple[str, int], PendingIndirectProbe] = field(default_factory=dict)
    # Number of proxy nodes to use for indirect probing
    k_proxies: int = 3
    
    # Resource limits
    max_pending: int = 100
    """Maximum concurrent indirect probes."""
    
    probe_ttl: float = 30.0
    """Time-to-live for probe records (seconds)."""
    
    # Stats for monitoring
    _started_count: int = 0
    _completed_count: int = 0
    _expired_count: int = 0
    _rejected_count: int = 0  # Rejected due to max_pending limit
    
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
    
    def start_indirect_probe(
        self,
        target: tuple[str, int],
        requester: tuple[str, int],
        timeout: float,
    ) -> PendingIndirectProbe | None:
        """
        Start tracking an indirect probe request.
        
        Returns None if at max_pending limit.
        """
        # Check limit
        if len(self.pending_probes) >= self.max_pending:
            # Try cleanup first
            self.cleanup_expired()
            if len(self.pending_probes) >= self.max_pending:
                self._rejected_count += 1
                return None
        
        probe = PendingIndirectProbe(
            target=target,
            requester=requester,
            start_time=time.monotonic(),
            timeout=timeout,
        )
        self.pending_probes[target] = probe
        self._started_count += 1
        return probe
    
    def get_pending_probe(self, target: tuple[str, int]) -> PendingIndirectProbe | None:
        """Get the pending probe for a target, if any."""
        return self.pending_probes.get(target)
    
    def record_ack(self, target: tuple[str, int]) -> bool:
        """
        Record that the target responded to an indirect probe.
        Returns True if the probe was pending and this is the first ack.
        """
        probe = self.pending_probes.get(target)
        if probe and probe.record_ack():
            del self.pending_probes[target]
            self._completed_count += 1
            return True
        return False
    
    def cancel_probe(self, target: tuple[str, int]) -> bool:
        """Cancel a pending probe (e.g., target confirmed dead)."""
        if target in self.pending_probes:
            del self.pending_probes[target]
            return True
        return False
    
    def get_expired_probes(self) -> list[PendingIndirectProbe]:
        """Get all probes that have timed out without an ack."""
        expired = []
        to_remove = []
        # Snapshot to avoid dict mutation during iteration
        for target, probe in list(self.pending_probes.items()):
            if probe.is_expired():
                expired.append(probe)
                to_remove.append(target)
        for target in to_remove:
            del self.pending_probes[target]
            self._expired_count += 1
        return expired
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired probes.
        
        Returns:
            Number of probes removed.
        """
        expired = self.get_expired_probes()
        return len(expired)
    
    def cleanup_old(self) -> int:
        """
        Remove probes older than probe_ttl.
        
        Returns:
            Number of probes removed.
        """
        now = time.monotonic()
        cutoff = now - self.probe_ttl

        to_remove = []
        # Snapshot to avoid dict mutation during iteration
        for target, probe in list(self.pending_probes.items()):
            if probe.start_time < cutoff:
                to_remove.append(target)

        for target in to_remove:
            del self.pending_probes[target]
            self._expired_count += 1
        
        return len(to_remove)
    
    def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.
        
        Returns:
            Dict with cleanup stats.
        """
        expired = self.cleanup_expired()
        old = self.cleanup_old()
        
        return {
            'expired_removed': expired,
            'old_removed': old,
            'pending_probes': len(self.pending_probes),
        }
    
    def clear_all(self) -> None:
        """Clear all pending probes."""
        self.pending_probes.clear()
    
    def get_stats(self) -> dict[str, int]:
        """Get manager statistics for monitoring."""
        return {
            'pending_probes': len(self.pending_probes),
            'total_started': self._started_count,
            'total_completed': self._completed_count,
            'total_expired': self._expired_count,
            'total_rejected': self._rejected_count,
        }
    
    async def log_capacity_warning(self) -> bool:
        """
        Log a warning if probes are being rejected due to capacity.
        
        Returns True if a warning was logged.
        Should be called periodically (e.g., from cleanup loop).
        """
        if not self._logger or self._rejected_count == 0:
            return False
        
        try:
            from hyperscale.logging.hyperscale_logging_models import ServerDebug
            await self._logger.log(ServerDebug(
                message=f"[IndirectProbeManager] Probes rejected due to capacity: {self._rejected_count} "
                        f"(max_pending={self.max_pending}, pending={len(self.pending_probes)})",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            ))
            return True
        except Exception:
            return False

