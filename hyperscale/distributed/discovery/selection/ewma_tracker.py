"""
Exponentially Weighted Moving Average (EWMA) latency tracker.

Tracks per-peer latency with exponential smoothing for load-aware selection.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from hyperscale.distributed.runtime import Clock, RealClock

from .ewma_config import EWMAConfig
from .peer_latency_stats import PeerLatencyStats

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass
class EWMATracker:
    """
    Track per-peer latency using Exponentially Weighted Moving Average.

    Provides load-aware peer selection by tracking response latencies
    and applying penalties for failures.

    Usage:
        tracker = EWMATracker()
        tracker.record_success("peer1", latency_ms=15.5)
        tracker.record_failure("peer2")

        # Get best peer (lowest effective latency)
        best = tracker.get_best_peer(["peer1", "peer2"])

        # Get effective latency including failure penalty
        latency = tracker.get_effective_latency("peer1")
    """

    config: EWMAConfig = field(default_factory=EWMAConfig)
    """Configuration for EWMA calculation."""

    _stats: dict[str, PeerLatencyStats] = field(default_factory=dict)
    """Per-peer latency statistics."""

    def record_success(self, peer_id: str, latency_ms: float) -> PeerLatencyStats:
        """
        Record a successful request with latency.

        Args:
            peer_id: The peer that handled the request
            latency_ms: Request latency in milliseconds

        Returns:
            Updated stats for the peer
        """
        stats = self._get_or_create_stats(peer_id)

        # Update EWMA
        if stats.sample_count == 0:
            # First sample: use as-is
            stats.ewma_ms = latency_ms
        else:
            # EWMA update: new = alpha * sample + (1 - alpha) * old
            stats.ewma_ms = (
                self.config.alpha * latency_ms
                + (1 - self.config.alpha) * stats.ewma_ms
            )

        # Update other stats
        stats.sample_count += 1
        stats.last_sample_ms = latency_ms
        stats.last_updated = _DEFAULT_CLOCK.monotonic()
        stats.min_ms = min(stats.min_ms, latency_ms)
        stats.max_ms = max(stats.max_ms, latency_ms)
        stats.failure_count = 0  # Reset on success

        return stats

    def record_failure(self, peer_id: str) -> PeerLatencyStats:
        """
        Record a failed request.

        Increments failure count which adds penalty to effective latency.

        Args:
            peer_id: The peer that failed

        Returns:
            Updated stats for the peer
        """
        stats = self._get_or_create_stats(peer_id)
        stats.failure_count += 1
        stats.last_updated = _DEFAULT_CLOCK.monotonic()
        return stats

    def get_effective_latency(self, peer_id: str) -> float:
        """
        Get the effective latency for a peer including failure penalty.

        Args:
            peer_id: The peer to look up

        Returns:
            Effective latency in milliseconds
        """
        stats = self._stats.get(peer_id)
        if stats is None:
            return self.config.initial_estimate_ms

        # Calculate failure penalty
        penalty = min(
            stats.failure_count * self.config.failure_penalty_ms,
            self.config.max_failure_penalty_ms,
        )

        return stats.ewma_ms + penalty

    def get_best_peer(self, peer_ids: list[str]) -> str | None:
        """
        Select the peer with lowest effective latency.

        Args:
            peer_ids: List of candidate peer IDs

        Returns:
            peer_id with lowest effective latency, or None if empty
        """
        if not peer_ids:
            return None

        best_peer: str | None = None
        best_latency = float("inf")

        for peer_id in peer_ids:
            latency = self.get_effective_latency(peer_id)
            if latency < best_latency:
                best_latency = latency
                best_peer = peer_id

        return best_peer

    def get_stats(self, peer_id: str) -> PeerLatencyStats | None:
        """
        Get raw stats for a peer.

        Args:
            peer_id: The peer to look up

        Returns:
            PeerLatencyStats or None if not tracked
        """
        return self._stats.get(peer_id)

    def get_all_stats(self) -> dict[str, PeerLatencyStats]:
        """Get all peer statistics."""
        return dict(self._stats)

    def decay_failure_counts(self) -> int:
        """
        Decay failure counts for all peers.

        Call periodically to allow failed peers to recover.

        Returns:
            Number of peers with decayed failure counts
        """
        decayed = 0
        for stats in self._stats.values():
            if stats.failure_count > 0:
                stats.failure_count = max(0, stats.failure_count - 1)
                decayed += 1
        return decayed

    def remove_peer(self, peer_id: str) -> bool:
        """
        Remove tracking for a peer.

        Args:
            peer_id: The peer to remove

        Returns:
            True if removed, False if not found
        """
        if peer_id in self._stats:
            del self._stats[peer_id]
            return True
        return False

    def reset_peer(self, peer_id: str) -> bool:
        """
        Reset statistics for a peer to initial state.

        Args:
            peer_id: The peer to reset

        Returns:
            True if reset, False if not found
        """
        if peer_id in self._stats:
            self._stats[peer_id] = PeerLatencyStats(peer_id=peer_id)
            return True
        return False

    def clear(self) -> int:
        """
        Clear all peer statistics.

        Returns:
            Number of peers cleared
        """
        count = len(self._stats)
        self._stats.clear()
        return count

    def _get_or_create_stats(self, peer_id: str) -> PeerLatencyStats:
        """Get or create stats for a peer."""
        stats = self._stats.get(peer_id)
        if stats is None:
            stats = PeerLatencyStats(peer_id=peer_id)
            self._stats[peer_id] = stats
        return stats

    @property
    def tracked_peer_count(self) -> int:
        """Return the number of tracked peers."""
        return len(self._stats)

_REHOMED = (
    PeerLatencyStats,
    EWMAConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
