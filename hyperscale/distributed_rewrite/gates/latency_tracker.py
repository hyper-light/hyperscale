"""
Latency Tracker for peer gate healthcheck measurements.

Tracks round-trip latency samples to detect network degradation
within the gate cluster.
"""

import time
from dataclasses import dataclass


@dataclass(slots=True)
class LatencyConfig:
    """Configuration for latency tracking."""
    sample_max_age: float = 60.0  # Max age of samples in seconds
    sample_max_count: int = 100   # Max samples to keep per peer


class LatencyTracker:
    """
    Tracks latency measurements to peer gates.

    Used to detect network degradation within the gate cluster.
    High latency to all peers indicates network issues vs specific
    gate failures.
    """

    __slots__ = ('_samples', '_config')

    def __init__(
        self,
        sample_max_age: float = 60.0,
        sample_max_count: int = 100,
    ):
        """
        Initialize the latency tracker.

        Args:
            sample_max_age: Maximum age of samples to keep (seconds).
            sample_max_count: Maximum number of samples per peer.
        """
        self._config = LatencyConfig(
            sample_max_age=sample_max_age,
            sample_max_count=sample_max_count,
        )
        self._samples: dict[str, list[tuple[float, float]]] = {}  # peer_id -> [(timestamp, latency_ms)]

    def record_latency(self, peer_id: str, latency_ms: float) -> None:
        """
        Record latency measurement from a peer gate healthcheck.

        Args:
            peer_id: The peer gate's node ID.
            latency_ms: Round-trip latency in milliseconds.
        """
        now = time.monotonic()
        samples = self._samples.setdefault(peer_id, [])
        samples.append((now, latency_ms))

        # Prune old samples and limit count
        cutoff = now - self._config.sample_max_age
        self._samples[peer_id] = [
            (ts, lat) for ts, lat in samples
            if ts > cutoff
        ][-self._config.sample_max_count:]

    def get_average_latency(self) -> float | None:
        """
        Get average latency across all peer gates.

        Returns:
            Average latency in ms, or None if no samples available.
        """
        all_latencies = [
            lat for samples in self._samples.values()
            for _, lat in samples
        ]
        if not all_latencies:
            return None
        return sum(all_latencies) / len(all_latencies)

    def get_peer_latency(self, peer_id: str) -> float | None:
        """
        Get average latency to a specific peer gate.

        Args:
            peer_id: The peer gate's node ID.

        Returns:
            Average latency in ms, or None if no samples available.
        """
        samples = self._samples.get(peer_id)
        if not samples:
            return None
        return sum(lat for _, lat in samples) / len(samples)

    def get_all_peer_latencies(self) -> dict[str, float]:
        """
        Get average latency for all tracked peers.

        Returns:
            Dict mapping peer_id to average latency in ms.
        """
        return {
            peer_id: sum(lat for _, lat in samples) / len(samples)
            for peer_id, samples in self._samples.items()
            if samples
        }

    def remove_peer(self, peer_id: str) -> None:
        """
        Remove latency samples for a peer.

        Args:
            peer_id: The peer gate's node ID.
        """
        self._samples.pop(peer_id, None)

    def clear_all(self) -> None:
        """Clear all latency samples."""
        self._samples.clear()

    def get_sample_count(self, peer_id: str) -> int:
        """
        Get number of samples for a peer.

        Args:
            peer_id: The peer gate's node ID.

        Returns:
            Number of latency samples.
        """
        samples = self._samples.get(peer_id)
        return len(samples) if samples else 0
