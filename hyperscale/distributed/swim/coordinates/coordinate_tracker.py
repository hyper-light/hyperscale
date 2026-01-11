import time

from hyperscale.distributed.models.coordinates import (
    NetworkCoordinate,
    VivaldiConfig,
)
from hyperscale.distributed.swim.coordinates.coordinate_engine import (
    NetworkCoordinateEngine,
)


class CoordinateTracker:
    """
    Tracks local and peer Vivaldi coordinates (AD-35).

    Provides RTT estimation, UCB calculation, and coordinate quality
    assessment for failure detection and routing decisions.
    """

    def __init__(
        self,
        engine: NetworkCoordinateEngine | None = None,
        config: VivaldiConfig | None = None,
    ) -> None:
        self._engine = engine or NetworkCoordinateEngine(config=config)
        self._peers: dict[str, NetworkCoordinate] = {}
        self._peer_last_seen: dict[str, float] = {}

    def get_coordinate(self) -> NetworkCoordinate:
        """Get the local node's coordinate."""
        return self._engine.get_coordinate()

    def update_peer_coordinate(
        self,
        peer_id: str,
        peer_coordinate: NetworkCoordinate,
        rtt_ms: float,
    ) -> NetworkCoordinate:
        """
        Update local coordinate based on RTT measurement to peer.

        Also stores the peer's coordinate for future RTT estimation.

        Args:
            peer_id: Identifier of the peer
            peer_coordinate: Peer's reported coordinate
            rtt_ms: Measured round-trip time in milliseconds

        Returns:
            Updated local coordinate
        """
        if rtt_ms <= 0.0:
            return self.get_coordinate()

        self._peers[peer_id] = peer_coordinate
        self._peer_last_seen[peer_id] = time.monotonic()
        return self._engine.update_with_rtt(peer_coordinate, rtt_ms / 1000.0)

    def estimate_rtt_ms(self, peer_coordinate: NetworkCoordinate) -> float:
        """Estimate RTT to a peer using Vivaldi distance."""
        return self._engine.estimate_rtt_ms(
            self._engine.get_coordinate(), peer_coordinate
        )

    def estimate_rtt_ucb_ms(
        self,
        peer_coordinate: NetworkCoordinate | None = None,
        peer_id: str | None = None,
    ) -> float:
        """
        Estimate RTT with upper confidence bound (AD-35 Task 12.1.4).

        Uses conservative estimates when coordinate quality is low.

        Args:
            peer_coordinate: Peer's coordinate (if known)
            peer_id: Peer ID to look up coordinate (if peer_coordinate not provided)

        Returns:
            RTT UCB in milliseconds
        """
        if peer_coordinate is None and peer_id is not None:
            peer_coordinate = self._peers.get(peer_id)

        return self._engine.estimate_rtt_ucb_ms(
            self._engine.get_coordinate(),
            peer_coordinate,
        )

    def get_peer_coordinate(self, peer_id: str) -> NetworkCoordinate | None:
        """Get stored coordinate for a peer."""
        return self._peers.get(peer_id)

    def coordinate_quality(
        self,
        coord: NetworkCoordinate | None = None,
    ) -> float:
        """
        Compute coordinate quality score (AD-35 Task 12.1.5).

        Args:
            coord: Coordinate to assess (defaults to local coordinate)

        Returns:
            Quality score in [0.0, 1.0]
        """
        return self._engine.coordinate_quality(coord)

    def is_converged(self) -> bool:
        """
        Check if local coordinate has converged (AD-35 Task 12.1.6).

        Returns:
            True if coordinate is converged and usable for routing
        """
        return self._engine.is_converged()

    def get_config(self) -> VivaldiConfig:
        """Get the Vivaldi configuration."""
        return self._engine.get_config()

    def cleanup_stale_peers(self, max_age_seconds: float | None = None) -> int:
        """
        Remove stale peer coordinates (AD-35 Task 12.1.8).

        Args:
            max_age_seconds: Maximum age for peer coordinates (defaults to config TTL)

        Returns:
            Number of peers removed
        """
        if max_age_seconds is None:
            max_age_seconds = self._engine.get_config().coord_ttl_seconds

        now = time.monotonic()
        stale_peers = [
            peer_id
            for peer_id, last_seen in self._peer_last_seen.items()
            if now - last_seen > max_age_seconds
        ]

        for peer_id in stale_peers:
            self._peers.pop(peer_id, None)
            self._peer_last_seen.pop(peer_id, None)

        return len(stale_peers)

    def get_peer_count(self) -> int:
        """Get the number of tracked peer coordinates."""
        return len(self._peers)

    def get_all_peer_ids(self) -> list[str]:
        """Get all tracked peer IDs."""
        return list(self._peers.keys())
