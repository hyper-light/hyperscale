from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.models.coordinates import (
    NetworkCoordinate,
    VivaldiConfig,
)
from hyperscale.distributed.swim.coordinates.coordinate_engine import (
    NetworkCoordinateEngine,
)


_DEFAULT_CLOCK: Clock = RealClock()


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
        *,
        clock: Clock | None = None,
    ) -> None:
        self._engine = engine or NetworkCoordinateEngine(config=config or VivaldiConfig())
        self._peers: dict[str, NetworkCoordinate] = {}
        self._peer_last_seen: dict[str, float] = {}
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK

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

        self._require_dimensions(peer_coordinate)
        self._peers[peer_id] = peer_coordinate
        self._peer_last_seen[peer_id] = self._clock.monotonic()
        return self._engine.update_with_rtt(peer_coordinate, rtt_ms / 1000.0)

    def record_peer_coordinate(
        self,
        peer_id: str,
        peer_coordinate: NetworkCoordinate,
    ) -> None:
        """Remember where a peer is without a round-trip measurement to
        adjust our own coordinate by."""
        self._require_dimensions(peer_coordinate)
        self._peers[peer_id] = peer_coordinate
        self._peer_last_seen[peer_id] = self._clock.monotonic()

    def _require_dimensions(self, peer_coordinate: NetworkCoordinate) -> None:
        """Refuse a coordinate of another dimension: distances and updates
        pair components positionally, so a shorter or longer vector would
        silently truncate them."""
        dimensions = self._engine.get_config().dimensions
        if len(peer_coordinate.vec) != dimensions:
            raise ValueError(
                f"coordinate has {len(peer_coordinate.vec)} dimensions, not {dimensions}"
            )

    def estimate_rtt_ms(self, peer_coordinate: NetworkCoordinate) -> float:
        """Estimate RTT to a peer using Vivaldi distance."""
        return self._engine.estimate_rtt_ms(
            self._engine.get_coordinate(), peer_coordinate
        )

    def estimate_rtt_ucb_ms(self, peer_coordinate: NetworkCoordinate) -> float:
        """
        Estimate RTT with upper confidence bound (AD-35 Task 12.1.4): the
        Vivaldi distance to ``peer_coordinate`` plus a margin for both
        coordinates' error.

        Returns:
            RTT UCB in milliseconds
        """
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

        now = self._clock.monotonic()
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
