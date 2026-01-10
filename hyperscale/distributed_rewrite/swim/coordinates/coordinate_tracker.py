from hyperscale.distributed_rewrite.models.coordinates import NetworkCoordinate
from hyperscale.distributed_rewrite.swim.coordinates.coordinate_engine import (
    NetworkCoordinateEngine,
)


class CoordinateTracker:
    def __init__(self, engine: NetworkCoordinateEngine | None = None) -> None:
        self._engine = engine or NetworkCoordinateEngine()
        self._peers: dict[str, NetworkCoordinate] = {}

    def get_coordinate(self) -> NetworkCoordinate:
        return self._engine.get_coordinate()

    def update_peer_coordinate(
        self,
        peer_id: str,
        peer_coordinate: NetworkCoordinate,
        rtt_ms: float,
    ) -> NetworkCoordinate:
        if rtt_ms <= 0.0:
            return self.get_coordinate()

        self._peers[peer_id] = peer_coordinate
        return self._engine.update_with_rtt(peer_coordinate, rtt_ms / 1000.0)

    def estimate_rtt_ms(self, peer_coordinate: NetworkCoordinate) -> float:
        return self._engine.estimate_rtt_ms(
            self._engine.get_coordinate(), peer_coordinate
        )

    def get_peer_coordinate(self, peer_id: str) -> NetworkCoordinate | None:
        return self._peers.get(peer_id)
