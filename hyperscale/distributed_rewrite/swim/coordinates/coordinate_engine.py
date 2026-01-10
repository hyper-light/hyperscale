import math
import time
from typing import Iterable

from hyperscale.distributed_rewrite.models.coordinates import NetworkCoordinate


class NetworkCoordinateEngine:
    def __init__(
        self,
        dimensions: int = 8,
        ce: float = 0.25,
        error_decay: float = 0.25,
        gravity: float = 0.01,
        height_adjustment: float = 0.25,
        adjustment_smoothing: float = 0.05,
        min_error: float = 0.05,
        max_error: float = 10.0,
    ) -> None:
        self._dimensions = dimensions
        self._ce = ce
        self._error_decay = error_decay
        self._gravity = gravity
        self._height_adjustment = height_adjustment
        self._adjustment_smoothing = adjustment_smoothing
        self._min_error = min_error
        self._max_error = max_error
        self._coordinate = NetworkCoordinate(
            vec=[0.0 for _ in range(dimensions)],
            height=0.0,
            adjustment=0.0,
            error=1.0,
        )

    def get_coordinate(self) -> NetworkCoordinate:
        return NetworkCoordinate(
            vec=list(self._coordinate.vec),
            height=self._coordinate.height,
            adjustment=self._coordinate.adjustment,
            error=self._coordinate.error,
            updated_at=self._coordinate.updated_at,
            sample_count=self._coordinate.sample_count,
        )

    def update_with_rtt(
        self, peer: NetworkCoordinate, rtt_seconds: float
    ) -> NetworkCoordinate:
        if rtt_seconds <= 0.0:
            return self.get_coordinate()

        predicted = self.estimate_rtt_seconds(self._coordinate, peer)
        diff = rtt_seconds - predicted

        vec_distance = self._vector_distance(self._coordinate.vec, peer.vec)
        unit = self._unit_vector(self._coordinate.vec, peer.vec, vec_distance)

        weight = self._weight(self._coordinate.error, peer.error)
        step = self._ce * weight

        for index, component in enumerate(unit):
            self._coordinate.vec[index] += step * diff * component
            self._coordinate.vec[index] *= 1.0 - self._gravity

        height_delta = self._height_adjustment * step * diff
        self._coordinate.height = max(0.0, self._coordinate.height + height_delta)

        adjustment_delta = self._adjustment_smoothing * diff
        self._coordinate.adjustment = self._clamp(
            self._coordinate.adjustment + adjustment_delta,
            -1.0,
            1.0,
        )

        new_error = self._coordinate.error + self._error_decay * (
            abs(diff) - self._coordinate.error
        )
        self._coordinate.error = self._clamp(
            new_error, self._min_error, self._max_error
        )
        self._coordinate.updated_at = time.monotonic()
        self._coordinate.sample_count += 1

        return self.get_coordinate()

    @staticmethod
    def estimate_rtt_seconds(
        local: NetworkCoordinate, peer: NetworkCoordinate
    ) -> float:
        vec_distance = NetworkCoordinateEngine._vector_distance(local.vec, peer.vec)
        rtt = vec_distance + local.height + peer.height
        adjusted = rtt + local.adjustment + peer.adjustment
        return adjusted if adjusted > 0.0 else 0.0

    @staticmethod
    def estimate_rtt_ms(local: NetworkCoordinate, peer: NetworkCoordinate) -> float:
        return NetworkCoordinateEngine.estimate_rtt_seconds(local, peer) * 1000.0

    @staticmethod
    def _vector_distance(left: Iterable[float], right: Iterable[float]) -> float:
        return math.sqrt(sum((l - r) ** 2 for l, r in zip(left, right)))

    @staticmethod
    def _unit_vector(
        left: list[float], right: list[float], distance: float
    ) -> list[float]:
        if distance <= 0.0:
            unit = [0.0 for _ in left]
            if unit:
                unit[0] = 1.0
            return unit
        return [(l - r) / distance for l, r in zip(left, right)]

    @staticmethod
    def _weight(local_error: float, peer_error: float) -> float:
        denom = local_error + peer_error
        if denom <= 0.0:
            return 1.0
        return local_error / denom

    @staticmethod
    def _clamp(value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, value))
