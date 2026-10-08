import math
from typing import Iterable

from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.models.coordinates import (
    NetworkCoordinate,
    VivaldiConfig,
)


_DEFAULT_CLOCK: Clock = RealClock()


class NetworkCoordinateEngine:
    def __init__(
        self,
        config: VivaldiConfig,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK
        self._config = config
        self._dimensions = self._config.dimensions
        self._ce = self._config.ce
        self._error_decay = self._config.error_decay
        self._gravity = self._config.gravity
        self._height_adjustment = self._config.height_adjustment
        self._adjustment_smoothing = self._config.adjustment_smoothing
        self._max_error = self._config.max_error
        self._coordinate = NetworkCoordinate(
            vec=[0.0 for _ in range(self._dimensions)],
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
        self._coordinate.error = self._clamp(new_error, 0.0, self._max_error)
        self._coordinate.updated_at = self._clock.monotonic()
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

    def estimate_rtt_ucb_ms(
        self,
        local: NetworkCoordinate,
        remote: NetworkCoordinate,
    ) -> float:
        """
        Estimate RTT with upper confidence bound (AD-35 Task 12.1.4).

        Uses Vivaldi distance plus a safety margin based on coordinate error.
        There is no estimate without both coordinates: a caller missing one
        has no evidence and decides for itself what that means.

        Formula: rtt_ucb = clamp(rtt_hat + K_SIGMA * sigma, RTT_MIN, RTT_MAX)

        Args:
            local: Local node coordinate
            remote: Remote node coordinate

        Returns:
            RTT upper confidence bound in milliseconds
        """
        # Estimate RTT from coordinate distance (in seconds, convert to ms)
        rtt_hat_ms = self.estimate_rtt_ms(local, remote)
        # Sigma is combined error of both coordinates (in seconds → ms)
        sigma_ms = self._clamp(
            (local.error + remote.error) * 1000.0,
            self._config.sigma_min_ms,
            self._config.sigma_max_ms,
        )

        # Apply UCB formula: rtt_hat + K_SIGMA * sigma
        rtt_ucb = rtt_hat_ms + self._config.k_sigma * sigma_ms

        return self._clamp(
            rtt_ucb,
            self._config.rtt_min_ms,
            self._config.rtt_max_ms,
        )

    def coordinate_quality(
        self,
        coord: NetworkCoordinate | None = None,
    ) -> float:
        """
        Compute coordinate quality score (AD-35 Task 12.1.5).

        Quality is a value in [0.0, 1.0] based on:
        - Sample count: More samples = higher quality
        - Error: Lower error = higher quality
        - Staleness: Fresher coordinates = higher quality

        Formula: quality = sample_quality * error_quality * staleness_quality

        Args:
            coord: Coordinate to assess (defaults to local coordinate)

        Returns:
            Quality score in [0.0, 1.0]
        """
        if coord is None:
            coord = self._coordinate

        # Sample quality: ramps up to 1.0 as sample_count approaches min_samples
        sample_quality = min(
            1.0,
            coord.sample_count / self._config.min_samples_for_routing,
        )

        # Error quality: error in seconds, config threshold in ms
        error_ms = coord.error * 1000.0
        error_quality = min(
            1.0,
            self._config.error_good_ms / max(error_ms, 1.0),
        )

        # Staleness quality: degrades after coord_ttl_seconds
        staleness_seconds = self._clock.monotonic() - coord.updated_at
        if staleness_seconds <= self._config.coord_ttl_seconds:
            staleness_quality = 1.0
        else:
            staleness_quality = self._config.coord_ttl_seconds / staleness_seconds

        # Combined quality (all factors multiplicative)
        quality = sample_quality * error_quality * staleness_quality

        return self._clamp(quality, 0.0, 1.0)

    def is_converged(self, coord: NetworkCoordinate | None = None) -> bool:
        """
        Check if coordinate has converged (AD-35 Task 12.1.6): it has the
        samples and the error that earn full coordinate quality --
        ``min_samples_for_routing`` and ``error_good_ms``.

        Args:
            coord: Coordinate to check (defaults to local coordinate)

        Returns:
            True if coordinate is converged
        """
        if coord is None:
            coord = self._coordinate

        return (
            coord.sample_count >= self._config.min_samples_for_routing
            and coord.error * 1000.0 <= self._config.error_good_ms
        )

    def get_config(self) -> VivaldiConfig:
        """Get the Vivaldi configuration."""
        return self._config
