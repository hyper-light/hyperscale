import math
import time
from typing import Iterable

from hyperscale.distributed_rewrite.models.coordinates import (
    NetworkCoordinate,
    VivaldiConfig,
)


class NetworkCoordinateEngine:
    def __init__(
        self,
        config: VivaldiConfig | None = None,
        dimensions: int = 8,
        ce: float = 0.25,
        error_decay: float = 0.25,
        gravity: float = 0.01,
        height_adjustment: float = 0.25,
        adjustment_smoothing: float = 0.05,
        min_error: float = 0.05,
        max_error: float = 10.0,
    ) -> None:
        # Use config if provided, otherwise use individual parameters
        self._config = config or VivaldiConfig(
            dimensions=dimensions,
            ce=ce,
            error_decay=error_decay,
            gravity=gravity,
            height_adjustment=height_adjustment,
            adjustment_smoothing=adjustment_smoothing,
            min_error=min_error,
            max_error=max_error,
        )
        self._dimensions = self._config.dimensions
        self._ce = self._config.ce
        self._error_decay = self._config.error_decay
        self._gravity = self._config.gravity
        self._height_adjustment = self._config.height_adjustment
        self._adjustment_smoothing = self._config.adjustment_smoothing
        self._min_error = self._config.min_error
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

    def estimate_rtt_ucb_ms(
        self,
        local: NetworkCoordinate | None,
        remote: NetworkCoordinate | None,
    ) -> float:
        """
        Estimate RTT with upper confidence bound (AD-35 Task 12.1.4).

        Uses Vivaldi distance plus a safety margin based on coordinate error.
        Falls back to conservative defaults when coordinates are unavailable.

        Formula: rtt_ucb = clamp(rtt_hat + K_SIGMA * sigma, RTT_MIN, RTT_MAX)

        Args:
            local: Local node coordinate (or None for default)
            remote: Remote node coordinate (or None for default)

        Returns:
            RTT upper confidence bound in milliseconds
        """
        if local is None or remote is None:
            rtt_hat_ms = self._config.rtt_default_ms
            sigma_ms = self._config.sigma_default_ms
        else:
            # Estimate RTT from coordinate distance (in seconds, convert to ms)
            rtt_hat_ms = self.estimate_rtt_ms(local, remote)
            # Sigma is combined error of both coordinates (in seconds → ms)
            combined_error = (local.error + remote.error) * 1000.0
            sigma_ms = self._clamp(
                combined_error,
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
        staleness_seconds = time.monotonic() - coord.updated_at
        if staleness_seconds <= self._config.coord_ttl_seconds:
            staleness_quality = 1.0
        else:
            staleness_quality = self._config.coord_ttl_seconds / staleness_seconds

        # Combined quality (all factors multiplicative)
        quality = sample_quality * error_quality * staleness_quality

        return self._clamp(quality, 0.0, 1.0)

    def is_converged(self, coord: NetworkCoordinate | None = None) -> bool:
        """
        Check if coordinate has converged (AD-35 Task 12.1.6).

        A coordinate is converged when:
        - Error is below the convergence threshold
        - Sample count is at or above minimum

        Args:
            coord: Coordinate to check (defaults to local coordinate)

        Returns:
            True if coordinate is converged
        """
        if coord is None:
            coord = self._coordinate

        error_converged = coord.error <= self._config.convergence_error_threshold
        samples_sufficient = coord.sample_count >= self._config.convergence_min_samples

        return error_converged and samples_sufficient

    def get_config(self) -> VivaldiConfig:
        """Get the Vivaldi configuration."""
        return self._config
