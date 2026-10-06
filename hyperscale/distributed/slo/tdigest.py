from __future__ import annotations

from dataclasses import dataclass, field

import msgspec
import numpy as np

from .centroid import Centroid
from .slo_config import SLOConfig


@dataclass(slots=True)
class TDigest:
    """T-Digest for streaming quantile estimation."""

    _config: SLOConfig
    _centroids: list[Centroid] = field(default_factory=list, init=False)
    _unmerged: list[tuple[float, float]] = field(default_factory=list, init=False)
    _total_weight: float = field(default=0.0, init=False)
    _min: float = field(default=float("inf"), init=False)
    _max: float = field(default=float("-inf"), init=False)

    @property
    def delta(self) -> float:
        """Compression parameter."""
        return self._config.tdigest_delta

    @property
    def max_unmerged(self) -> int:
        """Max unmerged points before compression."""
        return self._config.tdigest_max_unmerged

    def add(self, value: float, weight: float = 1.0) -> None:
        """Add a value to the digest."""
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight}")
        self._unmerged.append((value, weight))
        self._total_weight += weight
        self._min = min(self._min, value)
        self._max = max(self._max, value)

        if len(self._unmerged) >= self.max_unmerged:
            self._compress()

    def add_batch(self, values: list[float]) -> None:
        """Add multiple values efficiently."""
        for value in values:
            self.add(value)

    def _collect_points(self) -> list[tuple[float, float]]:
        points = [(centroid.mean, centroid.weight) for centroid in self._centroids]
        points.extend(self._unmerged)
        return points

    def _compress(self) -> None:
        """Compress unmerged points into centroids."""
        points = self._collect_points()
        if not points:
            self._centroids = []
            self._unmerged.clear()
            self._total_weight = 0.0
            return

        points.sort(key=lambda entry: entry[0])
        total_weight = sum(weight for _, weight in points)
        if total_weight <= 0:
            self._centroids = []
            self._unmerged.clear()
            self._total_weight = 0.0
            return

        # Dunning & Ertl (2019), "Computing Extremely Accurate Quantiles
        # Using t-Digests", Algorithm 1: a centroid grows while it spans at
        # most one unit of k from its left edge, ``q_left``. The limit is
        # fixed when a centroid opens; re-deriving it from the growing right
        # edge let a lower-tail centroid hold several units of k and, past
        # k(q) = delta/2 - 1/2, made the limit negative -- every upper-tail
        # point became a centroid of its own, without bound in the stream.
        new_centroids: list[Centroid] = []
        current_mean, current_weight = points[0]
        weight_before_current = 0.0
        quantile_limit = self._k_inverse(self._k(0.0) + 1.0)

        for mean, weight in points[1:]:
            if (
                weight_before_current + current_weight + weight
            ) / total_weight <= quantile_limit:
                new_weight = current_weight + weight
                current_mean = (
                    current_mean * current_weight + mean * weight
                ) / new_weight
                current_weight = new_weight
            else:
                new_centroids.append(Centroid(current_mean, current_weight))
                weight_before_current += current_weight
                quantile_limit = self._k_inverse(
                    self._k(weight_before_current / total_weight) + 1.0
                )
                current_mean = mean
                current_weight = weight

        new_centroids.append(Centroid(current_mean, current_weight))
        self._centroids = new_centroids
        self._unmerged.clear()
        self._total_weight = total_weight

    def _k(self, quantile: float) -> float:
        """Scaling function k(q) = δ/2 * (arcsin(2q-1)/π + 0.5)."""
        return (self.delta / 2.0) * (np.arcsin(2.0 * quantile - 1.0) / np.pi + 0.5)

    def _k_inverse(self, scaled: float) -> float:
        """Inverse scaling function; k never exceeds k(1) = delta/2, so a
        scale past it is the whole digest."""
        return 0.5 * (
            np.sin((min(scaled, self.delta / 2.0) / (self.delta / 2.0) - 0.5) * np.pi)
            + 1.0
        )

    def quantile(self, quantile: float) -> float:
        """Get the value at quantile q (0 <= q <= 1)."""
        if quantile < 0.0 or quantile > 1.0:
            raise ValueError(f"Quantile must be in [0, 1], got {quantile}")

        self._compress()

        if not self._centroids:
            return 0.0

        if quantile == 0.0:
            return self._min
        if quantile == 1.0:
            return self._max

        # Dunning & Ertl (2019) section 2.3: a centroid's weight is centred
        # on its mean, so the estimate interpolates between adjacent
        # centroids' midpoints -- from the minimum at weight 0 to the first
        # midpoint, and from the last midpoint to the maximum at the total.
        # Extrapolating past a centroid's midpoint along the previous
        # segment broke monotonicity and overshot the maximum.
        target_weight = quantile * self._total_weight
        first_centroid = self._centroids[0]
        if target_weight <= first_centroid.weight / 2.0:
            return self._min + (target_weight / (first_centroid.weight / 2.0)) * (
                first_centroid.mean - self._min
            )

        weight_before_previous = 0.0
        for previous_centroid, centroid in zip(self._centroids, self._centroids[1:]):
            midpoint_previous = weight_before_previous + previous_centroid.weight / 2.0
            midpoint_current = (
                weight_before_previous + previous_centroid.weight + centroid.weight / 2.0
            )
            if target_weight <= midpoint_current:
                ratio = (target_weight - midpoint_previous) / (
                    midpoint_current - midpoint_previous
                )
                return previous_centroid.mean + ratio * (
                    centroid.mean - previous_centroid.mean
                )
            weight_before_previous += previous_centroid.weight

        last_centroid = self._centroids[-1]
        midpoint_last = self._total_weight - last_centroid.weight / 2.0
        ratio = (target_weight - midpoint_last) / (last_centroid.weight / 2.0)
        return last_centroid.mean + min(ratio, 1.0) * (self._max - last_centroid.mean)

    def p50(self) -> float:
        """Median."""
        return self.quantile(0.50)

    def p95(self) -> float:
        """95th percentile."""
        return self.quantile(0.95)

    def p99(self) -> float:
        """99th percentile."""
        return self.quantile(0.99)

    def count(self) -> float:
        """Total weight (count if weights are 1)."""
        return self._total_weight

    def merge(self, other: "TDigest") -> "TDigest":
        """Merge another digest into this one."""
        self._compress()
        other._compress()

        combined_points = self._collect_points()
        combined_points.extend(other._collect_points())

        if not combined_points:
            return self

        self._centroids = []
        self._unmerged = combined_points
        self._total_weight = sum(weight for _, weight in combined_points)
        self._min = min(self._min, other._min)
        self._max = max(self._max, other._max)
        self._compress()
        return self

    def to_bytes(self) -> bytes:
        """Serialize for SWIM gossip transfer."""
        self._compress()
        payload = {
            "centroids": [
                (centroid.mean, centroid.weight) for centroid in self._centroids
            ],
            "total_weight": self._total_weight,
            "min": self._min if self._min != float("inf") else None,
            "max": self._max if self._max != float("-inf") else None,
        }
        return msgspec.msgpack.encode(payload)

    @classmethod
    def from_bytes(cls, data: bytes, config: SLOConfig) -> "TDigest":
        """Deserialize from SWIM gossip transfer."""
        parsed = msgspec.msgpack.decode(data)
        digest = cls(_config=config)
        digest._centroids = [
            Centroid(mean=mean, weight=weight)
            for mean, weight in parsed.get("centroids", [])
        ]
        digest._total_weight = parsed.get("total_weight", 0.0)
        digest._min = (
            parsed.get("min") if parsed.get("min") is not None else float("inf")
        )
        digest._max = (
            parsed.get("max") if parsed.get("max") is not None else float("-inf")
        )
        return digest
