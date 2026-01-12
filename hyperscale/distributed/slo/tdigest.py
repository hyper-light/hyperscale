from __future__ import annotations

from dataclasses import dataclass, field

import msgspec
import numpy as np

from .centroid import Centroid
from .slo_config import SLOConfig


@dataclass(slots=True)
class TDigest:
    """T-Digest for streaming quantile estimation."""

    _config: SLOConfig = field(default_factory=SLOConfig.from_env)
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

        new_centroids: list[Centroid] = []
        current_mean, current_weight = points[0]
        cumulative_weight = current_weight

        for mean, weight in points[1:]:
            quantile = cumulative_weight / total_weight
            limit = self._k_inverse(self._k(quantile) + 1.0) - quantile
            max_weight = total_weight * limit

            if current_weight + weight <= max_weight:
                new_weight = current_weight + weight
                current_mean = (
                    current_mean * current_weight + mean * weight
                ) / new_weight
                current_weight = new_weight
            else:
                new_centroids.append(Centroid(current_mean, current_weight))
                current_mean = mean
                current_weight = weight

            cumulative_weight += weight

        new_centroids.append(Centroid(current_mean, current_weight))
        self._centroids = new_centroids
        self._unmerged.clear()
        self._total_weight = total_weight

    def _k(self, quantile: float) -> float:
        """Scaling function k(q) = δ/2 * (arcsin(2q-1)/π + 0.5)."""
        return (self.delta / 2.0) * (np.arcsin(2.0 * quantile - 1.0) / np.pi + 0.5)

    def _k_inverse(self, scaled: float) -> float:
        """Inverse scaling function."""
        return 0.5 * (np.sin((scaled / (self.delta / 2.0) - 0.5) * np.pi) + 1.0)

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

        target_weight = quantile * self._total_weight
        cumulative_weight = 0.0

        for index, centroid in enumerate(self._centroids):
            if cumulative_weight + centroid.weight >= target_weight:
                if index == 0:
                    weight_after = cumulative_weight + centroid.weight / 2.0
                    if target_weight <= weight_after:
                        ratio = target_weight / max(weight_after, 1e-10)
                        return self._min + ratio * (centroid.mean - self._min)

                previous_centroid = self._centroids[index - 1] if index > 0 else None
                if previous_centroid is not None:
                    midpoint_previous = (
                        cumulative_weight - previous_centroid.weight / 2.0
                    )
                    midpoint_current = cumulative_weight + centroid.weight / 2.0
                    ratio = (target_weight - midpoint_previous) / max(
                        midpoint_current - midpoint_previous, 1e-10
                    )
                    return previous_centroid.mean + ratio * (
                        centroid.mean - previous_centroid.mean
                    )

                return centroid.mean

            cumulative_weight += centroid.weight

        return self._max

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
    def from_bytes(cls, data: bytes, config: SLOConfig | None = None) -> "TDigest":
        """Deserialize from SWIM gossip transfer."""
        parsed = msgspec.msgpack.decode(data)
        digest = cls(_config=config or SLOConfig.from_env())
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
