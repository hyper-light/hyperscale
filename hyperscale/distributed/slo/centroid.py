from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Centroid:
    """A weighted centroid in the T-Digest."""

    mean: float
    weight: float
