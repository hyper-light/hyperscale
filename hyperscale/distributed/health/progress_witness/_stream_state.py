"""``_StreamState`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.throughput_witness`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Deque

from .bocpd import BayesianOnlineChangePointDetector


@dataclass(slots=True)
class _StreamState:
    """Per-(worker, workflow) BOCPD state plus a bounded history."""

    detector: BayesianOnlineChangePointDetector
    history: Deque[float]
    last_observation_time: float = 0.0
