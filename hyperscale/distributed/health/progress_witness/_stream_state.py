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
    # Posterior-marginalised predictive mean just before and just after
    # the newest sample: the direction of a confirmed regime change.
    predictive_mean_before: float = 0.0
    predictive_mean_after: float = 0.0
    # The workflow's progress counters at the last sample taken from its
    # progress reports (``ingest_progress``): the next sample is the rate
    # between them and the newer report.
    last_completed_count: int = 0
    last_elapsed_seconds: float = 0.0
