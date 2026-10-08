"""``WitnessVerdictKind`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.throughput_witness`` (see that module)."""

from __future__ import annotations

from enum import Enum, auto


class WitnessVerdictKind(Enum):
    """Possible witness outcomes for a single observation."""

    COLD_START = auto()
    """Detector hasn't seen enough samples yet — defer to other
    witnesses. Default during the first ~5–10 heartbeats per
    (worker, workflow) pair."""

    STATIONARY = auto()
    """No change-point detected. Throughput consistent with the
    learned baseline distribution."""

    REGIME_CHANGE_DOWN = auto()
    """Change-point detected AND predictive mean dropped. Strong
    evidence that the workflow's throughput regime has degraded —
    H5 should deny the extension."""

    REGIME_CHANGE_UP = auto()
    """Change-point detected AND predictive mean rose. Workflow is
    recovering from a slowdown — H5 should treat as a healthy
    signal."""
