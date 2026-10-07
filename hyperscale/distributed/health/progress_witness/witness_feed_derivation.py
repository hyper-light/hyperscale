"""How often the AD-26 H6 throughput witness samples a workflow's rate, and
how long a run its BOCPD detector models -- both derived from what the K-S
confirmation needs, never picked.

The manager feeds every in-flight workflow's ``WorkflowProgress.rate_per_second``
to the witness (one BOCPD update per sample, O(``run_length_max``) each). The
worker sends progress every ``WORKER_PROGRESS_FLUSH_INTERVAL`` (50 ms): taking
every report would cost 20 updates per workflow-second. The decision only needs
enough samples for the K-S test to confirm a collapse at the α floor between
two extension requests, so:

* ``confirmation_sample_count(alpha_floor)``: the smallest post-change
  segment ``n`` whose complete separation (K-S ``D = 1``) is significant at
  the floor. A fresh run's post-change segment is at most
  ``MAP_FRESHNESS_RATIO`` of the window, so the pre-change one is at least
  ``(1 - r)/r`` times longer and ``N_e = n·(1 - r)``; ``N_e`` must also reach
  the asymptotic p-value's accuracy bound (4). At the default floor 1e-5:
  ``N_e`` >= 5.33 -> ``n`` = 8.
* ``sample_interval_seconds``: those ``n`` samples within one extension
  trigger poll (``HYPERSCALE_EXTENSION_TRIGGER_INTERVAL``, 5 s), the shortest
  gap between two requests: 5 s / 8 = 0.625 s.
* ``run_length_cap``: the fresh-run window (``MAP_FRESHNESS_RATIO`` of the
  cap) spans one base extension deadline (``EXTENSION_BASE_DEADLINE``, 30 s,
  the longest a granted window runs), so a collapse anywhere in it is still
  "recent" at the next request: 30 s / 0.625 s / 0.25 = 192 run lengths.

Cost (microbenchmark 2026-10-06, Apple M-series, CPython 3.14): one BOCPD
update at ``run_length_max`` = 192 takes ~200 us (~1.0 us per run length;
925 us at the old cap of 1000). At 1.6 samples/s that is ~0.33 ms per
in-flight workflow per second -- 0.033% of the event loop per workflow, or
~16 us amortised over the 20 progress reports that workflow sends each
second. Taking every report at the old cap would have cost 18.5 ms per
workflow-second (1.85%), i.e. a saturated loop at ~54 in-flight workflows.
"""

from __future__ import annotations

import math

from .bocpd import BOCPDConfig
from .hierarchical_alpha import HierarchicalAlphaConfig
from .throughput_witness import MAP_FRESHNESS_RATIO
from .throughput_witness_config import ThroughputWitnessConfig
from .throughput_witness import _MINIMUM_KOLMOGOROV_EFFECTIVE_SAMPLE_SIZE
from .two_sample_kolmogorov_smirnov import _kolmogorov_survival


def confirmation_sample_count(alpha_floor: float) -> int:
    """The smallest post-change segment whose complete separation the K-S
    confirmation accepts at ``alpha_floor`` (see the module docstring)."""
    if alpha_floor < 0.0:
        raise ValueError(f"alpha_floor must be non-negative, got {alpha_floor}")
    return _smallest_confirming_segment(alpha_floor)


def _smallest_confirming_segment(alpha_floor: float) -> int:
    """Scan segment sizes upward; ``_kolmogorov_survival`` is 0 once its
    argument reaches 6, so the scan ends for every ``alpha_floor >= 0``."""
    post_change_samples = 1
    while (
        effective_samples := post_change_samples * (1.0 - MAP_FRESHNESS_RATIO)
    ) < _MINIMUM_KOLMOGOROV_EFFECTIVE_SAMPLE_SIZE or _kolmogorov_survival(
        math.sqrt(effective_samples) + 0.12 + 0.11 / math.sqrt(effective_samples)
    ) > alpha_floor:
        post_change_samples += 1
    return post_change_samples


def sample_interval_seconds(trigger_interval_seconds: float, alpha_floor: float) -> float:
    """The witness's sampling interval: the confirmation's sample count
    within one extension-trigger poll."""
    return trigger_interval_seconds / confirmation_sample_count(alpha_floor)


def run_length_cap(base_deadline_seconds: float, interval_seconds: float) -> int:
    """The detector's ``run_length_max``: a fresh-run window of one base
    extension deadline at the sampling interval."""
    return math.ceil(base_deadline_seconds / interval_seconds / MAP_FRESHNESS_RATIO)


def throughput_witness_config(
    fpr_budget: float, trigger_interval_seconds: float, base_deadline_seconds: float
) -> ThroughputWitnessConfig:
    """The manager's witness configuration: the H6 α budget
    (``HYPERSCALE_EXTENSION_FPR_BUDGET``), sampled and windowed as derived
    above from the extension trigger poll and base deadline."""
    alpha_config = HierarchicalAlphaConfig(alpha_system=fpr_budget)
    interval_seconds = sample_interval_seconds(trigger_interval_seconds, alpha_config.alpha_workflow_floor)
    return ThroughputWitnessConfig(
        bocpd=BOCPDConfig(run_length_max=run_length_cap(base_deadline_seconds, interval_seconds)),
        alpha=alpha_config,
        minimum_sample_interval_seconds=interval_seconds,
    )
