"""
Progress witness — adaptive change-point detection for AD-26 Phase H6.

Detects regime shifts in per-(worker, workflow) throughput streams
without magic constants. Combines:

* ``BayesianOnlineChangePointDetector`` — Adams-MacKay BOCPD with a
  Normal-inverse-Gamma conjugate prior over Gaussian observations.
  Posterior predictive Student-t. Returns ``P(change-point | history)``
  on every observation.
* ``TwoSampleKolmogorovSmirnov`` — K-S two-sample stationarity test.
  Used to pick the largest TimeWindowedTDigest scale at which the
  throughput distribution is currently stationary, giving an
  adaptive baseline window without a hardcoded length.
* ``HierarchicalAlphaBudget`` — Benjamini-Hochberg FDR splitter from
  the cluster-wide ``HYPERSCALE_EXTENSION_FPR_BUDGET`` down to
  per-workflow α. The active concurrent-workflow count is the only
  scale parameter; everything else is derived.
* ``ThroughputWitness`` — public façade composing the three. Takes
  raw throughput samples, returns structured ``WitnessVerdict``
  values that the H5 multi-witness decision evaluates against.

Per the architecture conversation, the only deployment-policy inputs
are:

1. ``acceptable_extension_fpr`` — cluster-wide tolerated false-deny
   rate. Default 0.01 (1%) per the production-fleet rationale.
2. ``velocity_history_autocorrelation_widen_threshold`` — when to
   widen the snapshot history window if velocity autocorrelation
   indicates burst-then-coast patterns. Default 0.7 (standard
   significance threshold).

Everything else (windows, thresholds, sample counts, critical values)
is derived from observed data via Bayesian inference, K-S
stationarity testing, and outcome-driven Bayesian feedback.
"""

from .bocpd import (
    BayesianOnlineChangePointDetector,
    BOCPDConfig,
    RunLengthPosterior,
)
from .hierarchical_alpha import (
    HierarchicalAlphaBudget,
    HierarchicalAlphaConfig,
)
from .kolmogorov_smirnov import (
    TwoSampleKolmogorovSmirnov,
    KSResult,
)
from .throughput_witness import (
    ThroughputWitness,
    ThroughputWitnessConfig,
    WitnessVerdict,
    WitnessVerdictKind,
)

__all__ = [
    "BayesianOnlineChangePointDetector",
    "BOCPDConfig",
    "RunLengthPosterior",
    "HierarchicalAlphaBudget",
    "HierarchicalAlphaConfig",
    "TwoSampleKolmogorovSmirnov",
    "KSResult",
    "ThroughputWitness",
    "ThroughputWitnessConfig",
    "WitnessVerdict",
    "WitnessVerdictKind",
]
