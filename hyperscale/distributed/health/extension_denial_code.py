"""``ExtensionDenialCode`` -- pickled under the namespace
``hyperscale.distributed.health.extension_decision`` (see that module)."""

from __future__ import annotations

from enum import Enum


class ExtensionDenialCode(Enum):
    """Structured denial codes for AD-26 H5 multi-witness decisions.

    Each code is observable end-to-end: emitted in the
    ``HealthcheckExtensionResponse.denial_reason_code`` wire field
    (Phase H5 wire-format extension), recorded in the
    ``ExtensionDecisionEvent`` ledger entry (H7), and consumed by
    the H8 outcome-feedback loop for Bayesian alpha tuning.
    """

    NONE = "none"
    """Granted — no denial."""

    MAX_EXHAUSTED = "max_exhausted"
    """``extension_count >= max_extensions``. AD-26 hard cap."""

    COUNTER_REGRESSION = "counter_regression"
    """``WorkflowProgressSnapshot.all_non_regressed`` failed —
    counters went backward. Most-likely worker bug or attempted
    gaming."""

    NO_ADVANCEMENT = "no_advancement"
    """``WorkflowProgressSnapshot.any_advanced`` failed — every
    dimension stalled. Truly stuck workflow; AD-26 progress
    requirement violated."""

    THROUGHPUT_REGIME_DOWN = "throughput_regime_down"
    """H6 ThroughputWitness flagged a sustained throughput drop.
    Deny so the workflow's hard timeout can fire and the dispatcher
    can reschedule somewhere healthier."""

    OVERLOADED_STATE = "overloaded_state"
    """Worker reported AD-19 ``health_overload_state == "overloaded"``.
    Adding an extension to an already-overloaded worker delays
    eviction; deny and let drain logic engage."""

    RATE_LIMITED = "rate_limited"
    """Last grant was too recent
    (< ``min_between_extensions_seconds`` ago). Prevents extension
    spam within a single heartbeat interval."""
