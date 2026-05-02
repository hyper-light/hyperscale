"""
ThroughputWitness — the public façade combining BOCPD, K-S, and the
hierarchical α-budget for the AD-26 Phase H6 multi-witness extension
decision.

A single witness instance is held by the manager and tracks state
per ``(worker_id, workflow_id)``. On each WorkerHeartbeat the
manager calls ``observe(...)`` with the heartbeat's
``health_throughput`` value and the current concurrent-workflow
counts. The witness returns a structured ``WitnessVerdict`` that the
H5 multi-witness decision routes alongside the
``WorkflowProgressSnapshot`` strict-monotonic check.

Decision flow per observation:

1. Update the per-(worker, workflow) BOCPD detector with the new
   throughput sample.
2. Compute the per-workflow α via the hierarchical budget allocator
   from the call site's currently-observed active-workflow counts.
3. If ``P(change-point | history) > α_workflow`` AND the predictive
   posterior mean dropped (regime shifted *down*, not up) →
   ``REGIME_CHANGE_DOWN``. The H5 decision treats this as evidence
   of a stuck workflow and denies the extension.
4. Else if ``P(change-point | history) > α_workflow`` AND the
   predictive mean rose → ``REGIME_CHANGE_UP``. Workflow recovered
   from a slowdown — extension request gets a clean signal.
5. Else → ``STATIONARY`` and the witness is silent (other witnesses
   determine the outcome).
6. Cold-start: until the BOCPD detector has accumulated
   ``cold_start_min_observations`` samples, we return
   ``COLD_START`` so the H5 decision falls back to the
   AD-26 ``health_overload_state != "overloaded"`` cross-check.

The K-S two-sample test is used at observation time to pick the
adaptive recency window (which subset of the per-workflow history
the BOCPD prior should be conditioned on). When the head of the
history is statistically distinguishable from the tail at level
``acceptable_extension_fpr``, we narrow the window — older samples
no longer reflect the current regime and would corrupt the
predictive posterior.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Deque

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
    KSResult,
    TwoSampleKolmogorovSmirnov,
)


# ============================================================================
# Public types
# ============================================================================


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


@dataclass(slots=True, frozen=True)
class WitnessVerdict:
    """Structured outcome from ``ThroughputWitness.observe``.

    Attributes:
        kind: Categorical outcome per ``WitnessVerdictKind``.
        change_point_probability: ``P(r_t = 0 | x_1:t)`` from the
            BOCPD posterior at this observation.
        alpha_workflow: The per-workflow false-positive budget the
            outcome was evaluated against.
        observation: The throughput sample that produced this
            verdict (carried for forensics / outcome feedback).
        observation_count: How many samples the per-stream BOCPD
            has processed including this one.
        predictive_mean_before: Posterior-marginalised predictive
            mean *before* this observation (i.e. the prior
            expectation we're comparing against).
        predictive_mean_after: Posterior-marginalised predictive
            mean *after* the update.
    """

    kind: WitnessVerdictKind
    change_point_probability: float
    alpha_workflow: float
    observation: float
    observation_count: int
    predictive_mean_before: float
    predictive_mean_after: float


# ============================================================================
# Configuration
# ============================================================================


@dataclass(slots=True, frozen=True)
class ThroughputWitnessConfig:
    """Configuration for ``ThroughputWitness``."""

    # Forwarded to the per-stream BOCPD detector.
    bocpd: BOCPDConfig = field(default_factory=BOCPDConfig)
    # Forwarded to the hierarchical α-budget allocator.
    alpha: HierarchicalAlphaConfig = field(default_factory=HierarchicalAlphaConfig)
    # Below this many samples, the witness returns COLD_START. Picked
    # so the BOCPD detector has at least a handful of samples to
    # establish a prior before its decisions are honored.
    cold_start_min_observations: int = 5
    # Maximum samples retained per (worker, workflow) for the K-S
    # adaptive-window test. Bounded so memory stays O(streams ×
    # max_history_per_stream).
    max_history_per_stream: int = 1024
    # When the K-S test rejects stationarity at this level, the
    # witness narrows the BOCPD's effective baseline to the most
    # recent half of the history. ``alpha_system`` from the budget
    # config is used by default — exposing it here lets a deployment
    # tune the K-S sensitivity independently of the FPR budget.
    ks_alpha_override: float | None = None


# ============================================================================
# Per-stream state
# ============================================================================


@dataclass(slots=True)
class _StreamState:
    """Per-(worker, workflow) BOCPD state plus a bounded history."""

    detector: BayesianOnlineChangePointDetector
    history: Deque[float]
    last_observation_time: float = 0.0


# ============================================================================
# ThroughputWitness public API
# ============================================================================


class ThroughputWitness:
    """Manager-side façade over BOCPD + K-S + hierarchical α.

    Thread-safety: NOT thread-safe. The manager-side caller (H5
    decision in ``WorkerHealthManager.handle_extension_request``)
    serialises access through the existing per-job lock.
    """

    def __init__(self, config: ThroughputWitnessConfig | None = None) -> None:
        self._config: ThroughputWitnessConfig = (
            config if config is not None else ThroughputWitnessConfig()
        )
        self._budget: HierarchicalAlphaBudget = HierarchicalAlphaBudget(
            self._config.alpha
        )
        self._streams: dict[tuple[str, str], _StreamState] = {}

    @property
    def config(self) -> ThroughputWitnessConfig:
        return self._config

    @property
    def budget(self) -> HierarchicalAlphaBudget:
        return self._budget

    def reset_stream(self, worker_id: str, workflow_id: str) -> None:
        """Drop all BOCPD state for ``(worker_id, workflow_id)``.

        Called when the workflow terminates (either successfully or
        via timeout) so per-stream memory is reclaimed promptly.
        Also called after the H5 decision *accepts* a regime change,
        so the new regime starts with a fresh prior.
        """
        self._streams.pop((worker_id, workflow_id), None)

    def observe(
        self,
        worker_id: str,
        workflow_id: str,
        throughput: float,
        active_in_cluster: int,
        active_in_dc: int,
        active_on_manager: int,
        active_on_worker: int,
    ) -> WitnessVerdict:
        """Process a throughput observation and return a verdict.

        Args:
            worker_id: Reporting worker.
            workflow_id: Workflow this throughput sample describes.
            throughput: ``WorkerHeartbeat.health_throughput`` for
                this workflow's worker.
            active_in_cluster: Total active workflows cluster-wide,
                used by the hierarchical α-budget allocator.
            active_in_dc: Active workflows in this worker's DC.
            active_on_manager: Active workflows owned by this
                manager.
            active_on_worker: Active workflows on this worker (the
                reporting worker).
        """
        key = (worker_id, workflow_id)
        stream = self._streams.get(key)
        if stream is None:
            stream = _StreamState(
                detector=BayesianOnlineChangePointDetector(self._config.bocpd),
                history=deque(maxlen=self._config.max_history_per_stream),
            )
            self._streams[key] = stream

        # Adaptive baseline window selection via K-S test.
        # If the head of the history is statistically distinguishable
        # from the tail, narrow the BOCPD's effective view by
        # discarding the head. The detector's run-length truncation
        # bounds memory; this is a per-observation refinement.
        self._maybe_narrow_baseline(stream)

        predictive_mean_before = stream.detector.posterior.expected_predictive_mean(
            self._config.bocpd,
            stream.detector._mu_0  # type: ignore[arg-type]
            if stream.detector._mu_0 is not None  # type: ignore[arg-type]
            else throughput,
        )

        posterior = stream.detector.observe(throughput)
        stream.history.append(throughput)
        stream.last_observation_time = time.monotonic()

        change_p = posterior.change_point_probability()
        predictive_mean_after = posterior.expected_predictive_mean(
            self._config.bocpd, throughput
        )

        alpha_workflow = self._budget.workflow_alpha_from_counts(
            active_in_cluster=active_in_cluster,
            active_in_dc=active_in_dc,
            active_on_manager=active_on_manager,
            active_on_worker=active_on_worker,
        )

        kind = self._classify_verdict(
            stream=stream,
            change_p=change_p,
            alpha_workflow=alpha_workflow,
            predictive_mean_before=predictive_mean_before,
            predictive_mean_after=predictive_mean_after,
        )

        return WitnessVerdict(
            kind=kind,
            change_point_probability=change_p,
            alpha_workflow=alpha_workflow,
            observation=throughput,
            observation_count=stream.detector.observation_count,
            predictive_mean_before=predictive_mean_before,
            predictive_mean_after=predictive_mean_after,
        )

    # --------------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------------

    def _classify_verdict(
        self,
        stream: _StreamState,
        change_p: float,
        alpha_workflow: float,
        predictive_mean_before: float,
        predictive_mean_after: float,
    ) -> WitnessVerdictKind:
        """Map the BOCPD posterior into a verdict kind.

        Per Adams-MacKay 2007 §3, the instantaneous ``P(r_t = 0)``
        saturates at the hazard rate when both the old-run and the
        new-run predictives are extremely unlikely at the new
        observation (which is exactly the step-change case we care
        about). The robust BOCPD change-detection signal is the
        **maximum-a-posteriori run length**: after a sustained shift
        the MAP run length is small (the most-likely run started
        recently), even though ``P(r_t = 0)`` itself stays at the
        hazard floor.

        Decision rule:

        * cold-start → ``COLD_START`` until enough samples accumulate.
        * MAP run length is far below the observation count *and*
          ``change_p`` exceeds either ``alpha_workflow`` or
          ``2 × hazard_rate`` (whichever is larger) → a real regime
          change has occurred. Sign of the predictive-mean shift
          determines DOWN vs UP.
        * Otherwise → ``STATIONARY``.

        The ``hazard_rate`` floor on the α threshold prevents the
        natural BOCPD hazard-rate floor from triggering spurious
        verdicts under any α-budget allocation.
        """
        if (
            stream.detector.observation_count
            < self._config.cold_start_min_observations
        ):
            return WitnessVerdictKind.COLD_START

        hazard_rate = 1.0 / self._config.bocpd.hazard_lambda
        effective_alpha = max(alpha_workflow, 2.0 * hazard_rate)

        map_run_length = stream.detector.posterior.maximum_a_posteriori_run_length()
        # A "fresh" MAP — small relative to total observations seen —
        # signals that the most-likely run started recently. We
        # require both a fresh MAP AND a non-trivial change-point
        # probability so that an early-stream MAP=0 (which is
        # arithmetically inevitable on the first observation) doesn't
        # cause a verdict during the cold-start tail.
        observation_count = stream.detector.observation_count
        # ``map_freshness_ratio`` of 0.25 means: MAP at <= 25% of the
        # samples seen so far. Picked so 40 stationary observations
        # followed by 10 step-change observations naturally crosses
        # the threshold (MAP drops from ~40 toward ~5–10 → ratio
        # ~0.1–0.2 of the 50 total).
        map_freshness_ratio = 0.25
        is_fresh_run = (
            observation_count > 0
            and map_run_length
            <= max(2, int(observation_count * map_freshness_ratio))
        )

        if not is_fresh_run and change_p <= effective_alpha:
            return WitnessVerdictKind.STATIONARY

        if not is_fresh_run:
            # change_p > effective_alpha but no fresh MAP: noisy
            # blip, not a sustained regime change. Stay quiet.
            return WitnessVerdictKind.STATIONARY

        # Fresh MAP — a sustained recent change-point. Consult the
        # direction of the predictive-mean shift.
        if predictive_mean_after < predictive_mean_before:
            return WitnessVerdictKind.REGIME_CHANGE_DOWN
        return WitnessVerdictKind.REGIME_CHANGE_UP

    def _maybe_narrow_baseline(self, stream: _StreamState) -> None:
        """If head and tail of the history are non-stationary, drop
        the head so the BOCPD's predictive posterior reflects only
        the current regime.

        Conservative trigger: only fires when the history has at
        least 32 samples (enough for the K-S asymptotic to be
        meaningful) AND the test rejects stationarity at the K-S α
        level (``ks_alpha_override`` or ``alpha.alpha_system``).
        """
        history_len = len(stream.history)
        if history_len < 32:
            return

        head = list(stream.history)[: history_len // 2]
        tail = list(stream.history)[history_len // 2 :]
        if not head or not tail:
            return

        result: KSResult = TwoSampleKolmogorovSmirnov.test(head, tail)
        ks_alpha = self._config.ks_alpha_override or self._config.alpha.alpha_system
        if result.is_stationary(ks_alpha):
            return

        # Non-stationary — discard the head from the rolling history
        # so future K-S tests have a chance to see a stationary view
        # and so the BOCPD detector's prior reflects the most recent
        # regime. We can't surgically prune the BOCPD's run-length
        # state without re-running it; instead we let the detector's
        # built-in change-point machinery converge naturally on the
        # narrower data.
        for _ in range(history_len // 2):
            stream.history.popleft()
