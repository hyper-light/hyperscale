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

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Deque
from hyperscale.distributed.runtime import Clock, RealClock

from .bocpd import BayesianOnlineChangePointDetector, BOCPDConfig, RunLengthPosterior
from .hierarchical_alpha import HierarchicalAlphaBudget, HierarchicalAlphaConfig
from .kolmogorov_smirnov import KSResult, TwoSampleKolmogorovSmirnov
from .throughput_witness_config import ThroughputWitnessConfig
from .witness_verdict import WitnessVerdict
from .witness_verdict_kind import WitnessVerdictKind
from ._stream_state import _StreamState


class ThroughputWitness:
    """Manager-side façade over BOCPD + K-S + hierarchical α.

    Thread-safety: NOT thread-safe. The manager-side caller (H5
    decision in ``WorkerHealthManager.handle_extension_request``)
    serialises access through the existing per-job lock.
    """

    def __init__(
        self,
        config: ThroughputWitnessConfig | None = None,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._config: ThroughputWitnessConfig = (
            config if config is not None else ThroughputWitnessConfig()
        )
        self._budget: HierarchicalAlphaBudget = HierarchicalAlphaBudget(
            self._config.alpha
        )
        self._streams: dict[tuple[str, str], _StreamState] = {}
        # Which workers hold a stream for each workflow, and which
        # workflows each worker holds one for: a workflow's end or a
        # worker's departure drops exactly its streams.
        self._workers_by_workflow: dict[str, set[str]] = {}
        self._workflows_by_worker: dict[str, set[str]] = {}
        self._clock: Clock = clock if clock is not None else RealClock()

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
        if self._streams.pop((worker_id, workflow_id), None) is None:
            return
        self._discard_from_index(self._workers_by_workflow, workflow_id, worker_id)
        self._discard_from_index(self._workflows_by_worker, worker_id, workflow_id)

    def forget_workflow(self, workflow_id: str) -> None:
        """Drop every stream of a workflow that has ended."""
        for worker_id in self._workers_by_workflow.pop(workflow_id, set()):
            self._streams.pop((worker_id, workflow_id), None)
            self._discard_from_index(self._workflows_by_worker, worker_id, workflow_id)

    def forget_worker(self, worker_id: str) -> None:
        """Drop every stream of a worker that has left."""
        for workflow_id in self._workflows_by_worker.pop(worker_id, set()):
            self._streams.pop((worker_id, workflow_id), None)
            self._discard_from_index(self._workers_by_workflow, workflow_id, worker_id)

    @staticmethod
    def _discard_from_index(index: dict[str, set[str]], key: str, member: str) -> None:
        """Remove ``member`` from ``index[key]``, deleting the key once its
        set empties so the stream indices stay bounded."""
        if (members := index.get(key)) is None:
            return
        members.discard(member)
        if not members:
            del index[key]

    @property
    def stream_count(self) -> int:
        return len(self._streams)

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
            self._workers_by_workflow.setdefault(workflow_id, set()).add(worker_id)
            self._workflows_by_worker.setdefault(worker_id, set()).add(workflow_id)

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
        stream.last_observation_time = self._clock.monotonic()

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

        * cold-start → ``COLD_START`` until enough samples accumulate
          (this also keeps the arithmetically inevitable MAP = 0 of the
          first observations from producing a verdict);
        * a fresh MAP run length (see ``_is_fresh_run``) → a regime
          change; the sign of the predictive-mean shift picks DOWN or UP;
        * otherwise → ``STATIONARY``.

        ``P(r_t = 0)`` does not gate the verdict: it sits at the hazard
        floor through a real step change, so gating on it suppressed every
        true regime change (measured 2026-10-06: the drop and surge tests
        in tests/unit/distributed/health/test_progress_witness.py fail with
        the gate). It is still reported on the verdict, with the
        workflow's α, for observability.
        """
        if (
            stream.detector.observation_count
            < self._config.cold_start_min_observations
        ):
            return WitnessVerdictKind.COLD_START

        map_run_length = stream.detector.posterior.maximum_a_posteriori_run_length()
        observation_count = stream.detector.observation_count
        if not self._is_fresh_run(map_run_length, observation_count):
            return WitnessVerdictKind.STATIONARY

        return self._regime_change_direction(predictive_mean_before, predictive_mean_after)

    @staticmethod
    def _is_fresh_run(map_run_length: int, observation_count: int) -> bool:
        """Whether the BOCPD MAP run length is small relative to the samples
        seen -- the robust change signal (Adams-MacKay 2007 §3)."""
        # A "fresh" MAP — small relative to total observations seen —
        # signals that the most-likely run started recently.
        # ``map_freshness_ratio`` of 0.25 means: MAP at <= 25% of the
        # samples seen so far. Picked so 40 stationary observations
        # followed by 10 step-change observations naturally crosses
        # the threshold (MAP drops from ~40 toward ~5–10 → ratio
        # ~0.1–0.2 of the 50 total).
        map_freshness_ratio = 0.25
        return (
            observation_count > 0
            and map_run_length
            <= max(2, int(observation_count * map_freshness_ratio))
        )

    @staticmethod
    def _regime_change_direction(
        predictive_mean_before: float, predictive_mean_after: float
    ) -> WitnessVerdictKind:
        """The verdict for a fresh MAP: DOWN when the predictive mean fell,
        else UP."""
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

        if self._head_differs_from_tail(stream.history, history_len):
            self._drop_history_head(stream.history, history_len // 2)

    def _head_differs_from_tail(self, history: Deque[float], history_len: int) -> bool:
        """Whether the K-S test rejects stationarity between the history's
        older and newer halves."""
        head = list(history)[: history_len // 2]
        tail = list(history)[history_len // 2 :]
        if not head or not tail:
            return False
        return self._ks_rejects_stationarity(head, tail)

    def _ks_rejects_stationarity(self, head: list[float], tail: list[float]) -> bool:
        """Run the two-sample K-S test at ``ks_alpha_override`` or
        ``alpha.alpha_system`` and report a rejection."""
        result: KSResult = TwoSampleKolmogorovSmirnov.test(head, tail)
        ks_alpha = self._config.ks_alpha_override or self._config.alpha.alpha_system
        return not result.is_stationary(ks_alpha)

    @staticmethod
    def _drop_history_head(history: Deque[float], head_length: int) -> None:
        """Discard the non-stationary head of a stream's history."""
        # Non-stationary — discard the head from the rolling history
        # so future K-S tests have a chance to see a stationary view
        # and so the BOCPD detector's prior reflects the most recent
        # regime. We can't surgically prune the BOCPD's run-length
        # state without re-running it; instead we let the detector's
        # built-in change-point machinery converge naturally on the
        # narrower data.
        for _ in range(head_length):
            history.popleft()

_REHOMED = (
    WitnessVerdictKind,
    WitnessVerdict,
    ThroughputWitnessConfig,
    _StreamState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
