"""
ThroughputWitness — the public façade combining BOCPD, K-S, and the
hierarchical α-budget for the AD-26 Phase H6 multi-witness extension
decision.

A single witness instance is held by the manager and tracks state
per ``(worker_id, workflow_id)``. The manager feeds it each
workflow's progress reports (``ingest_progress``); the H5 decision evaluator asks
for a verdict at the per-test level ``alpha_workflow`` (``assess``).
The witness returns a structured ``WitnessVerdict`` that the H5
multi-witness decision routes alongside the
``WorkflowProgressSnapshot`` strict-monotonic check.

Decision flow per observation:

1. ``ingest_progress``: the manager's progress path feeds each
   workflow's own rate -- ``Δcompleted_count / Δelapsed_seconds`` between
   its ``WorkflowProgress`` reports, one sample per
   ``minimum_sample_interval_seconds`` -- into its per-(worker, workflow)
   BOCPD detector and bounded history (``ingest`` takes a rate directly).
2. ``assess`` (at an extension decision) -- cold-start: until the detector has accumulated
   ``cold_start_min_observations`` samples → ``COLD_START`` (the H5
   decision falls back to its other witnesses).
3. A stale MAP run length (the most-likely regime is old) →
   ``STATIONARY``.
4. A fresh MAP run length PROPOSES a change point; the two-sample
   Kolmogorov-Smirnov test of the history before it against the
   history after it CONFIRMS it at the caller's per-test level
   ``alpha_workflow``. Rejected stationarity → ``REGIME_CHANGE_DOWN``
   when the predictive mean fell (the H5 decision denies), else
   ``REGIME_CHANGE_UP``; otherwise → ``STATIONARY``.

``alpha_workflow`` is therefore the per-test false-deny rate the H6
hierarchical budget allocates: the H5 evaluator composes it with the
H8 outcome posterior of the workflow's class
(``HierarchicalAlphaTuner.alpha_budget``) before each call, so the
learned outcomes move how much evidence a deny needs.

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

# Smallest effective sample size ``n1·n2 / (n1 + n2)`` at which the
# asymptotic two-sample Kolmogorov p-value is accurate: Numerical
# Recipes 3rd ed. §14.3.3 (Stephens 1970) -- "already quite good for
# N_e ≥ 4".
_MINIMUM_KOLMOGOROV_EFFECTIVE_SAMPLE_SIZE: float = 4.0

# A MAP run length at or under this share of the samples the detector
# models reads as a regime that started recently. Picked so 40
# stationary observations followed by 10 step-change observations
# cross it (MAP drops from ~40 toward ~5-10, 0.1-0.2 of the 50 total).
# It also fixes the K-S split: a fresh run's post-change segment is at
# most this share of the window, so the pre-change one is at least
# ``(1 - ratio) / ratio`` times as long (``witness_feed_derivation``).
MAP_FRESHNESS_RATIO: float = 0.25


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

    def ingest(self, worker_id: str, workflow_id: str, throughput: float) -> None:
        """Feed one rate sample of ``workflow_id`` on ``worker_id`` -- the
        workflow's ``WorkflowProgress.rate_per_second`` -- into its stream.

        A sample arriving sooner than ``minimum_sample_interval_seconds``
        after the stream's last one is not taken: the interval is the
        sampling rate the K-S confirmation needs (see
        ``witness_feed_derivation``), and every sample taken costs one
        O(``run_length_max``) BOCPD update.
        """
        now = self._clock.monotonic()
        stream = self._streams.get((worker_id, workflow_id))
        if stream is None:
            stream = self._open_stream(worker_id, workflow_id)
        elif now - stream.last_observation_time < self._config.minimum_sample_interval_seconds:
            return
        self._update_stream(stream, throughput, now)

    def ingest_progress(
        self, worker_id: str, workflow_id: str, completed_count: int, elapsed_seconds: float
    ) -> None:
        """Feed one ``WorkflowProgress`` report of ``workflow_id`` on
        ``worker_id``: its cumulative ``completed_count`` and the worker's
        ``elapsed_seconds`` for the run.

        The sample is the workflow's rate over the span since the last
        sample taken, ``Δcompleted / Δelapsed``, timed on the worker's own
        clock. The report's ``rate_per_second`` is not used: it is the
        run's cumulative mean (``completed_count / elapsed_seconds``,
        nodes/worker/workflow_executor.py ``_completion_rate``), which
        turns a step collapse into a slow hyperbolic decay and whose
        successive values are far from independent -- the K-S test
        assumes independent samples. A span shorter than
        ``minimum_sample_interval_seconds`` is not sampled yet; the
        first report of a stream only sets its baseline."""
        if (stream := self._streams.get((worker_id, workflow_id))) is None:
            stream = self._open_stream(worker_id, workflow_id)
            stream.last_completed_count = completed_count
            stream.last_elapsed_seconds = elapsed_seconds
            return
        self._sample_progress_span(stream, completed_count, elapsed_seconds)

    def _sample_progress_span(self, stream: _StreamState, completed_count: int, elapsed_seconds: float) -> None:
        """Take the span's rate as a sample once it is long enough. A run
        whose counters went backwards (re-dispatched under the same id)
        is re-baselined rather than sampled."""
        if elapsed_seconds < stream.last_elapsed_seconds or completed_count < stream.last_completed_count:
            stream.last_completed_count = completed_count
            stream.last_elapsed_seconds = elapsed_seconds
            return
        self._sample_if_span_due(stream, completed_count, elapsed_seconds)

    def _sample_if_span_due(self, stream: _StreamState, completed_count: int, elapsed_seconds: float) -> None:
        """One BOCPD sample of the span's rate, when the span is positive
        and at least the sampling interval."""
        span_seconds = elapsed_seconds - stream.last_elapsed_seconds
        if span_seconds <= 0.0 or span_seconds < self._config.minimum_sample_interval_seconds:
            return
        span_rate = (completed_count - stream.last_completed_count) / span_seconds
        stream.last_completed_count = completed_count
        stream.last_elapsed_seconds = elapsed_seconds
        self._update_stream(stream, span_rate, self._clock.monotonic())

    def _update_stream(self, stream: _StreamState, throughput: float, now: float) -> None:
        """One BOCPD update of ``stream`` with ``throughput`` taken at
        ``now``, recording the predictive mean on either side of it."""
        stream.predictive_mean_before = stream.detector.posterior.expected_predictive_mean(
            self._config.bocpd,
            stream.detector._mu_0  # type: ignore[arg-type]
            if stream.detector._mu_0 is not None  # type: ignore[arg-type]
            else throughput,
        )
        posterior = stream.detector.observe(throughput)
        stream.history.append(throughput)
        stream.last_observation_time = now
        stream.predictive_mean_after = posterior.expected_predictive_mean(
            self._config.bocpd, throughput
        )

    def assess(self, worker_id: str, workflow_id: str, alpha_workflow: float) -> WitnessVerdict:
        """The witness's verdict on the stream as it stands, its K-S
        confirmation run at ``alpha_workflow`` -- the H6 hierarchical α
        composed with the workflow class's H8 outcome posterior by the
        caller. A workflow with no stream yet is ``COLD_START``."""
        if (stream := self._streams.get((worker_id, workflow_id))) is None:
            return WitnessVerdict(
                kind=WitnessVerdictKind.COLD_START,
                change_point_probability=0.0,
                alpha_workflow=alpha_workflow,
                observation=0.0,
                observation_count=0,
                predictive_mean_before=0.0,
                predictive_mean_after=0.0,
            )
        return WitnessVerdict(
            kind=self._classify_verdict(
                stream=stream,
                predictive_mean_before=stream.predictive_mean_before,
                predictive_mean_after=stream.predictive_mean_after,
                alpha_workflow=alpha_workflow,
            ),
            change_point_probability=stream.detector.posterior.change_point_probability(),
            alpha_workflow=alpha_workflow,
            observation=stream.history[-1],
            observation_count=stream.detector.observation_count,
            predictive_mean_before=stream.predictive_mean_before,
            predictive_mean_after=stream.predictive_mean_after,
        )

    def observe(
        self,
        worker_id: str,
        workflow_id: str,
        throughput: float,
        alpha_workflow: float,
    ) -> WitnessVerdict:
        """``ingest`` one sample, then ``assess`` the stream at
        ``alpha_workflow``."""
        self.ingest(worker_id, workflow_id, throughput)
        return self.assess(worker_id, workflow_id, alpha_workflow)

    def _open_stream(self, worker_id: str, workflow_id: str) -> _StreamState:
        """A new stream for ``(worker_id, workflow_id)``, indexed for its
        workflow's and its worker's cleanup. Its history holds the
        detector's window, ``run_length_max`` samples: the pre-change
        segment of a K-S confirmation never reaches past the regime the
        detector models."""
        stream = _StreamState(
            detector=BayesianOnlineChangePointDetector(self._config.bocpd),
            history=deque(maxlen=self._config.bocpd.run_length_max),
        )
        self._streams[(worker_id, workflow_id)] = stream
        self._workers_by_workflow.setdefault(workflow_id, set()).add(worker_id)
        self._workflows_by_worker.setdefault(worker_id, set()).add(workflow_id)
        return stream

    # --------------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------------

    def _classify_verdict(
        self,
        stream: _StreamState,
        predictive_mean_before: float,
        predictive_mean_after: float,
        alpha_workflow: float,
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
        * a stale MAP run length → ``STATIONARY``;
        * a fresh MAP run length (see ``_is_fresh_run``) proposes a
          change point, which ``_confirmed_regime_change`` tests at
          ``alpha_workflow``.

        ``P(r_t = 0)`` does not gate the verdict: it sits at the hazard
        floor through a real step change, so gating on it suppressed every
        true regime change (measured 2026-10-06: the drop and surge tests
        in tests/unit/distributed/health/test_progress_witness.py fail with
        the gate). It is still reported on the verdict for observability.
        """
        if (
            stream.detector.observation_count
            < self._config.cold_start_min_observations
        ):
            return WitnessVerdictKind.COLD_START

        map_run_length = stream.detector.posterior.maximum_a_posteriori_run_length()
        modelled_samples = min(stream.detector.observation_count, self._config.bocpd.run_length_max)
        if not self._is_fresh_run(map_run_length, modelled_samples):
            return WitnessVerdictKind.STATIONARY

        return self._confirmed_regime_change(
            stream.history,
            map_run_length,
            alpha_workflow,
            predictive_mean_before,
            predictive_mean_after,
        )

    @classmethod
    def _confirmed_regime_change(
        cls,
        history: Deque[float],
        map_run_length: int,
        alpha_workflow: float,
        predictive_mean_before: float,
        predictive_mean_after: float,
    ) -> WitnessVerdictKind:
        """The regime change a fresh MAP run proposes, when the K-S test
        confirms it at ``alpha_workflow``; else ``STATIONARY``."""
        if not cls._kolmogorov_smirnov_confirms_change_point(history, map_run_length, alpha_workflow):
            return WitnessVerdictKind.STATIONARY
        return cls._regime_change_direction(predictive_mean_before, predictive_mean_after)

    @staticmethod
    def _kolmogorov_smirnov_confirms_change_point(
        history: Deque[float], map_run_length: int, alpha_workflow: float
    ) -> bool:
        """Whether the two-sample K-S test rejects "same distribution" for
        the history before vs after the change point the MAP run length
        proposes, at level ``alpha_workflow``.

        The detector's run-length index ``r`` covers ``r + 1`` samples
        (index 0 holds the newest sample alone), so the post-change
        segment is the newest ``map_run_length + 1`` samples. The test's
        p-value is the asymptotic Kolmogorov one (Stephens' correction,
        Numerical Recipes 3rd ed. §14.3.3), which NR rates "already quite
        good for N_e ≥ 4", ``N_e = n_pre·n_post / (n_pre + n_post)``;
        below that — including no pre-change sample at all — the change
        point is left unconfirmed rather than tested on an inaccurate
        p-value.

        The change point is chosen from the same data, so the p-value
        is a lower bound on the true one (a selected split). The
        confirmation can only remove BOCPD denials, never add them.
        """
        post_change_length = map_run_length + 1
        pre_change_length = len(history) - post_change_length
        if (
            pre_change_length * post_change_length
            < _MINIMUM_KOLMOGOROV_EFFECTIVE_SAMPLE_SIZE * (pre_change_length + post_change_length)
        ):
            return False
        samples = list(history)
        result: KSResult = TwoSampleKolmogorovSmirnov.test(
            samples[:pre_change_length], samples[pre_change_length:]
        )
        return not result.is_stationary(alpha_workflow)

    @staticmethod
    def _is_fresh_run(map_run_length: int, modelled_samples: int) -> bool:
        """Whether the BOCPD MAP run length is small relative to the samples
        the detector models -- the robust change signal (Adams-MacKay 2007
        §3). ``modelled_samples`` is the samples seen, capped at
        ``run_length_max``: past the cap a stationary stream's MAP sits at
        ``run_length_max - 1``, which must still read as old."""
        return (
            modelled_samples > 0
            and map_run_length
            <= max(2, int(modelled_samples * MAP_FRESHNESS_RATIO))
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


_REHOMED = (
    WitnessVerdictKind,
    WitnessVerdict,
    ThroughputWitnessConfig,
    _StreamState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
