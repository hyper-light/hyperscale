"""
Per-peer probe-failure-rate tracker for suspicion-timer composition.

This component is the per-peer counterpart to ``LocalHealthMultiplier``.
LHM remains a *global self-health* signal scaling probe timeouts (per
Lifeguard §4.3); this tracker provides a *per-peer measurement
reliability* signal feeding the global suspicion-bracket composition.

Why a separate signal? Suspecting node X must never be delayed by
probe-failures *to X itself* — that is the canonical SWIM/Lifeguard
positive-feedback pathology. Funnelling probe outcomes through a
single global LHM and reading that LHM into X's bracket re-creates the
loop regardless of how the inputs are composed (multiplicative,
log-bounded, or probabilistic-OR all leak through). The fix is
architectural: use a signal that is per-peer by construction so X's
failures inflate only X's bracket — and that bracket is itself
mathematically bounded by the prob-OR composition in
``HierarchicalFailureDetector.suspect_global``.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from collections import deque
from dataclasses import dataclass
from itertools import compress, repeat
from operator import itemgetter, lt, not_, truth
from hyperscale.distributed.runtime import Clock, RealClock

from .peer_probe_reliability_config import PeerProbeReliabilityConfig

_DEFAULT_CLOCK: Clock = RealClock()

NodeAddress = tuple[str, int]


class PeerProbeReliabilityTracker:
    """Per-peer sliding-window tracker of probe success/failure outcomes.

    Each peer maintains a bounded ``deque`` of ``(timestamp, success)``
    samples. The tracker exposes two read paths:

    * ``get_reliability(peer)`` returns the empirical probe-success
      rate ∈ [0, 1] over the configured window, ignoring stale samples.
      Empty histories return 1.0 (assume healthy by default — SWIM's
      own baseline).
    * ``get_unreliability_multiplier(peer)`` returns the inverse of
      reliability, clamped to a finite maximum derived from the
      window size. This form is provided for callers that compose
      multipliers; it is *not* used in the canonical prob-OR
      composition (which prefers the reliability form directly).

    Memory is bounded by ``max_tracked_peers × window_size``. The
    ``evict_stale_peers`` method should be called periodically by the
    host's reconciliation loop to drop peers whose window contains
    only stale samples.
    """

    def __init__(
        self,
        config: PeerProbeReliabilityConfig | None = None,
        *,
        clock: Clock | None = None,
    ) -> None:
        if config is None:
            config = PeerProbeReliabilityConfig()
        self._config = config
        self._windows: dict[NodeAddress, deque[tuple[float, bool]]] = {}
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK

    def record_probe_outcome(
        self,
        peer: NodeAddress,
        success: bool,
        now: float | None = None,
    ) -> None:
        """Record a probe outcome for ``peer``.

        New peers beyond ``max_tracked_peers`` are silently ignored —
        existing peers continue to accumulate samples.
        """
        now = self._resolve_now(now)
        window = self._windows.get(peer)
        if window is None:
            if len(self._windows) >= self._config.max_tracked_peers:
                return
            window = deque(maxlen=self._config.window_size)
            self._windows[peer] = window
        window.append((now, success))

    def get_reliability(
        self,
        peer: NodeAddress,
        now: float | None = None,
    ) -> float:
        """Return the per-peer probe-success rate ∈ [0, 1].

        The estimate uses **window-default smoothing**: positions in
        the configured window for which no live sample has been
        observed (either because the window has not yet filled, or
        because samples have aged past ``sample_ttl_s``) are treated
        as implicit successes. This encodes SWIM's "assume healthy by
        default" posture as a Bayesian-style prior whose strength is
        determined entirely by ``window_size`` — no extra constants,
        no magic numbers — and decays to zero effect once the window
        is full of recent samples.

        Concretely::

            successes_observed = #(non-stale samples in window with success=True)
            non_stale_total    = #(non-stale samples in window)
            implicit_successes = window_size − non_stale_total
            reliability = (successes_observed + implicit_successes) / window_size

        Properties:

        * Empty window → ``reliability = 1.0`` (full prior dominates).
        * Single failure on a fresh peer → ``(window_size − 1) / window_size``
          — a *small* signal that does not crash the bracket on one
          probe miss; consistent with SWIM's tolerance for transient
          loss.
        * Window full of failures → ``0.0`` — strongest possible
          signal, full bracket extension.
        * Intermediate states interpolate cleanly.

        Stale samples are excluded from ``non_stale_total``, which
        means they automatically convert into implicit successes — a
        peer that was unreachable an hour ago and has no recent
        samples drifts back toward the healthy prior, exactly as
        intended.
        """
        window = self._windows.get(peer)
        window_size = self._config.window_size
        if not window:
            return 1.0
        now = self._resolve_now(now)
        cutoff = now - self._config.sample_ttl_s
        # A sample is live unless it is older than the cutoff.
        live_flags = list(map(not_, map(lt, map(itemgetter(0), window), repeat(cutoff))))
        non_stale_total = sum(live_flags)
        successes = sum(map(truth, compress(map(itemgetter(1), window), live_flags)))
        implicit_successes = window_size - non_stale_total
        return (successes + implicit_successes) / window_size

    def get_unreliability_multiplier(
        self,
        peer: NodeAddress,
        now: float | None = None,
    ) -> float:
        """Return ``1 / reliability`` clamped to the window-derived cap.

        Under the smoothed reliability formula in ``get_reliability``,
        an all-failed window of size ``N`` reports reliability ``0``
        (numerator is zero). The cap maps that to ``window_size + 1``
        — strictly bounded, no infinity, no magic constant beyond the
        operator-configured window size.
        """
        reliability = self.get_reliability(peer, now=now)
        floor = 1.0 / (self._config.window_size + 1)
        if reliability < floor:
            return float(self._config.window_size + 1)
        return 1.0 / reliability

    def had_recent_success(
        self,
        peer: NodeAddress,
        within_seconds: float,
        now: float | None = None,
    ) -> bool:
        """Return True iff the **most recent** in-window probe for ``peer`` succeeded.

        AD-53 escalation gate. Two competing readings are possible for
        "had recent success":

        * "Any sample within the window is success" — too lenient for
          escalation gating. A peer that succeeded at ``t=−25 s`` and
          has failed every probe since (e.g. a gracefully-leaving
          worker whose LEAVE has not yet been processed) would still
          satisfy this predicate, so the burst-failure speculative
          DEAD path would refuse to kill it and registry cleanup
          stalls until the LEAVE handler runs.

        * "Most recent in-window sample is success" — the intended
          semantics. The most recent outcome is the only one that
          reflects the peer's *current* SWIM reachability. A peer
          whose latest probe failed is part of the burst by
          construction; one whose latest probe succeeded is not.

        This implementation uses the latter. ``False`` for an unknown
        peer (no samples) is preserved — the predicate asks for
        evidence of success, not absence of evidence of failure.
        """
        window = self._windows.get(peer)
        if not window:
            return False
        now = self._resolve_now(now)
        cutoff = now - within_seconds
        latest_time, latest_success = window[-1]
        if latest_time < cutoff:
            return False
        return latest_success

    def had_success_since(
        self,
        peer: NodeAddress,
        since: float,
        now: float | None = None,
    ) -> bool:
        """Return True iff the latest live probe sample since ``since`` succeeded.

        This is the burst-failure refutation predicate. A sample before
        the burst began is not useful evidence that the peer survived
        the burst, and a later failure must override an earlier success.
        Unknown peers return ``False`` because this asks for concrete
        positive liveness evidence.
        """
        window = self._windows.get(peer)
        if not window:
            return False

        now = self._resolve_now(now)

        cutoff = max(since, now - self._config.sample_ttl_s)
        latest_time, latest_success = window[-1]
        if latest_time < cutoff:
            return False
        return latest_success

    def remove_peer(self, peer: NodeAddress) -> None:
        """Drop tracking state for ``peer`` (e.g. on declared death)."""
        self._windows.pop(peer, None)

    def evict_stale_peers(self, now: float | None = None) -> int:
        """Evict peers whose entire window is stale.

        Returns the number of peers evicted. Should be invoked
        periodically by the host's reconciliation loop to bound
        long-term memory growth from peer churn.
        """
        now = self._resolve_now(now)
        cutoff = now - self._config.sample_ttl_s
        # Keys and values iterate in the same order, so compress selects the stale peers.
        stale: list[NodeAddress] = list(
            compress(
                self._windows.keys(),
                map(self._window_is_stale, self._windows.values(), repeat(cutoff)),
            )
        )
        for peer in stale:
            self._windows.pop(peer, None)
        return len(stale)

    @staticmethod
    def _window_is_stale(window: deque[tuple[float, bool]], cutoff: float) -> bool:
        """Whether ``window`` is empty or holds only samples older than ``cutoff``."""
        return not window or all(sample_time < cutoff for sample_time, _ in window)

    def _resolve_now(self, now: float | None) -> float:
        """``now`` when the caller supplied it, else a fresh read of the tracker's clock."""
        if now is None:
            now = self._clock.monotonic()
        return now

    def get_tracked_peer_count(self) -> int:
        """Number of peers with at least one sample in the window."""
        return len(self._windows)

_REHOMED = (
    PeerProbeReliabilityConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
