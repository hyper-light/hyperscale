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
"""

import time
from collections import deque
from dataclasses import dataclass

NodeAddress = tuple[str, int]


@dataclass(slots=True)
class PeerProbeReliabilityConfig:
    """Configuration for ``PeerProbeReliabilityTracker``.

    ``window_size``
        Maximum number of recent probe outcomes retained per peer. The
        empirical reliability is the success-rate over this window. A
        small window reacts faster to changes; a large window is more
        stable. The window also bounds memory usage to
        ``max_tracked_peers × window_size`` ``(timestamp, success)``
        tuples.

    ``sample_ttl_s``
        Maximum age (seconds) of a sample before it is ignored on
        read. Stale outcomes from an outage hours ago must not bias
        a current suspicion.

    ``max_tracked_peers``
        Hard cap on the number of peers tracked simultaneously. New
        peers beyond this cap are silently dropped (the call becomes a
        no-op). The cap exists to prevent memory exhaustion from
        adversarial or buggy peer churn.
    """

    window_size: int = 8
    sample_ttl_s: float = 60.0
    max_tracked_peers: int = 10000


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

    def __init__(self, config: PeerProbeReliabilityConfig | None = None) -> None:
        if config is None:
            config = PeerProbeReliabilityConfig()
        self._config = config
        self._windows: dict[NodeAddress, deque[tuple[float, bool]]] = {}

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
        if now is None:
            now = time.monotonic()
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
        if now is None:
            now = time.monotonic()
        cutoff = now - self._config.sample_ttl_s
        successes = 0
        non_stale_total = 0
        for sample_time, success in window:
            if sample_time < cutoff:
                continue
            non_stale_total += 1
            if success:
                successes += 1
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
        """Return True iff a probe to ``peer`` succeeded within the window.

        AD-53 escalation gate: distinguishes "SWIM just confirmed this peer
        is alive" (defer to the global layer; job silence is workflow-side)
        from "no probe has touched this peer recently, or recent probes
        failed" (job-layer evidence is the freshest signal and may
        escalate). Unlike :meth:`get_reliability` — which smooths an
        empty window toward 1.0 because the SWIM default posture is
        "healthy unless proven otherwise" — this predicate returns
        ``False`` for an unknown peer because the question is
        specifically about *evidence of success*, not the absence of
        evidence of failure.
        """
        window = self._windows.get(peer)
        if not window:
            return False
        if now is None:
            now = time.monotonic()
        cutoff = now - within_seconds
        for sample_time, success in reversed(window):
            if sample_time < cutoff:
                return False
            if success:
                return True
        return False

    def remove_peer(self, peer: NodeAddress) -> None:
        """Drop tracking state for ``peer`` (e.g. on declared death)."""
        self._windows.pop(peer, None)

    def evict_stale_peers(self, now: float | None = None) -> int:
        """Evict peers whose entire window is stale.

        Returns the number of peers evicted. Should be invoked
        periodically by the host's reconciliation loop to bound
        long-term memory growth from peer churn.
        """
        if now is None:
            now = time.monotonic()
        cutoff = now - self._config.sample_ttl_s
        stale: list[NodeAddress] = []
        for peer, window in self._windows.items():
            if not window:
                stale.append(peer)
                continue
            if all(sample_time < cutoff for sample_time, _ in window):
                stale.append(peer)
        for peer in stale:
            self._windows.pop(peer, None)
        return len(stale)

    def get_tracked_peer_count(self) -> int:
        """Number of peers with at least one sample in the window."""
        return len(self._windows)
