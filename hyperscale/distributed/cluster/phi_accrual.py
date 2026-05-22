"""
Phi-accrual failure detector (AD-52 §8).

Per Hayashibara et al. ("The phi-accrual failure detector", 2004), used
in production by Cassandra, Akka, Hazelcast.

For each monitored peer, maintain a sliding window of heartbeat
inter-arrival times. At time t, compute

    P_later(t) ≈ 1 - F(t)
    φ(t)       = -log10(P_later(t))

where F is the CDF of inter-arrival intervals (approximated as a
Normal distribution from the sliding window's mean + variance).

Higher φ means the heartbeat is increasingly overdue relative to the
peer's historical cadence. Threshold of 8.0 ≈ 99.999999% probability
of failure; 4.0 ≈ 99.99%. AD-52 §8 picks 8.0 for declaration, 4.0 for
warning.

Composed alongside SWIM in failure_detector.py — phi-accrual provides
per-edge precision for circuit-breaking decisions while SWIM provides
cluster-wide convergence.

Outside the deterministic apply layer (AD-52 §15) so monotonic time is
allowed.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass


_DEFAULT_WINDOW_SIZE: int = 100
_DEFAULT_MIN_STDDEV_MS: float = 100.0
_DEFAULT_ACCEPTABLE_HEARTBEAT_PAUSE_MS: float = 0.0


@dataclass(slots=True)
class PhiAccrualConfig:
    """
    window_size                    Sliding-window length for inter-arrival
                                   samples.
    min_stddev_ms                  Floor on the stddev used in the Normal
                                   CDF — avoids φ exploding on a perfectly-
                                   regular peer (where stddev → 0).
    acceptable_heartbeat_pause_ms  Grace window subtracted from the elapsed
                                   time before computing φ. Useful for
                                   cross-DC edges where the AD-35 baseline
                                   is naturally jittery.
    """

    window_size: int = _DEFAULT_WINDOW_SIZE
    min_stddev_ms: float = _DEFAULT_MIN_STDDEV_MS
    acceptable_heartbeat_pause_ms: float = _DEFAULT_ACCEPTABLE_HEARTBEAT_PAUSE_MS


class PhiAccrualDetector:
    """
    One detector per monitored peer. Thread-safe under the AD-2
    asyncio-everywhere assumption: heartbeat() and phi() must be called
    from the same event loop (no cross-thread synchronization here).
    """

    __slots__ = (
        "_config",
        "_last_arrival_monotonic",
        "_intervals_ms",
        "_baseline_rtt_ms",
        "_running_sum_ms",
        "_running_sum_sq_ms",
    )

    def __init__(
        self,
        config: PhiAccrualConfig | None = None,
        baseline_rtt_ms: float | None = None,
    ) -> None:
        """
        Args:
          baseline_rtt_ms: AD-35 Vivaldi RTT estimate used to prime the
            mean inter-arrival before any heartbeats arrive. If None,
            phi() returns 0.0 until at least one heartbeat has landed.
        """
        self._config = config or PhiAccrualConfig()
        self._last_arrival_monotonic: float = 0.0
        self._intervals_ms: deque[float] = deque(maxlen=self._config.window_size)
        self._baseline_rtt_ms = baseline_rtt_ms
        self._running_sum_ms: float = 0.0
        self._running_sum_sq_ms: float = 0.0

    def heartbeat(self, arrival_monotonic: float | None = None) -> None:
        """Record a heartbeat arrival. Pass arrival_monotonic for
        deterministic testing; otherwise time.monotonic() is sampled."""
        sampled_monotonic = (
            arrival_monotonic
            if arrival_monotonic is not None
            else time.monotonic()
        )
        if self._last_arrival_monotonic > 0.0:
            interval_ms = (sampled_monotonic - self._last_arrival_monotonic) * 1000.0
            if interval_ms > 0.0:
                if len(self._intervals_ms) == self._intervals_ms.maxlen:
                    # Maintain incremental sum / sum-of-squares by
                    # removing the oldest entry before appending.
                    oldest = self._intervals_ms[0]
                    self._running_sum_ms -= oldest
                    self._running_sum_sq_ms -= oldest * oldest
                self._intervals_ms.append(interval_ms)
                self._running_sum_ms += interval_ms
                self._running_sum_sq_ms += interval_ms * interval_ms
        self._last_arrival_monotonic = sampled_monotonic

    def phi(self, current_monotonic: float | None = None) -> float:
        """Compute φ at the given (or current) time. Returns 0.0 until at
        least one heartbeat has arrived AND either we have a sliding-
        window sample or a baseline RTT."""
        if self._last_arrival_monotonic == 0.0:
            return 0.0
        sampled_monotonic = (
            current_monotonic
            if current_monotonic is not None
            else time.monotonic()
        )
        elapsed_ms = (sampled_monotonic - self._last_arrival_monotonic) * 1000.0
        elapsed_ms -= self._config.acceptable_heartbeat_pause_ms
        if elapsed_ms <= 0.0:
            return 0.0

        mean_ms, stddev_ms = self._mean_and_stddev_ms()
        if mean_ms is None or stddev_ms is None:
            return 0.0

        # Clamp stddev to the floor — avoids φ blowing up for perfectly
        # regular heartbeats.
        stddev_ms = max(stddev_ms, self._config.min_stddev_ms)

        # P(later than elapsed) ≈ 1 - Φ((elapsed - mean) / stddev)
        # where Φ is the standard Normal CDF. We use the erfc
        # approximation for numeric stability at the tails.
        normalized = (elapsed_ms - mean_ms) / stddev_ms
        p_later = 0.5 * math.erfc(normalized / math.sqrt(2.0))
        if p_later <= 0.0:
            # Floor at a tiny value so log10 is finite.
            p_later = 1e-300
        return -math.log10(p_later)

    def _mean_and_stddev_ms(self) -> tuple[float | None, float | None]:
        sample_count = len(self._intervals_ms)
        if sample_count == 0:
            if self._baseline_rtt_ms is None:
                return None, None
            # Use the baseline as both mean and stddev floor — gives a
            # reasonable detector on a peer that has not yet acked.
            return self._baseline_rtt_ms, self._baseline_rtt_ms / 2.0
        mean_ms = self._running_sum_ms / sample_count
        if sample_count == 1:
            return mean_ms, self._config.min_stddev_ms
        variance_ms = (self._running_sum_sq_ms / sample_count) - (mean_ms * mean_ms)
        if variance_ms < 0.0:
            variance_ms = 0.0
        return mean_ms, math.sqrt(variance_ms)
