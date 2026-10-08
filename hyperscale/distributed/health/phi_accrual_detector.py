import math
from collections import deque

from .phi_accrual_config import PhiAccrualConfig

# math.exp overflows past ~709.78; phi below the mean is 0 well before.
_EXPONENT_CEILING = 700.0


class PhiAccrualDetector:
    """A phi-accrual failure detector for one edge (Hayashibara et al.,
    "The phi accrual failure detector", 2004; AD-52 section 8): phi is how
    unlikely it is, given the heartbeats' observed inter-arrival times, that
    the next one is still on its way -- ``-log10(P(next arrives later))``.
    Phi 1 is a 10% chance of a wrong suspicion, 8 one in a hundred million.

    Arrivals are modelled as normally distributed (Akka's formulation, with
    the logistic approximation of the normal CDF), over a bounded window of
    intervals kept as running sums: O(1) per heartbeat and per reading, and
    bounded memory per edge.
    """

    __slots__ = (
        "_config",
        "_intervals",
        "_interval_sum",
        "_interval_square_sum",
        "_last_heartbeat_at",
    )

    def __init__(self, config: PhiAccrualConfig) -> None:
        self._config = config
        # Seeded as Akka seeds it: two intervals a quarter of the estimate
        # either side of it, so the first readings are neither certain nor
        # blind.
        estimate = config.first_heartbeat_estimate_seconds
        deviation = estimate / 4
        seeded = (estimate - deviation, estimate + deviation)
        self._intervals: deque[float] = deque(seeded, maxlen=config.max_sample_size)
        self._interval_sum = sum(seeded)
        self._interval_square_sum = sum(interval * interval for interval in seeded)
        self._last_heartbeat_at: float | None = None

    def heartbeat(self, now: float) -> None:
        """Record a heartbeat arriving at ``now``."""
        if self._last_heartbeat_at is not None:
            interval = now - self._last_heartbeat_at
            if len(self._intervals) == self._intervals.maxlen:
                evicted = self._intervals[0]
                self._interval_sum -= evicted
                self._interval_square_sum -= evicted * evicted
            self._intervals.append(interval)
            self._interval_sum += interval
            self._interval_square_sum += interval * interval
        self._last_heartbeat_at = now

    def phi(self, now: float) -> float:
        """The suspicion level at ``now``: 0 before any heartbeat, rising
        the longer the next one is overdue."""
        if self._last_heartbeat_at is None:
            return 0.0
        count = len(self._intervals)
        mean = self._interval_sum / count
        variance = max(0.0, self._interval_square_sum / count - mean * mean)
        deviation = max(math.sqrt(variance), self._config.min_std_deviation_seconds)
        expected = mean + self._config.acceptable_heartbeat_pause_seconds
        elapsed = now - self._last_heartbeat_at
        normalized = (elapsed - expected) / deviation
        exponent = min(-normalized * (1.5976 + 0.070566 * normalized * normalized), _EXPONENT_CEILING)
        logistic = math.exp(exponent)
        if elapsed > expected:
            # The chance the heartbeat is still coming underflows to 0 once
            # it is overdue by many deviations: certainly suspected.
            return math.inf if logistic == 0.0 else -math.log10(logistic / (1.0 + logistic))
        return -math.log10(1.0 - 1.0 / (1.0 + logistic))

    def is_available(self, now: float) -> bool:
        """Whether the edge is below the suspicion threshold at ``now``."""
        return self.phi(now) < self._config.threshold
