"""
TimeWindowedTDigest prunes when a window opens, not on every add (D-5: the
manager records each dispatch twice, per datacenter and per worker).

The observation read at any time must equal what pruning after every add
gave -- the reference below is that former ``add``. The window cap bounds
the digests kept between openings, and the read prunes by age itself, so
the two can never be told apart: checked after every add, read at the add's
own time and later, over clocks that creep within a window, jump across
several, idle past the whole retention, and step backwards.
"""

import random

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.slo import LatencyObservation, SLOConfig, TimeWindowedTDigest

SEEDS = range(8)
STEPS_PER_RUN = 1_500
# Reads prune too, so most adds go unread: the windows an add keeps are
# observed as they stand after many adds, not after each.
READ_PROBABILITY = 0.1


class PruneOnEveryAddDigest(TimeWindowedTDigest):
    """The reference: the former add, which pruned after every sample."""

    def add(self, value: float, timestamp: float, weight: float = 1.0) -> None:
        window_start = int(timestamp / self._window_duration_seconds) * self._window_duration_seconds
        self._register_window(window_start)
        self._windows[window_start].add(value, weight)
        self._prune_windows(timestamp)


def observed(digest: TimeWindowedTDigest, now: float) -> tuple | None:
    observation: LatencyObservation | None = digest.get_recent_observation(target_id="target", now=now)
    if observation is None:
        return None
    return (
        observation.p50_ms,
        observation.p95_ms,
        observation.p99_ms,
        observation.sample_count,
        observation.window_start,
        observation.window_end,
    )


def clock_steps(seed: int, window_seconds: float, retention_seconds: float) -> list[float]:
    """A clock that creeps, jumps windows, idles past retention and steps back."""
    generator = random.Random(seed)
    step_choices = (
        lambda: generator.uniform(0.0, window_seconds / 20.0),
        lambda: generator.uniform(window_seconds, 3.0 * window_seconds),
        lambda: generator.uniform(retention_seconds, 2.0 * retention_seconds),
        lambda: -generator.uniform(0.0, 2.0 * window_seconds),
    )
    weights = (0.85, 0.08, 0.02, 0.05)
    timestamps: list[float] = []
    now = retention_seconds
    for _ in range(STEPS_PER_RUN):
        now = max(0.0, now + generator.choices(step_choices, weights)[0]())
        timestamps.append(now)
    return timestamps


@pytest.mark.parametrize("seed", SEEDS)
def test_pruning_on_window_open_reads_as_pruning_on_every_add(seed: int) -> None:
    config = SLOConfig.from_env(Env())
    retention_seconds = config.window_duration_seconds * config.max_windows
    digest = TimeWindowedTDigest(config=config)
    reference = PruneOnEveryAddDigest(config=config)
    generator = random.Random(seed)

    for timestamp in clock_steps(seed, config.window_duration_seconds, retention_seconds):
        latency_ms = generator.lognormvariate(3.0, 1.0)
        digest.add(latency_ms, timestamp)
        reference.add(latency_ms, timestamp)
        # The cap holds between reads too: adds alone never grow the digest.
        assert len(digest._windows) <= config.max_windows
        if generator.random() < READ_PROBABILITY:
            read_at = timestamp + generator.uniform(0.0, config.window_duration_seconds)
            assert observed(digest, read_at) == observed(reference, read_at)
