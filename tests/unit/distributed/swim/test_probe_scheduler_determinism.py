"""
SWIM probe order is a determinism input and must be seeded.

The probe scheduler shuffles the member list and inserts new members at
a random position. Under an unseeded ``random`` those choices differed
run to run, so the sequence of peers probed — and therefore the order in
which cross-datacenter messages are emitted — was non-deterministic,
breaking multi-DC replay. The scheduler now routes both choices through
the module-level ``_DEFAULT_RANDOM`` seam (``sample`` for the shuffle,
``randrange`` for the insert), which ``swap_defaults`` rebinds to the
seeded SIM ``Random``. These tests pin that: same seed → identical probe
order, different seed → different order, and the shuffle stays a real
permutation.
"""

import hyperscale.distributed.swim.detection.probe_scheduler as probe_scheduler_module
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from tests.simulation.harness.sim.seeded_random import SeededRandom


_MEMBERS = [("10.0.0.%d" % index, 9000 + index) for index in range(12)]


def _probe_order_for_seed(seed: int) -> list[tuple[str, int] | None]:
    """Return two full cycles of probe targets under a seeded RNG."""
    swap_defaults(random_source=SeededRandom(seed=seed))
    scheduler = probe_scheduler_module.ProbeScheduler()
    scheduler.update_members(list(_MEMBERS))
    return [
        scheduler.get_next_target()
        for _ in range(len(_MEMBERS) * 2)
    ]


def test_swap_defaults_reaches_probe_scheduler():
    """The seam is worthless if ``swap_defaults`` doesn't rebind this
    module — pin that the module exposes a swappable ``_DEFAULT_RANDOM``."""
    snapshot = snapshot_defaults()
    try:
        touched = swap_defaults(random_source=SeededRandom(seed=1))
        assert (
            "hyperscale.distributed.swim.detection.probe_scheduler" in touched
        )
    finally:
        restore_defaults(snapshot)


def test_probe_order_is_deterministic_for_a_fixed_seed():
    snapshot = snapshot_defaults()
    try:
        first_run = _probe_order_for_seed(7)
        second_run = _probe_order_for_seed(7)
        assert first_run == second_run
    finally:
        restore_defaults(snapshot)


def test_probe_order_varies_with_the_seed():
    snapshot = snapshot_defaults()
    try:
        # If the order didn't depend on the seed, the shuffle wouldn't be
        # driven by the RNG at all (e.g. a no-op) — this guards that.
        assert _probe_order_for_seed(7) != _probe_order_for_seed(99)
    finally:
        restore_defaults(snapshot)


def test_shuffle_is_a_full_permutation():
    snapshot = snapshot_defaults()
    try:
        order = _probe_order_for_seed(7)
        first_cycle = order[: len(_MEMBERS)]
        # Every member appears exactly once per cycle — ``sample(list,
        # len(list))`` must behave like a shuffle, not a lossy sample.
        assert sorted(first_cycle) == sorted(_MEMBERS)
    finally:
        restore_defaults(snapshot)
