"""
AdaptiveEWMASelector's random-selection mode is seeded.

With ``PowerOfTwoConfig.random_seed=None`` (the production default) the
selector previously built an unseeded ``random.Random(None)`` — random-
mode candidate picks differed run to run, a replay-determinism hole.
The no-seed path now draws from the module ``_DEFAULT_RANDOM`` seam
(rebound by ``swap_defaults`` under SIM); an explicit seed still gets
its own independent ``random.Random(seed)`` stream.
"""

import hyperscale.distributed.discovery.selection.adaptive_selector as selector_module
from hyperscale.distributed.discovery.selection.adaptive_selector import (
    AdaptiveEWMASelector,
    PowerOfTwoConfig,
)
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from tests.simulation.harness.sim.seeded_random import SeededRandom


def _random_mode_selector(random_seed: int | None = None) -> AdaptiveEWMASelector:
    selector = AdaptiveEWMASelector(
        power_of_two_config=PowerOfTwoConfig(
            candidate_count=2,
            use_rendezvous_ranking=False,
            random_seed=random_seed,
        ),
    )
    for peer_index in range(8):
        selector.add_peer(f"peer-{peer_index}")
    return selector


def _selection_sequence(selector: AdaptiveEWMASelector) -> list[str]:
    return [selector.select(f"key-{index}").peer_id for index in range(32)]


def test_swap_defaults_reaches_adaptive_selector():
    snapshot = snapshot_defaults()
    try:
        touched = swap_defaults(random_source=SeededRandom(seed=1))
        assert (
            "hyperscale.distributed.discovery.selection.adaptive_selector"
            in touched
        )
    finally:
        restore_defaults(snapshot)


def test_unseeded_selector_is_deterministic_under_sim_swap():
    snapshot = snapshot_defaults()
    try:

        def run_once() -> list[str]:
            swap_defaults(random_source=SeededRandom(seed=7))
            return _selection_sequence(_random_mode_selector())

        assert run_once() == run_once()
    finally:
        restore_defaults(snapshot)


def test_explicit_seed_gets_an_independent_deterministic_stream():
    snapshot = snapshot_defaults()
    try:
        swap_defaults(random_source=SeededRandom(seed=7))
        seeded_first = _selection_sequence(_random_mode_selector(random_seed=99))
        # Drain the module seam so a shared stream WOULD diverge...
        selector_module._DEFAULT_RANDOM.sample(list(range(64)), 32)
        seeded_second = _selection_sequence(_random_mode_selector(random_seed=99))
        # ...but the explicit-seed stream is independent of the seam.
        assert seeded_first == seeded_second
    finally:
        restore_defaults(snapshot)
