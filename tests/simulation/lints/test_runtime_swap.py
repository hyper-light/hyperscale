"""
Phase 5 / Phase 6 contract test — verify ``runtime.swap_defaults``
actually propagates the SIM-mode clock through every module that
defines ``_DEFAULT_CLOCK`` / ``_DEFAULT_RANDOM``.

This test exists to lock in two correctness properties the
remediation pass restored:

1. **Lambda-wrapped dataclass defaults pick up the swap.** Every
   ``field(default_factory=_DEFAULT_CLOCK.X)`` was re-wrapped to
   ``field(default_factory=lambda: _DEFAULT_CLOCK.X())`` so the
   binding resolves at factory call time, not at class definition.
   If a future migration regresses this — by introducing a new
   ``field(default_factory=_DEFAULT_CLOCK.monotonic)`` — the dataclass
   below would silently keep using the original ``RealClock`` and
   this test would catch it.

2. **Every production submodule that holds a singleton is reachable
   by the swap.** ``swap_defaults`` walks ``sys.modules`` for any
   module under ``hyperscale.distributed`` that defines
   ``_DEFAULT_CLOCK`` or ``_DEFAULT_RANDOM``. The test imports a
   representative cross-section of those modules before swapping,
   so the walker has something to find, and then asserts that the
   touched-module count is non-trivial.

The test is a small structural smoke — it doesn't try to exhaustively
enumerate every singleton-bearing module (that's the lint's job).
It's about catching the swap mechanism itself regressing.
"""

from __future__ import annotations
from hyperscale.distributed.runtime.swap import _PRODUCTION_PREFIXES


def test_swap_defaults_propagates_to_dataclass_default_factories() -> None:
    """A fake clock installed via ``swap_defaults`` must be the one
    that dataclass ``field(default_factory=lambda: _DEFAULT_CLOCK.X())``
    invocations land on at construction time."""
    # Local imports keep the test self-contained and ensure
    # ``sys.modules`` is populated only at test execution.
    import hyperscale.distributed  # noqa: F401  — eager-import side effects
    from hyperscale.distributed.health.tracker import HealthPiggyback
    from hyperscale.distributed.runtime import (
        restore_defaults,
        snapshot_defaults,
        swap_defaults,
    )

    class _FakeClock:
        def monotonic(self) -> float:
            return 12345.0

        def time(self) -> float:
            return 67890.0

        async def sleep(self, seconds: float) -> None:
            return None

        async def wait_for(self, awaitable, timeout):
            return await awaitable

    snapshot = snapshot_defaults()
    fake = _FakeClock()
    try:
        touched = swap_defaults(clock=fake)
        assert touched, (
            "swap_defaults touched zero modules — the production tree "
            "should have at least one module-level _DEFAULT_CLOCK "
            "binding."
        )

        # ``HealthPiggyback.timestamp`` uses
        # ``field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())``,
        # so a fresh instance constructed *while the swap is active*
        # should pick up the fake's ``monotonic()`` return value.
        piggyback = HealthPiggyback(node_id="test", node_type="manager")
        assert piggyback.timestamp == 12345.0, (
            "Dataclass default_factory did not pick up the swapped "
            f"_DEFAULT_CLOCK; got timestamp={piggyback.timestamp}. "
            "This usually means the field was re-bound to a captured "
            "bound method (the Phase 5 SIM-correctness regression)."
        )

    finally:
        restore_defaults(snapshot)

    # After restore, a fresh instance must NOT use the fake clock —
    # i.e. the timestamp should differ from the fake's fixed return.
    piggyback_after = HealthPiggyback(node_id="test", node_type="manager")
    assert piggyback_after.timestamp != 12345.0, (
        "restore_defaults did not roll back the swap; "
        f"_DEFAULT_CLOCK is still the fake (timestamp={piggyback_after.timestamp})."
    )


def test_swap_defaults_covers_a_representative_module_set() -> None:
    """The swap walker must reach modules across every major
    subsystem (swim, health, nodes, jobs, raft, ...). The exact list
    is implementation-detail; we assert structural coverage by
    checking that the touched set spans more than one top-level
    subsystem under ``hyperscale.distributed``."""
    import hyperscale.distributed  # noqa: F401
    from hyperscale.distributed.runtime import (
        restore_defaults,
        snapshot_defaults,
        swap_defaults,
    )

    # Import a sampling of subsystems so their _DEFAULT_CLOCK
    # bindings are in sys.modules before the walker runs.
    import hyperscale.distributed.swim.detection.suspicion_state  # noqa: F401
    import hyperscale.distributed.health.tracker  # noqa: F401
    import hyperscale.distributed.routing.observed_latency_tracker  # noqa: F401
    import hyperscale.distributed.taskex.snowflake.snowflake_generator  # noqa: F401

    class _FakeClock:
        def monotonic(self) -> float:
            return 0.0

        def time(self) -> float:
            return 0.0

        async def sleep(self, seconds: float) -> None:
            return None

        async def wait_for(self, awaitable, timeout):
            return await awaitable

    snapshot = snapshot_defaults()
    try:
        touched = swap_defaults(clock=_FakeClock())
    finally:
        restore_defaults(snapshot)

    # The touched list is a flat list of dotted module names. Extract
    # the top-level subsystem ('swim', 'health', 'routing', etc.)
    # from each so we can assert breadth of coverage.
    subsystems: set[str] = set()
    for module_name in touched:
        # Assert against the swap module's OWN declared production
        # surface rather than a hardcoded prefix: the seam
        # legitimately covers hyperscale.logging (filesystem, HLC
        # wall clock) and the narrow hyperscale.core id/time
        # modules. Reading the prefixes from the module under test
        # keeps one source of truth and still fails if a swap ever
        # reaches outside the declared surface.
        assert module_name.startswith(
            tuple(f"{prefix}." for prefix in _PRODUCTION_PREFIXES)
        ), module_name
        suffix = module_name[len("hyperscale.distributed."):]
        first = suffix.split(".", 1)[0]
        subsystems.add(first)

    assert len(subsystems) >= 4, (
        "swap_defaults touched modules from too few subsystems "
        f"({sorted(subsystems)}). Expected ≥4 — if this drops, "
        "either the production tree's singleton coverage shrank "
        "unexpectedly or the walker is missing something."
    )
