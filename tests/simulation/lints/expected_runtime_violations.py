"""
Phase 5 ratcheted snapshot — the set of production files under
``hyperscale/distributed/`` that currently contain a direct call to
``time.X`` / ``asyncio.sleep`` / ``asyncio.wait_for`` /
non-crypto ``random.X``.

Each Phase 5b–5d migration commit removes its migrated file paths
from this set. The accompanying lint
``tests/simulation/lints/test_no_direct_time_random.py`` fails when
the discovered set differs from this snapshot in either direction,
so the snapshot must always reflect current state exactly.

Final state after Phase 5e: this set contains only
``runtime/real_clock.py`` and ``runtime/real_random.py`` — the two
modules that are intentionally allowed to call the underlying
stdlib functions, since they ARE the seam adapters.

Stored as Python (not a text snapshot) so the project's ``*.txt``
gitignore rule doesn't accidentally hide it from version control.
"""

EXPECTED_RUNTIME_VIOLATIONS: frozenset[str] = frozenset(
    {
        "hyperscale/distributed/runtime/real_clock.py",
        "hyperscale/distributed/runtime/real_random.py",
        # ``swim/health_aware_server.py`` is the SWIM dispatcher
        # façade. The Phase 5c.4d migration that routed every
        # ``time.X`` / ``asyncio.sleep`` / ``asyncio.wait_for`` /
        # ``random.X`` call through ``self._clock`` / ``self._random``
        # introduced a Phase 4 cluster-stabilization regression
        # (managers stayed at non-baseline LHM for >60 s — confirmed
        # by per-commit bisect). Even with a zero-overhead
        # ``RealClock`` (``sleep`` / ``wait_for`` as plain methods
        # returning the asyncio coroutines directly rather than
        # ``async def`` wrappers), some timing-sensitive probe-ack
        # race in this file's tight probe loop keeps the test
        # failing after the migration but passes against the
        # original direct calls. The functional reduction is left
        # in place; this allowlist entry documents the intentional
        # deferral until the regression can be diagnosed without
        # blocking the rest of Phase 5.
        "hyperscale/distributed/swim/health_aware_server.py",
    }
)
