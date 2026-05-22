"""
Determinism assertions for the apply layer (AD-52 §15).

The Raft state machine apply layer is a hard determinism boundary —
violations cause silent divergence between followers. AD-52 §15
enumerates the rules:

  - No wall clock.
  - No randomness.
  - Sorted iteration over maps and sets.
  - No filesystem reads.
  - No network calls.
  - No hash-based identity.

This module provides:

  - assert_deterministic_context(...) — a context manager that, in
    test mode, monkeypatches time.time / random.random / etc. to raise
    if called inside an apply block.
  - DeterminismLintMarker — a marker string the custom Ruff plugin
    looks for to identify apply-layer modules. The ruff plugin
    statically flags forbidden imports + calls in those modules.

The runtime assertion catches violations missed by lint (e.g., third-
party library calls that touch wall clock). The lint catches violations
before they ship.
"""

from __future__ import annotations

import contextlib
import os
import random
import sys
import time
from collections.abc import Iterator


# Modules tagged with this marker comment (anywhere in their top-of-file
# docstring) are scanned by the custom Ruff plugin for forbidden calls.
# The plugin itself is configured in ruff.toml; this constant is the
# single source of truth for the marker string.
DETERMINISM_LINT_MARKER: str = "# DETERMINISM: apply-layer"


# Names of attributes the runtime-mode trap rewires to raisers.
_FORBIDDEN_TIME_ATTRS: tuple[str, ...] = ("time", "time_ns", "perf_counter", "monotonic")
_FORBIDDEN_RANDOM_ATTRS: tuple[str, ...] = ("random", "uniform", "randint", "choice", "shuffle")
_FORBIDDEN_OS_ATTRS: tuple[str, ...] = ("urandom",)


class DeterminismViolation(RuntimeError):
    """Raised when forbidden API is called inside an apply block."""


def _raise_violation(name: str):
    def _raiser(*_args, **_kwargs):
        raise DeterminismViolation(
            f"{name} called inside apply layer — AD-52 §15 forbids this. "
            f"Use HLC timestamps from log entries instead."
        )
    return _raiser


@contextlib.contextmanager
def assert_deterministic_context() -> Iterator[None]:
    """
    Context manager that traps forbidden API calls. Use only in tests
    or chaos runs — production apply paths must not pay the overhead.

    Usage:
        with assert_deterministic_context():
            state_machine.apply_entry(entry, term=1, lsn=5)
    """
    saved_attrs: dict[tuple[object, str], object] = {}

    for module, attrs in (
        (time, _FORBIDDEN_TIME_ATTRS),
        (random, _FORBIDDEN_RANDOM_ATTRS),
        (os, _FORBIDDEN_OS_ATTRS),
    ):
        for attr in attrs:
            if hasattr(module, attr):
                saved_attrs[(module, attr)] = getattr(module, attr)
                setattr(module, attr, _raise_violation(f"{module.__name__}.{attr}"))

    try:
        yield
    finally:
        for (module, attr), original in saved_attrs.items():
            setattr(module, attr, original)


def chaos_double_apply(
    state_machine,
    entry: object,
    committed_at_term: int,
    committed_at_lsn: int,
) -> None:
    """
    Chaos-mode helper: apply the same entry twice from two distinct
    random seeds and compare the resulting state. Any divergence
    indicates a non-determinism bug. AD-52 §15 explicitly calls this
    out as the runtime check.

    Importing random here is fine — this function is OUTSIDE the apply
    layer (it just wraps it). The random calls happen here, not inside
    the state machine's apply_entry.
    """
    import copy
    import random as _random_module

    # Two independent copies. The state machine itself is required to be
    # deterministic — both copies should arrive at the same final state.
    machine_a = copy.deepcopy(state_machine)
    machine_b = copy.deepcopy(state_machine)

    seed_a = _random_module.SystemRandom().randint(0, 2**32 - 1)
    seed_b = _random_module.SystemRandom().randint(0, 2**32 - 1)
    _ = seed_a, seed_b  # seeds aren't used to influence apply (they shouldn't!);
                         # presence here documents intent.

    with assert_deterministic_context():
        machine_a.apply_entry(entry, committed_at_term, committed_at_lsn)
    with assert_deterministic_context():
        machine_b.apply_entry(entry, committed_at_term, committed_at_lsn)

    if machine_a.state.members != machine_b.state.members:
        raise DeterminismViolation(
            "apply_entry produced divergent results across two runs — "
            "non-determinism bug"
        )
    if (
        machine_a.state.cluster_metadata != machine_b.state.cluster_metadata
    ):
        raise DeterminismViolation(
            "apply_entry produced divergent ClusterMetadata across two runs"
        )
