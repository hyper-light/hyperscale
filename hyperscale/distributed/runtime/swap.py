"""
Phase 6 SIM-mode entry point — swap every module-level ``_DEFAULT_CLOCK``
and ``_DEFAULT_RANDOM`` binding under ``hyperscale.distributed.*`` to
the SIM implementations in one call.

Why this is needed
------------------

Phase 5 routed every production direct call to ``time`` / ``asyncio.sleep``
/ ``asyncio.wait_for`` / non-crypto ``random.X`` through one of two
patterns:

1. **Constructor-injected** ``self._clock`` / ``self._random`` on
   classes that already had an ``__init__`` — ``MercurySyncBaseServer``
   and its subclasses, the SWIM detection regular classes,
   ``ExtensionDecisionEvaluator``, ``RetryExecutor``, etc.
   These are Phase-6-ready out of the box: a SIM test fixture
   constructs the cluster and passes ``clock=virtual_clock`` /
   ``random_source=seeded_random`` through.

2. **Module-level singleton** ``_DEFAULT_CLOCK: Clock = RealClock()``
   on every other module — dataclasses, helper coordinators, the
   gate / manager / worker helper layers. These are SIM-correct
   only when the singletons are swapped *before* any consuming
   module reads them. Inline ``_DEFAULT_CLOCK.X()`` calls resolve
   from the module's globals at call time, so a swap propagates;
   the dataclass ``field(default_factory=lambda: _DEFAULT_CLOCK.X())``
   defaults wrap the lookup in a lambda for the same reason.

This module is the canonical place a Phase 6 SIM-mode entry point
calls into before constructing the cluster::

    from hyperscale.distributed.runtime import swap_defaults
    from tests.simulation.harness.virtual_clock import VirtualClock
    from tests.simulation.harness.seeded_random import SeededRandom

    virtual_clock = VirtualClock()
    seeded_random = SeededRandom(seed=0)
    swap_defaults(clock=virtual_clock, random_source=seeded_random)
    # ... construct ClusterHarness, run scenario ...

Without this, every test would have to monkey-patch each module by
hand — fragile and easy to forget. With it, the swap is one call
and the test harness owns the contract.

What it does
------------

``swap_defaults`` walks ``sys.modules`` for any module whose
``__name__`` starts with ``hyperscale.distributed`` and rebinds the
module attributes ``_DEFAULT_CLOCK`` / ``_DEFAULT_RANDOM`` when
present. Returns the list of fully-qualified module names it
touched so the caller can assert coverage (the simulation harness
does this).

The swap is non-recursive (no need to swap nested modules
explicitly — they're already in ``sys.modules``) and idempotent:
calling it twice with the same values is a no-op. To restore the
defaults at test teardown, hold the return value of ``snapshot_defaults``
and pass it back to ``restore_defaults``. The harness does this
under a fixture so a SIM test's swap doesn't leak across tests in
the same process.

Constraints
-----------

The swap targets dataclass ``field(default_factory=lambda: _DEFAULT_CLOCK.X())``
defaults because the lambda resolves the binding at factory call
time, not at class definition time. The earlier Phase 5c batches
used ``field(default_factory=_DEFAULT_CLOCK.X)`` which captures the
bound method at definition time and would silently keep using the
old ``RealClock``. ``Remediation 5R-1`` re-wrapped every such
default in a lambda; this swap utility relies on that re-wrap.

For classes that already accept a ``clock`` constructor parameter,
the swap is a *fallback* — explicit constructor injection at the
SIM harness's construction sites still wins. ``swap_defaults`` is
the safety net for code paths the harness doesn't explicitly
instrument (worker subprocess pools, future helper additions, etc.).
"""

from __future__ import annotations

import sys
from typing import NamedTuple

from hyperscale.core.runtime import Filesystem
from hyperscale.core.runtime import SystemResources

from .clock import Clock
from .random_source import Random


# ``hyperscale.logging`` joined the walk with the Phase 7 Filesystem
# seam: the LoggerStream's disk writes live there (a bottom-layer
# package that cannot import from ``hyperscale.distributed``), and its
# module-level ``_DEFAULT_FILESYSTEM`` must be swappable exactly like
# the distributed-side singletons.
_PRODUCTION_PREFIXES = (
    "hyperscale.distributed",
    "hyperscale.logging",
    # Narrow core inclusions: the executor-protocol snowflake generator
    # and the replay guard that validates its timestamps — minting and
    # freshness-judging MUST follow the same clock axis under SIM (see
    # each hook's docstring). Deliberately NOT all of hyperscale.core.
    "hyperscale.core.snowflake",
    "hyperscale.core.jobs.protocols",
    # The core TaskRunner's age-based cleanup and Run bookkeeping —
    # its _DEFAULT_MONOTONIC_SOURCE must follow the clock's monotonic
    # axis or max_age cleanup fires on REAL wall age at nondeterministic
    # virtual instants (see run.py's hook docstring).
    "hyperscale.core.jobs.tasks",
    # The executor graph manager's workflow-timeout ledger (see its
    # hook docstring — the final chaos-VOPR divergence mechanism).
    "hyperscale.core.jobs.graphs",
)


class _ModuleDefaults(NamedTuple):
    """Snapshot of a single module's runtime defaults."""

    module_name: str
    clock: Clock | None
    random_source: Random | None
    filesystem: Filesystem | None
    system_resources: SystemResources | None
    time_source: object | None = None
    monotonic_source: object | None = None


# Every attribute a module's snapshot is taken for (``snapshot_defaults``).
_SNAPSHOT_ATTRIBUTE_NAMES = (
    "_DEFAULT_CLOCK",
    "_DEFAULT_RANDOM",
    "_DEFAULT_FILESYSTEM",
    "_DEFAULT_TIME_SOURCE",
    "_DEFAULT_MONOTONIC_SOURCE",
)


def _matches_production_prefix(name: str, prefix: str) -> bool:
    """Whether module ``name`` is ``prefix`` or a submodule of it."""
    return name == prefix or name.startswith(prefix + ".")


def _is_production_module_name(name: str) -> bool:
    """Whether module ``name`` falls under any production prefix."""
    return any(_matches_production_prefix(name, prefix) for prefix in _PRODUCTION_PREFIXES)


def _is_loaded_production_module(name: str, mod: object) -> bool:
    """Whether a ``sys.modules`` entry is a loaded production module."""
    return mod is not None and _is_production_module_name(name)


def _iter_production_modules() -> list[tuple[str, object]]:
    """Return ``(module_name, module_object)`` for every loaded module
    under the production prefixes. ``sys.modules`` mutates during
    iteration as side-effects of attribute access load lazy modules;
    snapshot the keys before iterating so the walk is stable.
    """
    snapshot = list(sys.modules.items())
    return [(name, mod) for name, mod in snapshot if _is_loaded_production_module(name, mod)]


def _clock_bindings(clock: Clock | None) -> tuple[tuple[str, object, bool], ...]:
    """The clock axis's ``(attribute, value, counts_as_rebind)`` bindings,
    none when no clock is given."""
    if clock is None:
        return ()
    return (
        ("_DEFAULT_CLOCK", clock, True),
        # Snowflake-style wall readings follow the clock axis: the
        # virtual clock's ``time`` models the wall (including the
        # skew knob), and the realtime default stays untouched in
        # REAL mode.
        ("_DEFAULT_TIME_SOURCE", clock.time, True),
        ("_DEFAULT_MONOTONIC_SOURCE", clock.monotonic, True),
    )


def _given_bindings(
    bindings: tuple[tuple[str, object, bool], ...],
) -> list[tuple[str, object, bool]]:
    """The bindings whose value was given (a None value leaves that axis
    unchanged, and its attribute is never probed)."""
    return [binding for binding in bindings if binding[1] is not None]


def _rebind_module(module: object, bindings: list[tuple[str, object, bool]]) -> bool:
    """Rebind each binding's attribute ``module`` defines, in order;
    whether any rebind that counts as one happened."""
    rebound = False
    for attribute_name, value, counts_as_rebind in bindings:
        if hasattr(module, attribute_name):
            setattr(module, attribute_name, value)
            rebound |= counts_as_rebind
    return rebound


def swap_defaults(
    *,
    clock: Clock | None = None,
    random_source: Random | None = None,
    filesystem: Filesystem | None = None,
    system_resources: SystemResources | None = None,
) -> list[str]:
    """Rebind ``_DEFAULT_CLOCK`` / ``_DEFAULT_RANDOM`` /
    ``_DEFAULT_FILESYSTEM`` on every loaded production submodule that
    defines them.

    Any argument may be ``None`` to leave that axis unchanged —
    useful when SIM mode wants a virtual clock but the real random.

    Returns the list of module names that received at least one
    rebind, in the order encountered. Callers can assert against
    this list to catch coverage gaps (a forgotten module would
    silently keep using the real clock).
    """
    bindings = _given_bindings(
        (
            *_clock_bindings(clock),
            ("_DEFAULT_RANDOM", random_source, True),
            # A filesystem rebind alone does not mark the module touched.
            ("_DEFAULT_FILESYSTEM", filesystem, False),
            ("_DEFAULT_SYSTEM_RESOURCES", system_resources, True),
        )
    )
    return [name for name, mod in _iter_production_modules() if _rebind_module(mod, bindings)]


def _holds_snapshotted_default(mod: object) -> bool:
    """Whether ``mod`` defines any default ``snapshot_defaults`` captures."""
    return any(hasattr(mod, attribute_name) for attribute_name in _SNAPSHOT_ATTRIBUTE_NAMES)


def snapshot_defaults() -> list[_ModuleDefaults]:
    """Capture every loaded module's current ``_DEFAULT_CLOCK`` /
    ``_DEFAULT_RANDOM`` binding so a later ``restore_defaults`` call
    can revert.

    The returned list is opaque — treat it as an opaque token and
    pass it back to ``restore_defaults`` unchanged. Test fixtures
    should capture this before calling ``swap_defaults`` and restore
    it in a ``finally`` block to keep SIM-mode swaps from leaking
    across tests in the same process.
    """
    return [
        _ModuleDefaults(
            module_name=name,
            clock=getattr(mod, "_DEFAULT_CLOCK", None),
            random_source=getattr(mod, "_DEFAULT_RANDOM", None),
            filesystem=getattr(mod, "_DEFAULT_FILESYSTEM", None),
            system_resources=getattr(mod, "_DEFAULT_SYSTEM_RESOURCES", None),
            time_source=getattr(mod, "_DEFAULT_TIME_SOURCE", None),
            monotonic_source=getattr(mod, "_DEFAULT_MONOTONIC_SOURCE", None),
        )
        for name, mod in _iter_production_modules()
        if _holds_snapshotted_default(mod)
    ]


def _entry_bindings(entry: _ModuleDefaults) -> list[tuple[str, object, bool]]:
    """A snapshot entry's captured bindings, in restore order."""
    return _given_bindings(
        (
            ("_DEFAULT_CLOCK", entry.clock, True),
            ("_DEFAULT_RANDOM", entry.random_source, True),
            ("_DEFAULT_TIME_SOURCE", entry.time_source, True),
            ("_DEFAULT_MONOTONIC_SOURCE", entry.monotonic_source, True),
            ("_DEFAULT_FILESYSTEM", entry.filesystem, True),
            ("_DEFAULT_SYSTEM_RESOURCES", entry.system_resources, True),
        )
    )


def restore_defaults(snapshot: list[_ModuleDefaults]) -> None:
    """Restore the runtime-default bindings captured by a prior
    ``snapshot_defaults`` call.

    Idempotent and safe to call when modules have been unloaded
    since the snapshot — missing modules are skipped silently.
    Modules added between snapshot and restore are not touched
    (they still hold whatever was set since their import — typically
    the post-swap SIM value). Test fixtures that care about
    perfect state restoration should pair a snapshot with a fresh
    process.
    """
    for entry in snapshot:
        mod = sys.modules.get(entry.module_name)
        if mod is None:
            continue
        _rebind_module(mod, _entry_bindings(entry))
