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
_PRODUCTION_PREFIXES = ("hyperscale.distributed", "hyperscale.logging")


class _ModuleDefaults(NamedTuple):
    """Snapshot of a single module's runtime defaults."""

    module_name: str
    clock: Clock | None
    random_source: Random | None
    filesystem: Filesystem | None
    system_resources: SystemResources | None


def _iter_production_modules() -> list[tuple[str, object]]:
    """Return ``(module_name, module_object)`` for every loaded module
    under the production prefixes. ``sys.modules`` mutates during
    iteration as side-effects of attribute access load lazy modules;
    snapshot the keys before iterating so the walk is stable.
    """
    snapshot = list(sys.modules.items())
    return [
        (name, mod)
        for name, mod in snapshot
        if (
            mod is not None
            and any(
                name == prefix or name.startswith(prefix + ".")
                for prefix in _PRODUCTION_PREFIXES
            )
        )
    ]


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
    touched: list[str] = []
    for name, mod in _iter_production_modules():
        rebound = False
        if clock is not None and hasattr(mod, "_DEFAULT_CLOCK"):
            setattr(mod, "_DEFAULT_CLOCK", clock)
            rebound = True
        if random_source is not None and hasattr(mod, "_DEFAULT_RANDOM"):
            setattr(mod, "_DEFAULT_RANDOM", random_source)
            rebound = True
        if filesystem is not None and hasattr(mod, "_DEFAULT_FILESYSTEM"):
            setattr(mod, "_DEFAULT_FILESYSTEM", filesystem)
        if system_resources is not None and hasattr(
            mod, "_DEFAULT_SYSTEM_RESOURCES"
        ):
            setattr(mod, "_DEFAULT_SYSTEM_RESOURCES", system_resources)
            rebound = True
        if rebound:
            touched.append(name)
    return touched


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
        )
        for name, mod in _iter_production_modules()
        if (
            hasattr(mod, "_DEFAULT_CLOCK")
            or hasattr(mod, "_DEFAULT_RANDOM")
            or hasattr(mod, "_DEFAULT_FILESYSTEM")
        )
    ]


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
        if entry.clock is not None and hasattr(mod, "_DEFAULT_CLOCK"):
            setattr(mod, "_DEFAULT_CLOCK", entry.clock)
        if entry.random_source is not None and hasattr(mod, "_DEFAULT_RANDOM"):
            setattr(mod, "_DEFAULT_RANDOM", entry.random_source)
        if entry.filesystem is not None and hasattr(
            mod, "_DEFAULT_FILESYSTEM"
        ):
            setattr(mod, "_DEFAULT_FILESYSTEM", entry.filesystem)
        if entry.system_resources is not None and hasattr(
            mod, "_DEFAULT_SYSTEM_RESOURCES"
        ):
            setattr(
                mod, "_DEFAULT_SYSTEM_RESOURCES", entry.system_resources
            )
