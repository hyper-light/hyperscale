"""
Phase 5 guard test — enforces the exit criterion in
``docs/dev/simulation_framework.md:868-870``: no production module
under ``hyperscale/distributed/`` may call ``time.monotonic`` /
``time.time`` / ``asyncio.sleep`` / ``asyncio.wait_for`` / non-crypto
``random.X`` directly. Every such call must route through a
``Clock`` or ``Random`` on ``self`` so Phase 6 SIM mode can swap the
backing implementation.

How the ratchet works
---------------------

The expected-violations snapshot lives in
``expected_runtime_violations.py`` next to this file (Python module,
not text, so the project's ``*.txt`` gitignore doesn't hide it).
It records every production source path that currently uses one of
the forbidden calls. The test fails when the discovered set differs
from the expected set in *either* direction:

* **Regression** — a file appears in actual but not expected:
  someone added a direct ``time.X`` / ``asyncio.sleep`` / ``random.X``
  call that should have used ``self._clock`` / ``self._random``.

* **Stale entry** — a file appears in expected but not actual: a
  Phase 5b–5d migration removed the last direct call from a module
  but left the path in the snapshot. The snapshot must shrink
  monotonically; every migration commit tightens the lint.

Phase 5 final state (after 5e): the snapshot contains only the two
seam-adapter files (``runtime/real_clock.py``,
``runtime/real_random.py``) that are intentionally allowed to
delegate to the stdlib.

What's flagged
--------------

Two AST patterns are detected, NOT just one:

1. **Attribute-access** form — ``time.monotonic()``, ``asyncio.sleep(...)``,
   ``random.uniform(...)``. ``ast.Attribute(value=ast.Name)`` where the
   module name matches the forbidden set. Also catches captured-callable
   forms like ``field(default_factory=time.monotonic)``.

2. **From-import** form — ``from time import monotonic; monotonic()``,
   ``from asyncio import sleep, wait_for; await sleep(...)``,
   ``from random import uniform; uniform(0, 1)``. The lint walker
   tracks the names imported into each file from the forbidden modules
   and flags ``ast.Call(func=ast.Name)`` where the name matches a
   tracked import. This closes the alias-import bypass that hid
   ``snowflake_generator.py`` from earlier passes.

What's NOT flagged
------------------

* ``secrets.X`` and ``os.urandom`` — cryptographic randomness,
  out of scope.
* ``time.perf_counter`` — measurement only, doesn't affect SIM
  control flow. Producer modules can keep using it.
* ``LamportClock`` and similar logical clocks — not wall time.
* ``from secrets import token_bytes; token_bytes(8)`` — same crypto
  carve-out as the attribute form.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator

from tests.simulation.lints.expected_runtime_violations import (
    EXPECTED_RUNTIME_VIOLATIONS,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale" / "distributed"


# Pairs of (module_name, attribute) that are forbidden as direct
# calls in production code.
FORBIDDEN_ATTRIBUTE_CALLS: frozenset[tuple[str, str]] = frozenset({
    ("time", "monotonic"),
    ("time", "time"),
    ("asyncio", "sleep"),
    ("asyncio", "wait_for"),
    ("random", "uniform"),
    ("random", "random"),
    ("random", "randrange"),
    ("random", "choice"),
    ("random", "choices"),
    ("random", "sample"),
})


# Per-module mapping of attribute names that are forbidden when
# brought into scope via ``from <module> import <name>``. Reuses the
# same forbidden set above; the index here just groups by source
# module so the ``ImportFrom`` walker can resolve aliases quickly.
FORBIDDEN_FROM_IMPORT_NAMES: dict[str, frozenset[str]] = {
    "time": frozenset({"monotonic", "time"}),
    "asyncio": frozenset({"sleep", "wait_for"}),
    "random": frozenset({"uniform", "random", "randrange",
                         "choice", "choices", "sample"}),
}


def _iter_python_files(root: Path) -> Iterator[Path]:
    """Yield every ``.py`` file under ``root`` excluding caches."""
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def _collect_forbidden_aliases(tree: ast.Module) -> set[str]:
    """Walk module-level ``ImportFrom`` nodes and return the set of
    local names brought into scope from ``time`` / ``asyncio`` /
    ``random`` that map to a forbidden attribute.

    Captures the ``asname`` form too — ``from time import time as wall_now``
    binds ``wall_now``, and a later ``wall_now()`` must still be
    flagged. Only inspects top-level imports; in-function ``from X
    import Y`` is rare and intentionally not tracked (any production
    code doing that under Phase 5 is the bug to surface).
    """
    aliases: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module not in FORBIDDEN_FROM_IMPORT_NAMES:
            continue
        forbidden_names = FORBIDDEN_FROM_IMPORT_NAMES[node.module]
        for alias in node.names:
            if alias.name in forbidden_names:
                # ``from time import time as wall_now`` → ``wall_now``;
                # plain ``from time import time`` → ``time``.
                aliases.add(alias.asname or alias.name)
    return aliases


def _file_has_forbidden_call(path: Path) -> bool:
    """Return True when ``path`` contains at least one forbidden
    direct call, in either of the two patterns documented in the
    module docstring.

    Pattern 1 (attribute-access): ``time.monotonic()``,
    ``asyncio.sleep(...)``, ``random.uniform(...)`` — surfaces as
    ``ast.Attribute(value=ast.Name)`` where the pair is in
    ``FORBIDDEN_ATTRIBUTE_CALLS``. Also matches captured-callable
    forms (``field(default_factory=time.monotonic)``,
    ``self._now = time.monotonic``) which the Phase 5 plan calls out.

    Pattern 2 (from-import aliasing): ``from time import time``
    binds ``time`` as a bare name, then ``time()`` is a
    ``ast.Call(func=ast.Name)`` whose name is in the alias set
    returned by ``_collect_forbidden_aliases``. The dataclass
    ``field(default_factory=monotonic)`` form is also caught because
    the bare ``monotonic`` references as ``ast.Name`` outside a
    ``ast.Call`` — we match ``Name`` nodes broadly, not only inside
    ``Call``.
    """
    # SyntaxError here means the file uses syntax newer than the
    # interpreter running this test — typically a stale interpreter,
    # not the file's fault. Re-raise so the failure is loud rather
    # than silently skipping the file.
    tree = ast.parse(path.read_text())

    forbidden_aliases = _collect_forbidden_aliases(tree)

    for node in ast.walk(tree):
        # Pattern 1: ``time.monotonic``, ``asyncio.sleep``, ``random.uniform``
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            key = (node.value.id, node.attr)
            if key in FORBIDDEN_ATTRIBUTE_CALLS:
                return True

        # Pattern 2: bare ``Name`` references to an aliased import.
        # We flag any ``Name`` (not only inside ``Call``) because
        # ``field(default_factory=monotonic)`` captures the callable
        # without an immediate ``Call`` — same regression the plan
        # called out for ``field(default_factory=time.monotonic)``.
        if (
            isinstance(node, ast.Name)
            and node.id in forbidden_aliases
            and not isinstance(node.ctx, ast.Store)
        ):
            return True

    return False


def _discover_violations() -> set[str]:
    """Walk the production tree; return repo-relative paths of
    files containing at least one forbidden call."""
    return {
        str(path.relative_to(REPO_ROOT))
        for path in _iter_python_files(PRODUCTION_ROOT)
        if _file_has_forbidden_call(path)
    }


def _load_expected_snapshot() -> set[str]:
    """Return the ratcheted allowlist as a mutable set view."""
    return set(EXPECTED_RUNTIME_VIOLATIONS)


def test_runtime_violation_set_matches_snapshot() -> None:
    """Phase 5 ratchet: discovered violations must exactly equal the
    expected snapshot.

    Regression direction (actual − expected): someone added a direct
    ``time.X`` / ``asyncio.sleep`` / non-crypto ``random.X`` call;
    refactor it to go through ``self._clock`` / ``self._random``.

    Stale-entry direction (expected − actual): a migration removed
    the last direct call from a module but the snapshot wasn't
    updated. Remove the offending file paths from
    ``EXPECTED_RUNTIME_VIOLATIONS`` in
    ``tests/simulation/lints/expected_runtime_violations.py``, then
    commit.
    """
    actual = _discover_violations()
    expected = _load_expected_snapshot()

    regressions = sorted(actual - expected)
    stale = sorted(expected - actual)

    diagnostic_parts: list[str] = []
    if regressions:
        diagnostic_parts.append(
            "Regressions — new direct time/asyncio.sleep/random.X "
            f"calls in production ({len(regressions)} file(s)):"
        )
        diagnostic_parts.extend(f"  + {path}" for path in regressions)

    if stale:
        diagnostic_parts.append(
            "Stale snapshot entries — these files no longer contain "
            f"forbidden calls ({len(stale)} file(s)); remove them "
            "from EXPECTED_RUNTIME_VIOLATIONS in "
            "expected_runtime_violations.py:"
        )
        diagnostic_parts.extend(f"  - {path}" for path in stale)

    if diagnostic_parts:
        raise AssertionError("\n".join(diagnostic_parts))


def test_runtime_module_is_clean() -> None:
    """``hyperscale/distributed/runtime/`` must never re-acquire a
    direct time/asyncio.sleep/random call outside the two real-impl
    files. This is the structural invariant that defines the seam."""
    runtime_root = PRODUCTION_ROOT / "runtime"
    allowed = {
        str((runtime_root / "real_clock.py").relative_to(REPO_ROOT)),
        str((runtime_root / "real_random.py").relative_to(REPO_ROOT)),
    }

    offenders = sorted(
        str(path.relative_to(REPO_ROOT))
        for path in _iter_python_files(runtime_root)
        if _file_has_forbidden_call(path)
        and str(path.relative_to(REPO_ROOT)) not in allowed
    )

    if offenders:
        raise AssertionError(
            "Files under hyperscale/distributed/runtime/ may not "
            "call time.X / asyncio.sleep / random.X directly outside "
            f"the real-impl files ({sorted(allowed)}). Offenders:\n"
            + "\n".join(f"  - {path}" for path in offenders)
        )
