"""
Phase 6b guard test — forbids direct ``asyncio.create_task`` /
``asyncio.ensure_future`` calls in production code under
``hyperscale/`` (R-G66; it covered only ``hyperscale/distributed/``
before).

Paths under ``EXEMPT_PATH_PREFIXES`` are not scanned at all; every
other exemption is one file in the snapshot with a one-line reason.

The Phase 6 ``SimulationLoop`` guarantees deterministic task
scheduling for callbacks registered through the loop it owns —
``loop.call_soon``, ``loop.call_later``, and ``loop.create_task``.
A direct ``asyncio.create_task(coro)`` resolves to
``asyncio.get_running_loop()`` which works correctly *most* of the
time but couples the callsite to the running-loop context. Under
SIM, the harness installs a ``SimulationLoop`` and the running loop
*is* the right one — but any code path that bypasses the harness
(future helper additions, accidental task fire-and-forget under a
nested loop) silently lands on a different loop and breaks the
determinism contract.

Routing every callsite through ``self._task_runner.run`` or
``loop.create_task`` removes that coupling: the running-loop
context is captured at construction time (or passed explicitly)
rather than at task-creation time.

How the ratchet works
---------------------

Same shape as ``test_no_direct_time_random.py`` — the expected
violations snapshot lives in
``expected_asyncio_task_violations.py`` next to this file. Each
migration commit removes its file from the snapshot. The test
fails when discovered ≠ expected in either direction.

What's flagged
--------------

* ``asyncio.create_task(coro)`` — direct call to the asyncio
  module-level function.
* ``asyncio.ensure_future(coro)`` — same intent, alternate name.
* ``from asyncio import create_task; create_task(coro)`` — alias
  import form, caught via the ``ImportFrom`` walker that mirrors
  the Phase 5 lint.
* ``from asyncio import ensure_future; ensure_future(coro)`` —
  same.

What's NOT flagged
------------------

* ``loop.create_task(coro)`` — the loop-method form is exactly
  what we want callers to use; it's explicit about which loop the
  task lands on.
* ``self._task_runner.run(callable)`` — the TaskRunner is the
  blessed indirection.
* ``asyncio.gather(*awaitables)`` — gather creates tasks
  internally via ``loop.create_task`` (same loop as the caller's),
  so it doesn't introduce the running-loop coupling. Forbidding
  gather would require a much larger refactor for marginal
  correctness gain; the Phase 6 plan intentionally scopes the
  lint to raw task creation only.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator

from tests.simulation.lints.expected_asyncio_task_violations import (
    EXPECTED_ASYNCIO_TASK_VIOLATIONS,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale"


# Repo-relative path prefixes the lint does not scan, each with its reason.
EXEMPT_PATH_PREFIXES: dict[str, str] = {
    "hyperscale/core/jobs/": (
        "owned by a peer session; its raw-task sites (its own TaskRunner, "
        "the pool-leader protocols, the graph runners) migrate there"
    ),
}


# Attributes on the ``asyncio`` module that are forbidden as
# direct calls in production code.
FORBIDDEN_ATTRIBUTE_NAMES: frozenset[str] = frozenset({
    "create_task",
    "ensure_future",
})


# Names that, when brought into local scope via ``from asyncio
# import X``, become a forbidden bare-name call.
FORBIDDEN_FROM_IMPORT_NAMES: frozenset[str] = frozenset({
    "create_task",
    "ensure_future",
})


def _iter_python_files(root: Path) -> Iterator[Path]:
    """Yield every ``.py`` file under ``root`` excluding caches and the
    exempt path prefixes."""
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        if path.relative_to(REPO_ROOT).as_posix().startswith(tuple(EXEMPT_PATH_PREFIXES)):
            continue
        yield path


def _collect_forbidden_aliases(tree: ast.Module) -> set[str]:
    """Return the set of local names brought into scope from
    ``asyncio`` that map to a forbidden function.

    Captures the ``asname`` form too — ``from asyncio import
    create_task as spawn`` binds ``spawn``, and a later ``spawn(...)``
    must still be flagged.
    """
    aliases: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module != "asyncio":
            continue
        for alias in node.names:
            if alias.name in FORBIDDEN_FROM_IMPORT_NAMES:
                aliases.add(alias.asname or alias.name)
    return aliases


def _file_has_forbidden_call(path: Path) -> bool:
    """Return True when ``path`` contains a direct
    ``asyncio.create_task`` / ``asyncio.ensure_future`` call in
    either of the two patterns.
    """
    tree = ast.parse(path.read_text())
    forbidden_aliases = _collect_forbidden_aliases(tree)

    for node in ast.walk(tree):
        # Pattern 1: ``asyncio.create_task`` / ``asyncio.ensure_future``
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "asyncio"
            and node.attr in FORBIDDEN_ATTRIBUTE_NAMES
        ):
            return True

        # Pattern 2: bare ``Name`` reference to an aliased import.
        # Matches the captured-callable form too (someone holding
        # the function in a variable for later invocation).
        if (
            isinstance(node, ast.Name)
            and node.id in forbidden_aliases
            and not isinstance(node.ctx, ast.Store)
        ):
            return True

    return False


def _discover_violations() -> set[str]:
    """Walk the production tree; return repo-relative paths of
    files containing a raw asyncio task-creation call."""
    return {
        str(path.relative_to(REPO_ROOT))
        for path in _iter_python_files(PRODUCTION_ROOT)
        if _file_has_forbidden_call(path)
    }


def _load_expected_snapshot() -> set[str]:
    """Return the ratcheted allowlist as a mutable set view."""
    return set(EXPECTED_ASYNCIO_TASK_VIOLATIONS)


def test_asyncio_task_violation_set_matches_snapshot() -> None:
    """Phase 6b ratchet: discovered violations must equal the
    expected snapshot exactly.

    Regression direction (actual − expected): someone added a
    direct ``asyncio.create_task`` / ``ensure_future`` call;
    route it through ``self._task_runner.run`` (preferred) or
    ``loop.create_task`` (when the loop reference is at hand).

    Stale-entry direction (expected − actual): a migration removed
    the last direct call from a module but the snapshot wasn't
    updated. Remove the offending file paths from
    ``EXPECTED_ASYNCIO_TASK_VIOLATIONS`` in
    ``expected_asyncio_task_violations.py``, then commit.
    """
    actual = _discover_violations()
    expected = _load_expected_snapshot()

    regressions = sorted(actual - expected)
    stale = sorted(expected - actual)

    diagnostic_parts: list[str] = []
    if regressions:
        diagnostic_parts.append(
            "Regressions — new direct asyncio.create_task / "
            f"ensure_future calls in production ({len(regressions)} "
            "file(s)):"
        )
        diagnostic_parts.extend(f"  + {path}" for path in regressions)

    if stale:
        diagnostic_parts.append(
            "Stale snapshot entries — these files no longer contain "
            f"forbidden calls ({len(stale)} file(s)); remove them "
            "from EXPECTED_ASYNCIO_TASK_VIOLATIONS in "
            "expected_asyncio_task_violations.py:"
        )
        diagnostic_parts.extend(f"  - {path}" for path in stale)

    if diagnostic_parts:
        raise AssertionError("\n".join(diagnostic_parts))
