"""
Phase 5 guard test — enforces the exit criterion in
``docs/dev/simulation_framework.md:868-870``: no production module
under ``hyperscale/distributed/`` may read wall/monotonic time
(``time.monotonic`` / ``time.time`` / ``time.monotonic_ns`` /
``time.time_ns``), schedule via ``asyncio.sleep`` / ``asyncio.wait_for``,
draw non-crypto randomness (ANY ``random.X`` — the whole module), or
mint a non-deterministic id (``uuid.uuid4`` / ``uuid.uuid1``) directly.
Every such call must route through a ``Clock`` / ``Random`` seam (or a
deterministic id source like the clock-seamed ``SnowflakeGenerator``) so
Phase 6 SIM mode can swap the backing implementation and replay is
byte-identical.

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
   ``random.getrandbits(...)``, ``uuid.uuid4()``. ``ast.Attribute(value=
   ast.Name)`` where the left name — resolved through any import alias
   (``import time as wall`` → ``wall.monotonic``) — is a whole-module-
   forbidden module (any attribute) or forms a forbidden (module, attr)
   pair. Also catches captured-callable forms like
   ``field(default_factory=time.monotonic)``.

2. **From-import** form — ``from time import monotonic; monotonic()``,
   ``from asyncio import sleep; await sleep(...)``, ``from uuid import
   uuid4; uuid4()``, and ``from random import <anything>`` (every name,
   since the module is wholly forbidden). The walker tracks the names
   imported into each file and flags ``ast.Name`` references to them.
   This closes the alias-import bypass that hid ``snowflake_generator.py``
   from earlier passes.

Why whole-module for ``random``: the earlier version enumerated six
seam-mirrored methods (uniform/random/randrange/choice/choices/sample),
so ``getrandbits`` / ``shuffle`` / ``randint`` were never flagged and a
real ``random.getrandbits`` id-nonce slipped through. A blocklist that
must track the seam's method set is a hole generator; the whole-module
block is correct by construction.

What's NOT flagged
------------------

* ``secrets.X`` and ``os.urandom`` — cryptographic randomness, out of
  scope. Logical ids that once leaned on these now use deterministic
  seams, so the remaining ``secrets`` calls are genuine crypto.
* ``uuid.UUID(...)`` (parsing), ``uuid.uuid3`` / ``uuid.uuid5``
  (namespace hashing) — deterministic, allowed; only the random/time
  generators ``uuid4`` / ``uuid1`` are banned.
* ``random.Random`` / ``random.SystemRandom`` — the generator CLASSES.
  An explicitly-constructed, independently-seedable RNG is the
  deterministic building block, not a hole; only the module-level
  global-state functions are banned.
* ``time.perf_counter`` — measurement only, doesn't affect SIM control
  flow. Producer modules can keep using it.
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


# Modules where EVERY attribute access is forbidden in production —
# there is no determinism-safe direct call, so the block is the whole
# module rather than an enumerated method list. ``random``: all
# non-crypto randomness must route through the ``Random`` seam. The
# earlier enumerated allowlist (uniform/random/randrange/choice/
# choices/sample) let ``getrandbits`` / ``shuffle`` / ``randint`` slip
# straight through — a blocklist that had to be kept in lockstep with
# the seam's method set and silently didn't. Blocking the module closes
# that class of gap by construction.
FORBIDDEN_WHOLE_MODULES: frozenset[str] = frozenset({"random"})


# Attributes that ARE allowed on an otherwise whole-module-forbidden
# module. ``random.Random`` / ``random.SystemRandom`` are the generator
# CLASSES — an explicitly-constructed, independently-seedable RNG is the
# deterministic building block (the SIM ``Random`` seam is itself built
# on ``random.Random``), not the hole. The hole is the module-level
# GLOBAL-STATE functions — ``random.random`` / ``uniform`` /
# ``getrandbits`` / ``shuffle`` / ``randint`` — which this still bans.
# (A ``random.Random()`` left unseeded is a determinism bug the tests /
# review catch; the lint can't see the seed statically.)
_WHOLE_MODULE_ALLOWED_ATTRS: dict[str, frozenset[str]] = {
    "random": frozenset({"Random", "SystemRandom"}),
}


# Pairs of (module_name, attribute) that are forbidden as direct calls
# in production code — for modules that ALSO have legitimate uses (so a
# whole-module block would over-reach). ``time.perf_counter`` stays
# allowed (measurement); ``asyncio`` has many non-time uses; ``uuid``
# keeps ``UUID``/``uuid3``/``uuid5`` (deterministic parse / namespace
# hashing) but bans the non-deterministic generators. ``monotonic_ns`` /
# ``time_ns`` join ``monotonic`` / ``time`` — id-generation sites read
# them directly for token uniqueness, which is just as un-seamed.
FORBIDDEN_ATTRIBUTE_CALLS: frozenset[tuple[str, str]] = frozenset({
    ("time", "monotonic"),
    ("time", "time"),
    ("time", "monotonic_ns"),
    ("time", "time_ns"),
    ("asyncio", "sleep"),
    ("asyncio", "wait_for"),
    ("uuid", "uuid4"),
    ("uuid", "uuid1"),
})


# Per-module mapping of attribute names that are forbidden when brought
# into scope via ``from <module> import <name>``. Mirrors
# ``FORBIDDEN_ATTRIBUTE_CALLS``; ``random`` is intentionally absent —
# it is a whole-module block, so every name from ``random`` is forbidden
# (handled in ``_collect_forbidden_aliases``).
FORBIDDEN_FROM_IMPORT_NAMES: dict[str, frozenset[str]] = {
    "time": frozenset({"monotonic", "time", "monotonic_ns", "time_ns"}),
    "asyncio": frozenset({"sleep", "wait_for"}),
    "uuid": frozenset({"uuid4", "uuid1"}),
}


# Modules whose local import aliases we track so that ``import time as
# wall`` / ``import random as rng`` cannot bypass the attribute check.
_ALIAS_TRACKED_MODULES: frozenset[str] = (
    frozenset({"time", "asyncio", "uuid"}) | FORBIDDEN_WHOLE_MODULES
)


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
        if node.module in FORBIDDEN_WHOLE_MODULES:
            # ``from random import <anything>`` — every imported name is
            # forbidden EXCEPT the allowed generator classes
            # (``from random import Random`` is fine).
            allowed = _WHOLE_MODULE_ALLOWED_ATTRS.get(node.module, frozenset())
            for alias in node.names:
                if alias.name not in allowed:
                    aliases.add(alias.asname or alias.name)
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


def _collect_module_aliases(tree: ast.Module) -> dict[str, str]:
    """Map each local module alias to its real module name for the
    modules this lint tracks, so an aliased import can't bypass the
    attribute check.

    ``import time as wall`` → ``{"wall": "time"}``; plain ``import
    random`` → ``{"random": "random"}``. Walks the whole tree (not just
    module scope) so a nested ``import time as t`` is caught too.
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Import):
            continue
        for alias in node.names:
            if alias.name in _ALIAS_TRACKED_MODULES:
                aliases[alias.asname or alias.name] = alias.name
    return aliases


def _source_has_forbidden_call(source: str) -> bool:
    """Return True when ``source`` contains at least one forbidden
    direct call, in either of the two patterns documented in the
    module docstring.

    Pattern 1 (attribute-access): ``time.monotonic()``,
    ``asyncio.sleep(...)``, ``random.getrandbits(...)``, ``uuid.uuid4()``
    — ``ast.Attribute(value=ast.Name)`` where the left name (resolved
    through import aliases) is a whole-module-forbidden module on a
    non-allowed attribute, or forms a forbidden (module, attr) pair.
    Also matches captured-callable forms
    (``field(default_factory=time.monotonic)``).

    Pattern 2 (from-import aliasing): ``from time import time``
    binds ``time`` as a bare name, then ``time()`` is a
    ``ast.Call(func=ast.Name)`` whose name is in the alias set
    returned by ``_collect_forbidden_aliases``. The dataclass
    ``field(default_factory=monotonic)`` form is also caught because
    the bare ``monotonic`` references as ``ast.Name`` outside a
    ``ast.Call`` — we match ``Name`` nodes broadly, not only inside
    ``Call``.
    """
    tree = ast.parse(source)

    forbidden_aliases = _collect_forbidden_aliases(tree)
    module_aliases = _collect_module_aliases(tree)

    for node in ast.walk(tree):
        # Pattern 1: ``time.monotonic``, ``asyncio.sleep``, ``random.X``,
        # ``uuid.uuid4`` — resolving the left name through any import
        # alias (``import time as wall`` → ``wall.monotonic``). A
        # whole-module-forbidden module fails on ANY attribute; other
        # modules fail only on the enumerated (module, attr) pairs.
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            real_module = module_aliases.get(node.value.id, node.value.id)
            if real_module in FORBIDDEN_WHOLE_MODULES:
                allowed = _WHOLE_MODULE_ALLOWED_ATTRS.get(real_module, frozenset())
                if node.attr not in allowed:
                    return True
            if (real_module, node.attr) in FORBIDDEN_ATTRIBUTE_CALLS:
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


def _file_has_forbidden_call(path: Path) -> bool:
    """Return True when ``path``'s source contains a forbidden call.

    Thin wrapper over ``_source_has_forbidden_call``. A SyntaxError here
    means the file uses syntax newer than the interpreter running this
    test — a stale interpreter, not the file's fault — so it propagates
    loudly rather than silently skipping the file.
    """
    return _source_has_forbidden_call(path.read_text())


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


# Detection self-tests — pin that the guard actually catches the
# patterns it claims to (and allows the deterministic building blocks),
# so a future edit to the AST logic can't silently blind it. Each
# snippet is parseable module source; undefined names are fine (never
# executed).
_FLAGGED_SNIPPETS: tuple[str, ...] = (
    # The class of gap the enumerated random allowlist used to miss.
    "import random\nrandom.getrandbits(32)\n",
    "import random\nrandom.shuffle([1, 2])\n",
    "import random\nrandom.randint(0, 9)\n",
    "import random\nrandom.uniform(0, 1)\n",
    # Non-deterministic id generators.
    "import uuid\nuuid.uuid4()\n",
    "import uuid\nuuid.uuid1()\n",
    # Nanosecond time reads that used to slip past the lint.
    "import time\ntime.monotonic_ns()\n",
    "import time\ntime.time_ns()\n",
    # Module-alias bypass (the lamport_clock ``import time as t`` hole).
    "import time as t\nt.monotonic()\n",
    "import random as rng\nrng.getrandbits(8)\n",
    # From-import forms.
    "from random import shuffle\nshuffle([1])\n",
    "from uuid import uuid4\nuuid4()\n",
    "import asyncio\nasyncio.sleep(1)\n",
)

_ALLOWED_SNIPPETS: tuple[str, ...] = (
    # Seeded / crypto generator CLASSES are the building blocks, not holes.
    "import random\nrandom.Random(0)\n",
    "import random\nrandom.SystemRandom()\n",
    "from random import Random\nRandom(0)\n",
    # Deterministic uuid uses.
    "import uuid\nuuid.uuid3(ns, 'x')\n",
    "import uuid\nuuid.UUID('abc')\n",
    # Measurement clock, explicitly allowed.
    "import time\ntime.perf_counter()\n",
    # Seam usage — attribute on ``self`` / the module singleton, not on a
    # forbidden module name.
    "self._random.getrandbits(32)\n",
    "self._clock.monotonic_ns()\n",
    "_DEFAULT_RANDOM.sample(pop, 3)\n",
)


def test_lint_flags_nondeterministic_calls() -> None:
    for snippet in _FLAGGED_SNIPPETS:
        assert _source_has_forbidden_call(snippet), (
            f"detection must flag this non-deterministic call:\n{snippet}"
        )


def test_lint_allows_deterministic_and_seam_calls() -> None:
    for snippet in _ALLOWED_SNIPPETS:
        assert not _source_has_forbidden_call(snippet), (
            f"detection must NOT flag this deterministic / seam call:\n{snippet}"
        )
