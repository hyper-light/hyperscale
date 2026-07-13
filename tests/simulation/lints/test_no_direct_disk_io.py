"""
Phase 7 guard test — no production module under
``hyperscale/distributed/`` or ``hyperscale/logging/`` may touch the
disk directly: every durable write and recovery read must route
through the ``Filesystem`` seam (``hyperscale/core/runtime``), so SIM
mode can substitute the in-memory filesystem and inject storage
faults (slow disk / disk full / fsync reordering / crash-torn writes),
and so no disk call can ever block the event loop again.

What's flagged (AST patterns, import-alias aware like the time/random
lint):

1. The builtin ``open(...)`` call (``ast.Call`` on the bare name).
   Method DEFINITIONS named ``open`` are not calls and don't match.
2. ``os.X`` / ``tempfile.X`` disk functions — fsync, fdopen, rename,
   replace, mkdir(s), unlink/remove, rmdir, truncate, mkstemp,
   mkdtemp, temporary files — including through ``import os as x``
   aliases, and ``os.path.exists`` / ``os.path.getsize`` through the
   two-level chain.
3. Pathlib-distinctive method calls on ANY receiver — write_text,
   write_bytes, read_text, read_bytes, touch, unlink, rmdir, mkdir,
   rename, exists, iterdir, glob — EXCEPT when the receiver is
   filesystem-named (``self._filesystem`` / ``self.filesystem`` /
   ``filesystem`` / ``_DEFAULT_FILESYSTEM``): those ARE the seam.
   Receiver-name convention is the carve-out, so the seam's own
   ``mkdir`` / ``exists`` calls pass while ``self._path.parent.mkdir``
   (the exact shape of a bug this campaign fixed in WALWriter) fails.

The ratcheted snapshot lives in ``expected_disk_violations.py`` and may
only shrink. The hard-durability components are additionally asserted
CLEAN by name — they must never reappear in the snapshot.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.simulation.lints.expected_disk_violations import (
    EXPECTED_DISK_VIOLATIONS,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOTS = (
    REPO_ROOT / "hyperscale" / "distributed",
    REPO_ROOT / "hyperscale" / "logging",
)

# Components whose durability contracts this campaign seamed — they must
# stay clean forever, never merely "in the snapshot".
_MUST_STAY_CLEAN = frozenset(
    {
        "hyperscale/distributed/ledger/wal/wal_writer.py",
        "hyperscale/distributed/ledger/wal/node_wal.py",
        "hyperscale/distributed/raft/raft_wal.py",
        "hyperscale/distributed/idempotency/manager_ledger.py",
        "hyperscale/distributed/swim/detection/incarnation_store.py",
        "hyperscale/distributed/ledger/checkpoint/checkpoint.py",
        "hyperscale/distributed/ledger/archive/job_archive_store.py",
    }
)

_FORBIDDEN_OS_ATTRIBUTES = frozenset(
    {
        "fsync",
        "fdopen",
        "rename",
        "replace",
        "mkdir",
        "makedirs",
        "unlink",
        "remove",
        "rmdir",
        "truncate",
        "ftruncate",
    }
)

_FORBIDDEN_OS_PATH_ATTRIBUTES = frozenset({"exists", "getsize"})

_FORBIDDEN_TEMPFILE_ATTRIBUTES = frozenset(
    {
        "mkstemp",
        "mkdtemp",
        "NamedTemporaryFile",
        "TemporaryFile",
        "TemporaryDirectory",
    }
)

# Pathlib-distinctive methods; flagged on any receiver except the
# filesystem-named seam receivers below.
_FORBIDDEN_PATH_METHODS = frozenset(
    {
        "write_text",
        "write_bytes",
        "read_text",
        "read_bytes",
        "touch",
        "unlink",
        "rmdir",
        "mkdir",
        "rename",
        "exists",
        "iterdir",
        "glob",
    }
)

# Receiver-name convention: any terminal identifier ENDING in
# "filesystem" (case-insensitive) is the seam — self._filesystem,
# filesystem, _DEFAULT_FILESYSTEM, self._storage_filesystem.
def _is_seam_receiver(terminal_name: str | None) -> bool:
    return (
        terminal_name is not None
        and terminal_name.lower().endswith("filesystem")
    )


def _iter_python_files() -> list[Path]:
    return [
        path
        for root in PRODUCTION_ROOTS
        for path in sorted(root.rglob("*.py"))
        if "__pycache__" not in path.parts
    ]


def _module_aliases(tree: ast.Module) -> dict[str, str]:
    """Local alias -> real module for ``os`` / ``tempfile`` imports."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in ("os", "tempfile", "os.path"):
                    aliases[alias.asname or alias.name.split(".")[0]] = (
                        alias.name.split(".")[0]
                    )
    return aliases


def _receiver_terminal_name(node: ast.expr) -> str | None:
    """The last identifier of a receiver expression: ``self._filesystem``
    -> ``_filesystem``; ``filesystem`` -> ``filesystem``."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _source_has_forbidden_disk_call(source: str) -> bool:
    tree = ast.parse(source)
    aliases = _module_aliases(tree)

    for node in ast.walk(tree):
        # Pattern 1: the builtin open(...) CALL.
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "open"
        ):
            return True

        if not isinstance(node, ast.Attribute):
            continue

        receiver = node.value

        # Pattern 2: os.X / tempfile.X (alias-resolved), os.path.X.
        if isinstance(receiver, ast.Name):
            real_module = aliases.get(receiver.id)
            if real_module == "os" and node.attr in _FORBIDDEN_OS_ATTRIBUTES:
                return True
            if (
                real_module == "tempfile"
                and node.attr in _FORBIDDEN_TEMPFILE_ATTRIBUTES
            ):
                return True
        if (
            isinstance(receiver, ast.Attribute)
            and isinstance(receiver.value, ast.Name)
            and aliases.get(receiver.value.id) == "os"
            and receiver.attr == "path"
            and node.attr in _FORBIDDEN_OS_PATH_ATTRIBUTES
        ):
            return True

        # Pattern 3: pathlib-distinctive methods on non-seam receivers.
        if node.attr in _FORBIDDEN_PATH_METHODS:
            if _is_seam_receiver(_receiver_terminal_name(receiver)):
                continue
            # ``os.path.exists`` handled above; a bare module receiver
            # resolving to os/tempfile is not a pathlib method.
            if isinstance(receiver, ast.Name) and receiver.id in aliases:
                continue
            return True

    return False


def _discover_violations() -> set[str]:
    return {
        str(path.relative_to(REPO_ROOT))
        for path in _iter_python_files()
        if _source_has_forbidden_disk_call(path.read_text())
    }


def test_disk_violation_set_matches_snapshot() -> None:
    actual = _discover_violations()
    expected = set(EXPECTED_DISK_VIOLATIONS)

    regressions = sorted(actual - expected)
    stale = sorted(expected - actual)

    diagnostics: list[str] = []
    if regressions:
        diagnostics.append(
            "Regressions — new direct disk IO outside the Filesystem "
            f"seam ({len(regressions)} file(s)):"
        )
        diagnostics.extend(f"  + {path}" for path in regressions)
    if stale:
        diagnostics.append(
            "Stale snapshot entries — no longer contain direct disk IO "
            f"({len(stale)} file(s)); remove from "
            "EXPECTED_DISK_VIOLATIONS:"
        )
        diagnostics.extend(f"  - {path}" for path in stale)

    if diagnostics:
        raise AssertionError("\n".join(diagnostics))


def test_hard_durability_components_stay_clean() -> None:
    """The seamed durability components must never reappear even in the
    snapshot — cleanliness here is a structural invariant, not a
    ratchet entry."""
    dirty = _MUST_STAY_CLEAN & (
        _discover_violations() | set(EXPECTED_DISK_VIOLATIONS)
    )
    assert not dirty, (
        f"hard-durability components regressed to raw disk IO: {sorted(dirty)}"
    )


# Detection self-tests — pin flagged and allowed patterns so an AST
# refactor can't silently blind the guard.
_FLAGGED_SNIPPETS: tuple[str, ...] = (
    "open('/tmp/x', 'ab')\n",
    "import os\nos.fsync(fd)\n",
    "import os\nos.rename(a, b)\n",
    "import os as sy\nsy.fdopen(fd, 'wb')\n",
    "import os\nos.path.exists(p)\n",
    "import tempfile\ntempfile.mkstemp(dir=d)\n",
    "path.write_text(data)\n",
    "self._path.parent.mkdir(parents=True)\n",  # the WALWriter bug shape
    "resolved.read_bytes()\n",
    "p.glob('checkpoint_*.bin')\n",
)

_ALLOWED_SNIPPETS: tuple[str, ...] = (
    # The seam itself — any filesystem-suffixed receiver.
    "await self._filesystem.mkdir(p, parents=True, exist_ok=True)\n",
    "await self.filesystem.exists(p)\n",
    "await _DEFAULT_FILESYSTEM.read_bytes(p)\n",
    "await filesystem.atomic_write(p, data)\n",
    "await self._storage_filesystem.remove(p)\n",
    # Method definitions named open are not calls.
    "class W:\n    async def open(self):\n        return None\n",
    # Non-disk os usage.
    "import os\nos.getcwd()\n",
    "import os\nos.path.join(a, b)\n",
    "import os\nos.environ.get('X')\n",
    # str.replace is not os.replace / Path.replace.
    "name.replace(':', '_')\n",
)


def test_lint_flags_direct_disk_io() -> None:
    for snippet in _FLAGGED_SNIPPETS:
        assert _source_has_forbidden_disk_call(snippet), snippet


def test_lint_allows_seam_and_non_disk_calls() -> None:
    for snippet in _ALLOWED_SNIPPETS:
        assert not _source_has_forbidden_disk_call(snippet), snippet
