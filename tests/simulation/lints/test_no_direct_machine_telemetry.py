"""
Determinism guard — no production module under
``hyperscale/distributed/`` may read machine telemetry (psutil)
directly: memory, CPU counts, and utilization are environmental
non-determinism inputs exactly like the wall clock, the RNG seed,
hash randomization, and the disk. Every read routes through the
``SystemResources`` seam (``hyperscale/core/runtime``) so SIM binds a
CONSTANT machine.

Found the hard way: SIM replay twins diverged only when earlier runs
in the same test process had shifted the host's available memory — the
live ``psutil.virtual_memory()`` reads rode into worker registration
payloads and flipped downstream scheduling by one poll quantum. Real
telemetry makes "identical seed, identical schedule" impossible by
construction.

Flags ``import psutil`` / ``from psutil import ...`` and any
``psutil.X`` attribute use (alias-aware). String literals (e.g. module
allowlists in the restricted unpickler) never match — this is an AST
lint. The ratcheted snapshot may only shrink; entries must be
REAL-mode-only telemetry that SIM provably never executes.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale" / "distributed"

# REAL-mode-only telemetry, justified:
# - process_resource_monitor: per-process CPU/RSS sampling for the
#   worker's resource loop — telemetry-gated OFF under SIM (the
#   monitors never start), and per-process introspection has no
#   deterministic analog worth modeling.
EXPECTED_TELEMETRY_VIOLATIONS: frozenset[str] = frozenset(
    {
        "hyperscale/distributed/resources/process_resource_monitor.py",
    }
)


def _source_reads_psutil(source: str) -> bool:
    tree = ast.parse(source)
    aliases: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "psutil" or alias.name.startswith("psutil."):
                    return True
        if isinstance(node, ast.ImportFrom):
            if node.module and (
                node.module == "psutil" or node.module.startswith("psutil.")
            ):
                return True
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in aliases
        ):
            return True

    return False


def _discover_violations() -> set[str]:
    return {
        str(path.relative_to(REPO_ROOT))
        for path in sorted(PRODUCTION_ROOT.rglob("*.py"))
        if "__pycache__" not in path.parts
        and _source_reads_psutil(path.read_text())
    }


def test_telemetry_violation_set_matches_snapshot() -> None:
    actual = _discover_violations()
    expected = set(EXPECTED_TELEMETRY_VIOLATIONS)

    regressions = sorted(actual - expected)
    stale = sorted(expected - actual)

    diagnostics: list[str] = []
    if regressions:
        diagnostics.append(
            "Regressions — direct machine-telemetry reads outside the "
            f"SystemResources seam ({len(regressions)} file(s)):"
        )
        diagnostics.extend(f"  + {path}" for path in regressions)
    if stale:
        diagnostics.append(
            "Stale snapshot entries — remove from "
            f"EXPECTED_TELEMETRY_VIOLATIONS ({len(stale)} file(s)):"
        )
        diagnostics.extend(f"  - {path}" for path in stale)

    if diagnostics:
        raise AssertionError("\n".join(diagnostics))


# Detection self-tests.
_FLAGGED_SNIPPETS: tuple[str, ...] = (
    "import psutil\n",
    "from psutil import virtual_memory\n",
    "import psutil as machine\n",
)

_ALLOWED_SNIPPETS: tuple[str, ...] = (
    # The seam.
    "memory = _DEFAULT_SYSTEM_RESOURCES.available_memory_bytes()\n",
    # String literals (unpickler allowlists) are not reads.
    "ALLOWED_MODULES = ('psutil', 'psutil.something')\n",
)


def test_lint_flags_psutil_reads() -> None:
    for snippet in _FLAGGED_SNIPPETS:
        assert _source_reads_psutil(snippet), snippet


def test_lint_allows_seam_and_literals() -> None:
    for snippet in _ALLOWED_SNIPPETS:
        assert not _source_reads_psutil(snippet), snippet
