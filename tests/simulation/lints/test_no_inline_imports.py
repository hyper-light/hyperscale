"""
Ratchet lint -- imports belong at the top of the module (Phase 5): an
import inside a function hides a dependency edge, runs a lookup each call,
and usually papers over an import cycle that should be broken instead.

Imports under ``if TYPE_CHECKING:`` are module-level and never counted.
Today's inline imports are snapshotted per function: none may be added,
and each one hoisted lowers its entry.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_inline_import_violations import EXPECTED_INLINE_IMPORT_VIOLATIONS
from tests.simulation.lints.ratchet import (
    REPO_ROOT,
    functions_with_qualnames,
    own_nodes,
    production_modules,
    ratchet_failures,
    write_snapshot,
)


def discover() -> dict[str, int]:
    discovered: dict[str, int] = {}
    for path, module in production_modules():
        for qualname, function in functions_with_qualnames(module):
            if (count := sum(isinstance(node, (ast.Import, ast.ImportFrom)) for node in own_nodes(function))) > 0:
                site = f"{path}::{qualname}"
                discovered[site] = discovered.get(site, 0) + count
    return discovered


def test_no_function_imports() -> None:
    failures = ratchet_failures(discover(), EXPECTED_INLINE_IMPORT_VIOLATIONS, __name__)
    assert not failures, "Imports inside functions:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_inline_import_violations.py",
        "EXPECTED_INLINE_IMPORT_VIOLATIONS",
        "Imports inside functions, per function, today (test_no_inline_imports).",
        discover(),
    )
