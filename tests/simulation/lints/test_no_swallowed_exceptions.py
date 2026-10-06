"""
Ratchet lint -- no ``except`` whose whole body is ``pass`` (CLAUDE.md: "We
*do not* EVER swallow errors").

Some swallows are deliberate guards -- teardown paths that run mid-
cancellation, where a caller needs abort never to raise -- and each is
classified one by one (Phase 7), never in bulk. Until then today's are
snapshotted per function: none may be added, and each one turned into a
log, a raise or a documented guard lowers its entry.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_swallowed_exception_violations import (
    EXPECTED_SWALLOWED_EXCEPTION_VIOLATIONS,
)
from tests.simulation.lints.ratchet import (
    REPO_ROOT,
    functions_with_qualnames,
    own_nodes,
    production_modules,
    ratchet_failures,
    write_snapshot,
)


def _swallows(node: ast.AST) -> bool:
    return isinstance(node, ast.ExceptHandler) and all(
        isinstance(statement, ast.Pass)
        or (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant))
        for statement in node.body
    )


def discover() -> dict[str, int]:
    discovered: dict[str, int] = {}
    for path, module in production_modules():
        for qualname, function in functions_with_qualnames(module):
            if (count := sum(_swallows(node) for node in own_nodes(function))) > 0:
                site = f"{path}::{qualname}"
                discovered[site] = discovered.get(site, 0) + count
        if (module_count := sum(_swallows(node) for node in own_nodes(module))) > 0:
            discovered[f"{path}::<module>"] = module_count
    return discovered


def test_no_exception_is_swallowed() -> None:
    failures = ratchet_failures(discover(), EXPECTED_SWALLOWED_EXCEPTION_VIOLATIONS, __name__)
    assert not failures, "except blocks that only pass:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_swallowed_exception_violations.py",
        "EXPECTED_SWALLOWED_EXCEPTION_VIOLATIONS",
        "except blocks that only pass, per function, today (test_no_swallowed_exceptions).",
        discover(),
    )
