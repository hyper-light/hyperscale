"""
Ratchet lint -- no function's cyclomatic complexity passes 3 (CLAUDE.md:
"Avoid cyclomatic complexity beyond three"; decision D7).

Complexity is McCabe's, counted as radon counts it: 1, plus 1 for each
``if``/``elif``, loop, ``except``, conditional expression, ``match`` case
and comprehension (and each of its ``if`` clauses), plus each extra
operand of an ``and``/``or``. A nested function, class or lambda is a site
of its own. Functions over the ceiling today are snapshotted at their
complexity: none may rise, and each one paid down lowers its entry.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_complexity_violations import EXPECTED_COMPLEXITY_VIOLATIONS
from tests.simulation.lints.ratchet import (
    REPO_ROOT,
    functions_with_qualnames,
    own_nodes,
    production_modules,
    ratchet_failures,
    write_snapshot,
)

COMPLEXITY_CEILING = 3
BRANCHES = (ast.If, ast.For, ast.AsyncFor, ast.While, ast.ExceptHandler, ast.IfExp, ast.match_case)


def complexity_of(function: ast.AST) -> int:
    score = 1
    for node in own_nodes(function):
        if isinstance(node, BRANCHES):
            score += 1
        elif isinstance(node, ast.BoolOp):
            score += len(node.values) - 1
        elif isinstance(node, ast.comprehension):
            score += 1 + len(node.ifs)
    return score


def discover() -> dict[str, int]:
    discovered: dict[str, int] = {}
    for path, module in production_modules():
        for qualname, function in functions_with_qualnames(module):
            if (score := complexity_of(function)) > COMPLEXITY_CEILING:
                site = f"{path}::{qualname}"
                discovered[site] = max(score, discovered.get(site, 0))
    return discovered


def test_no_function_is_more_complex_than_the_ceiling() -> None:
    failures = ratchet_failures(discover(), EXPECTED_COMPLEXITY_VIOLATIONS, __name__)
    assert not failures, f"Cyclomatic complexity above {COMPLEXITY_CEILING}:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_complexity_violations.py",
        "EXPECTED_COMPLEXITY_VIOLATIONS",
        "Functions above the complexity ceiling today, at their complexity (test_complexity_ceiling).",
        discover(),
    )
