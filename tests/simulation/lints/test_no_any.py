"""
Ratchet lint -- no ``Any`` (CLAUDE.md: "If you can use generics, do so.
Avoid using Any for typehints"; SCAN.md PROBLEM 4). ``Any`` switches the
type checker off for everything it touches: a value it types can flow
anywhere unchecked. The precise type is a generic (TypeVar, ParamSpec), a
Protocol, a union or an alias of one, or ``object`` for a value that is
genuinely opaque and only passed through.

Every reference to ``Any`` -- ``Any`` itself or ``typing.Any`` -- counts,
wherever it appears: annotations, aliases, ``cast`` targets, TypeVar
bounds. Importing it does not. Today's references are snapshotted per
file: none may be added, and each one replaced lowers its entry.

The snapshot holds two justified distributed/ exceptions:

* ``ledger/checkpoint/checkpoint_model.py`` -- ``Checkpoint.job_states``
  is a msgspec field holding each job's state as decoded, whose shape
  the checkpoint does not own.
* ``models/restricted_unpickler.py::restricted_loads`` -- the
  ``cloudpickle.loads`` drop-in returns whatever the allow-listed pickle
  holds; each caller narrows it to the message it expects.

hyperscale/core/engines (owner's OK needed), hyperscale/core/jobs
(peer-owned) and hyperscale/commands/cli (off-limits) are held at their
counts until their owners pay them down.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_any_violations import EXPECTED_ANY_VIOLATIONS
from tests.simulation.lints.ratchet import (
    REPO_ROOT,
    production_modules,
    ratchet_failures,
    write_snapshot,
)

TYPING_MODULE_NAMES = frozenset({"typing", "typing_extensions"})


def is_any_reference(node: ast.AST) -> bool:
    return (isinstance(node, ast.Name) and node.id == "Any") or (
        isinstance(node, ast.Attribute)
        and node.attr == "Any"
        and isinstance(node.value, ast.Name)
        and node.value.id in TYPING_MODULE_NAMES
    )


def discover() -> dict[str, int]:
    return {
        path: count
        for path, module in production_modules()
        if (count := sum(map(is_any_reference, ast.walk(module)))) > 0
    }


def test_no_any() -> None:
    failures = ratchet_failures(discover(), EXPECTED_ANY_VIOLATIONS, __name__)
    assert not failures, "References to Any:\n  " + "\n  ".join(failures)


def test_every_spelling_of_any_counts_and_imports_do_not() -> None:
    probe = ast.parse(
        "import typing\n"
        "from typing import Any\n"
        "Alias = dict[str, Any]\n"
        "def function(value: typing.Any) -> list[Any]: ...\n"
        "def opaque(value: object) -> None: ...\n"
    )
    assert sum(map(is_any_reference, ast.walk(probe))) == 3


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_any_violations.py",
        "EXPECTED_ANY_VIOLATIONS",
        "References to Any, per file, today (test_no_any).",
        discover(),
    )
