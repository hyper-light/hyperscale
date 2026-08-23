"""
Ratchet lint — forbids two methods with the same name in one class
body anywhere under ``hyperscale/``.

Why this is a crash lint, not a style lint
------------------------------------------

Python evaluates a class body top to bottom, so a redefined name
silently discards the earlier definition: the LAST one wins and the
first becomes unreachable. Nothing warns. The failure surfaces at the
call sites written against the *discarded* signature, as a TypeError
on a path that may run only in an emergency:

``GateServer._push_global_job_result`` was defined twice — first as
``(self, job_id, result)``, later as ``(self, result)``. The second
won, so ``handle_global_timeout``'s ``(job_id, timeout_result)`` call
raised ``TypeError: takes 2 positional arguments but 3 were given``
— the AD-34 global-timeout push to the client, i.e. the loud terminal
that exists precisely for when everything else has failed.

The same shape hides worse things than an arity mismatch: two
same-named methods whose bodies have drifted apart look, to a reader
and to a reviewer, like the behavior of the first — while the second
executes.

What's flagged
--------------

Two or more ``def`` / ``async def`` statements binding the same name
directly in one class body, including nested classes (each class body
is scanned independently).

What's NOT flagged
------------------

* Same-named methods in DIFFERENT classes (including a subclass
  overriding a base) — that's ordinary polymorphism.
* A method and a same-named nested class or class attribute — a
  different (and much louder) mistake; kept out to hold this lint to
  one precise meaning.
* ``@typing.overload`` stacks and ``@property`` / ``@x.setter`` /
  ``@x.deleter`` families — these legitimately repeat a name, and the
  decorator makes the intent explicit, so they are exempt by
  decorator inspection rather than by snapshot.

How the ratchet works
---------------------

Identical to the sibling lints: the expected-violations snapshot
lives next to this file, and the test fails when discovered differs
from expected in EITHER direction — a new duplicate is a failure, and
fixing one without updating the snapshot is also a failure (so the
snapshot can only shrink deliberately).
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator

from tests.simulation.lints.expected_duplicate_method_violations import (
    EXPECTED_DUPLICATE_METHOD_VIOLATIONS,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
PRODUCTION_ROOT = REPO_ROOT / "hyperscale"

# Decorators whose whole purpose is to bind a name more than once.
_REPEAT_LEGAL_DECORATORS: frozenset[str] = frozenset(
    {"overload", "setter", "deleter", "getter", "register"}
)


def _iter_python_files(root: Path) -> Iterator[Path]:
    """Yield every ``.py`` file under ``root`` excluding caches."""
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def _decorator_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """Return the trailing attribute/name of each decorator.

    ``@property`` -> {"property"}; ``@duration.setter`` -> {"setter"};
    ``@typing.overload`` -> {"overload"}.
    """
    names: set[str] = set()
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Attribute):
            names.add(target.attr)
        elif isinstance(target, ast.Name):
            names.add(target.id)
    return names


def _duplicate_methods_in_class(class_node: ast.ClassDef) -> list[tuple[str, int]]:
    """Return ``(method_name, line_of_the_shadowed_def)`` per duplicate."""
    first_definition_line: dict[str, int] = {}
    duplicates: list[tuple[str, int]] = []

    for statement in class_node.body:
        if not isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if _decorator_names(statement) & _REPEAT_LEGAL_DECORATORS:
            continue

        previous_line = first_definition_line.get(statement.name)
        if previous_line is not None:
            # Report the SHADOWED definition's line: that is the code a
            # reader believes is running and which never executes.
            duplicates.append((statement.name, previous_line))
        first_definition_line[statement.name] = statement.lineno

    return duplicates


def _discover_violations() -> set[str]:
    """Return ``"<repo-relative path>::<Class>.<method>:<line>"`` rows."""
    violations: set[str] = set()
    for path in _iter_python_files(PRODUCTION_ROOT):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            for method_name, shadowed_line in _duplicate_methods_in_class(node):
                relative_path = path.relative_to(REPO_ROOT).as_posix()
                violations.add(
                    f"{relative_path}::{node.name}.{method_name}:{shadowed_line}"
                )
    return violations


def test_no_duplicate_method_definitions() -> None:
    """Discovered duplicates must equal the recorded snapshot exactly."""
    discovered = _discover_violations()
    expected = set(EXPECTED_DUPLICATE_METHOD_VIOLATIONS)

    newly_introduced = sorted(discovered - expected)
    assert not newly_introduced, (
        "New duplicate method definition(s) — the later def silently "
        "discards the earlier one, so every call site written against "
        "the shadowed signature raises TypeError on first execution:\n  "
        + "\n  ".join(newly_introduced)
    )

    stale_entries = sorted(expected - discovered)
    assert not stale_entries, (
        "Snapshot lists duplicates that no longer exist — remove them "
        "from expected_duplicate_method_violations.py so the ratchet "
        "keeps its teeth:\n  " + "\n  ".join(stale_entries)
    )
