"""
Ratchet lint -- a dataclass declares ``slots=True`` and lives in a
``models`` package (CLAUDE.md: models are their own files, in their own
folder; slots keep per-instance memory to the fields).

Each ``@dataclass`` class under ``hyperscale/`` is a site; it counts one
for missing ``slots=True`` and one for living outside any ``models``
directory. Today's are snapshotted: none may be added, and each fixed
lowers its entry.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_dataclass_violations import EXPECTED_DATACLASS_VIOLATIONS
from tests.simulation.lints.ratchet import REPO_ROOT, production_modules, ratchet_failures, write_snapshot


def _dataclass_decorator(decorator: ast.expr) -> ast.expr | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return decorator if ast.unparse(target) in ("dataclass", "dataclasses.dataclass") else None


def discover() -> dict[str, int]:
    discovered: dict[str, int] = {}
    for path, module in production_modules():
        outside_models = "models" not in path.split("/")
        for node in ast.walk(module):
            if not isinstance(node, ast.ClassDef):
                continue
            for decorator in node.decorator_list:
                if (dataclass := _dataclass_decorator(decorator)) is None:
                    continue
                slotted = isinstance(dataclass, ast.Call) and any(
                    keyword.arg == "slots" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True
                    for keyword in dataclass.keywords
                )
                if (count := int(not slotted) + int(outside_models)) > 0:
                    discovered[f"{path}::{node.name}"] = count
    return discovered


def test_dataclasses_are_slotted_models() -> None:
    failures = ratchet_failures(discover(), EXPECTED_DATACLASS_VIOLATIONS, __name__)
    assert not failures, "Dataclasses without slots=True or outside models/:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_dataclass_violations.py",
        "EXPECTED_DATACLASS_VIOLATIONS",
        "Dataclasses missing slots=True (1) and/or outside models/ (1) today (test_dataclass_conventions).",
        discover(),
    )
