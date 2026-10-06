"""
Ratchet lint -- one class per file (CLAUDE.md: "One class per file.
Period."; decision D8).

Counts the top-level classes of each module under ``hyperscale/``. A file
holding more than one today is snapshotted at its count: none may gain a
class, and each one split out lowers its entry.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_multi_class_files import EXPECTED_MULTI_CLASS_FILES
from tests.simulation.lints.ratchet import REPO_ROOT, production_modules, ratchet_failures, write_snapshot


def discover() -> dict[str, int]:
    return {
        path: class_count
        for path, module in production_modules()
        if (class_count := sum(isinstance(node, ast.ClassDef) for node in module.body)) > 1
    }


def test_every_file_holds_at_most_one_class() -> None:
    failures = ratchet_failures(discover(), EXPECTED_MULTI_CLASS_FILES, __name__)
    assert not failures, "Files with more than one class:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_multi_class_files.py",
        "EXPECTED_MULTI_CLASS_FILES",
        "Files holding more than one class today, at their class count (test_one_class_per_file).",
        discover(),
    )
