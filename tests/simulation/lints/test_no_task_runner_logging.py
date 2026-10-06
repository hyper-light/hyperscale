"""
Ratchet lint -- a log call is awaited, never handed to the task runner
(CLAUDE.md: "The logger is async and you need to await .log(), don't add
it to the task runner").

A ``task_runner.run(<logger>.log, ...)`` makes a task per log line, logs
out of order with the work it describes, and loses the line when the run
ends first. Today's are snapshotted per function: none may be added, and
each one converted to ``await <logger>.log(...)`` lowers its entry.
"""

from __future__ import annotations

import ast

from tests.simulation.lints.expected_task_runner_logging_violations import (
    EXPECTED_TASK_RUNNER_LOGGING_VIOLATIONS,
)
from tests.simulation.lints.ratchet import (
    REPO_ROOT,
    functions_with_qualnames,
    own_nodes,
    production_modules,
    ratchet_failures,
    write_snapshot,
)


def _runs_a_log_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run"
        and "task_runner" in ast.unparse(node.func.value)
        and bool(node.args)
        and isinstance(node.args[0], ast.Attribute)
        and node.args[0].attr == "log"
    )


def discover() -> dict[str, int]:
    discovered: dict[str, int] = {}
    for path, module in production_modules():
        for qualname, function in functions_with_qualnames(module):
            if (count := sum(_runs_a_log_call(node) for node in own_nodes(function))) > 0:
                site = f"{path}::{qualname}"
                discovered[site] = discovered.get(site, 0) + count
    return discovered


def test_log_calls_are_awaited_not_run() -> None:
    failures = ratchet_failures(discover(), EXPECTED_TASK_RUNNER_LOGGING_VIOLATIONS, __name__)
    assert not failures, "Log calls handed to the task runner:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    write_snapshot(
        REPO_ROOT / "tests/simulation/lints/expected_task_runner_logging_violations.py",
        "EXPECTED_TASK_RUNNER_LOGGING_VIOLATIONS",
        "Log calls handed to the task runner, per function, today (test_no_task_runner_logging).",
        discover(),
    )
