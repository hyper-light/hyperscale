"""
Every name the distributed package uses resolves.

Python resolves names only when a line runs, and since 3.14 annotations
are evaluated lazily too, so a module that uses a name it never imports
loads cleanly and fails only when that line executes -- typically an
error or failover path that no happy-path test reaches. Found this way:
a gate's job-leader transfer to managers (JobLeaderGateTransfer), the
client's status-poll and tracking logs, a worker's registration
rejection log, cross-DC correlation's last-resort error report, and a
manager's workflow-progress cleanup that referenced a variable from
another method (it raised on every call and stranded that method's
worker cleanup).

Checks the whole package with pyflakes' undefined-name analysis.
"""

import pathlib

import pyflakes.api
import pyflakes.messages
import pyflakes.reporter

import hyperscale.distributed

DISTRIBUTED_ROOT = pathlib.Path(hyperscale.distributed.__file__).parent


class UndefinedNameCollector(pyflakes.reporter.Reporter):
    def __init__(self) -> None:
        super().__init__(warningStream=None, errorStream=None)
        self.undefined: list[str] = []
        self.unparseable: list[str] = []

    def flake(self, message: pyflakes.messages.Message) -> None:
        if isinstance(message, pyflakes.messages.UndefinedName):
            self.undefined.append(str(message))

    def syntaxError(self, filename, msg, lineno, offset, text) -> None:
        self.unparseable.append(f"{filename}:{lineno}: {msg}")

    def unexpectedError(self, filename, msg) -> None:
        self.unparseable.append(f"{filename}: {msg}")


def test_every_used_name_resolves() -> None:
    collector = UndefinedNameCollector()
    for path in sorted(DISTRIBUTED_ROOT.rglob("*.py")):
        pyflakes.api.checkPath(str(path), collector)

    assert collector.unparseable == []
    assert collector.undefined == []
