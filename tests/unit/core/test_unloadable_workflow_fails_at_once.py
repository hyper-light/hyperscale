"""
A workflow its workers would refuse to load fails at once, saying why.

A workflow reaches its workers pickled, and they load it with the
restricted unpickler, which refuses what it blocks -- a ``pathlib.Path``
held by the test file, for one. A refused message cannot be told apart
from any other (its request id is inside it), so the worker's error reply
answered no request: the leader's submission retried until it timed out,
and the run then reported "completed", exit status 0, with no results.

* the leader loads the workflow as its workers will, before submitting it,
  and a refusal fails the workflow with the refused module and the remedy;
* a workflow its workers load is submitted as before;
* a local run whose workflow failed or timed out exits 1 and names each
  workflow and its error; one whose workflows all completed exits 0;
* end to end: ``hyperscale run workflow`` on a test file holding a
  ``pathlib.Path`` exits 1 within seconds, with the refusal in its outcome.
"""

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import cloudpickle
import pytest

import hyperscale.testing  # noqa: F401  (the engines' import order)
from hyperscale.commands.run.workflow import local_outcome
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.protocols.restricted_unpickler import SecurityError
from hyperscale.core.state import Context

# Far past a local run's start and one refused workflow: a run that reaches
# it is waiting out a timeout.
REFUSAL_DEADLINE_SECONDS = 30.0
HYPERSCALE = Path(sys.executable).with_name("hyperscale")

HELD_PATH = Path("/tmp/held")


class HoldsAPath(Workflow):
    vus = 1
    duration = "1s"

    async def describe(self) -> str:
        return HELD_PATH.name


class HoldsAString(Workflow):
    vus = 1
    duration = "1s"
    output = "/tmp/held"


# As ``hyperscale run workflow`` registers a test file: pickled by value,
# since workers cannot import it.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def test_a_workflow_its_workers_refuse_fails_with_the_refused_module_and_the_remedy() -> None:
    manager = RemoteGraphManager.__new__(RemoteGraphManager)

    with pytest.raises(SecurityError) as refusal:
        manager._refuse_an_unloadable_workflow(1, HoldsAPath(), Context())

    message = str(refusal.value)
    assert "HoldsAPath cannot run" in message
    assert "pathlib" in message
    assert "use a str for a path" in message


def test_a_workflow_its_workers_load_is_submitted_as_before() -> None:
    manager = RemoteGraphManager.__new__(RemoteGraphManager)

    manager._refuse_an_unloadable_workflow(1, HoldsAString(), Context())


def test_a_local_run_whose_workflow_failed_exits_1_and_names_it() -> None:
    refusal = SecurityError("Workflow HoldsAPath cannot run")
    failed = local_outcome("run", {"test": "run", "results": {}, "timeouts": {"HoldsAPath": refusal}, "skipped": {}})
    completed = local_outcome("run", {"test": "run", "results": {}, "timeouts": {}, "skipped": {}})

    assert failed.exit_status == 1
    assert failed.line == "run: failed: workflow HoldsAPath: SecurityError: Workflow HoldsAPath cannot run"
    assert (completed.exit_status, completed.line) == (0, "run: completed")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


TEST_FILE = '''
from pathlib import Path

from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse

OUTPUT = Path("/tmp/out")


class PathHolder(Workflow):
    vus = 1
    duration = "2s"

    @step()
    async def get_index(self, url: URL = "http://127.0.0.1:9/") -> HTTPResponse:
        assert OUTPUT.name == "out"
        return await self.client.http.get(url)
'''


async def test_hyperscale_run_workflow_on_a_refused_workflow_exits_1_within_seconds(tmp_path: Path) -> None:
    (tmp_path / "path_workflow.py").write_text(TEST_FILE)
    (tmp_path / "config.json").write_text(
        f'{{"logs_directory": "{tmp_path / "logs"}", "server_port": {free_port()}}}'
    )

    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(
        str(HYPERSCALE),
        "run",
        "workflow",
        "path_workflow.py",
        "--config",
        "config.json",
        "--workers",
        "1",
        "--output-mode",
        "disabled",
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        async with asyncio.timeout(REFUSAL_DEADLINE_SECONDS):
            _, stderr = await process.communicate()

    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

    assert process.returncode == 1
    assert time.monotonic() - started < REFUSAL_DEADLINE_SECONDS
    assert "workflow PathHolder: SecurityError" in stderr.decode()
    assert "Blocked dangerous module: pathlib" in stderr.decode()
