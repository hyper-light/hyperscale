"""
A finished workflow run leaves no connection to its target open.

Two leaks kept a run's connections open for the executor's lifetime:
the runner never released a finished run (its workflow stayed in
_running_workflows and its engine client was closed only at runner
shutdown), and the HTTP engine's close() reached only POOLED
connections -- a request cancelled mid-flight, as every in-flight
request is when a run's duration ends, held a connection checked out
of the pool that nothing ever closed. Found by the AD-41 THROTTLE E2E,
whose target server could never finish closing.

Driven through the real WorkflowRunner running a real HTTP load
workflow against a local server that counts its open connections.
"""

import asyncio
import pathlib

import pytest

from hyperscale.core.jobs.graphs.workflow_runner import WorkflowRunner
from hyperscale.distributed.env import Env
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse

VUS = 5
DURATION_SECONDS = 2.0
HTTP_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"


@pytest.fixture(autouse=True)
def log_under_tmp_path(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner logs to files under the working directory when no logging
    directory reaches its logger's context: this test's own tmp_path, never
    the working tree."""
    monkeypatch.chdir(tmp_path)


class ConnectionCountingServer:
    def __init__(self) -> None:
        self.open_connections: set[asyncio.StreamWriter] = set()

    async def answer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.open_connections.add(writer)
        try:
            while await reader.readuntil(b"\r\n\r\n"):
                writer.write(HTTP_OK)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self.open_connections.discard(writer)
            writer.close()


def load_workflow(target: str) -> Workflow:
    async def hit(self, url: URL = target) -> HTTPResponse:
        return await self.client.http.get(url)

    workflow_class = type(
        "ConnectionLoadWorkflow",
        (Workflow,),
        {"vus": VUS, "duration": f"{DURATION_SECONDS}s", "hit": step()(hit)},
    )
    return workflow_class()


async def test_a_finished_run_closes_its_connections_to_the_target() -> None:
    counting = ConnectionCountingServer()
    server = await asyncio.start_server(counting.answer, "127.0.0.1", 0)
    target = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
    runner = WorkflowRunner(Env(), 1, 1, monitors_enabled=False)
    runner.setup()
    try:
        run = asyncio.ensure_future(runner.run(1, load_workflow(target), {}, VUS))
        connected_while_running = False
        while not run.done():
            if counting.open_connections:
                connected_while_running = True
                break
            await asyncio.sleep(0.01)
        assert connected_while_running, "the load workflow must be connected while it runs"

        await run
        # Closed sockets are observed by the server on its next read.
        for _ in range(100):
            if not counting.open_connections:
                break
            await asyncio.sleep(0.01)

        assert not counting.open_connections, (
            f"{len(counting.open_connections)} connection(s) to the target "
            "outlived the finished run"
        )
        assert not runner._running_workflows[1], "the finished run was not released"
    finally:
        server.close()
        # A leaked connection must fail the assertion above, not hang
        # teardown: wait_closed() waits for every client connection.
        server.close_clients()
        await server.wait_closed()
