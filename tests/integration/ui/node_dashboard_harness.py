"""
Shared harness for the node dashboard tests: a pipe stands in for the
terminal on stdout and collects the frames a dashboard renders; helpers
wait for a frame showing given values, start the nodes, and find any task
of the dashboard's still alive.
"""

import asyncio
import contextlib
import os
import pathlib
import sys
import time
from collections.abc import AsyncIterator, Coroutine
from types import CoroutineType

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import ManagerServer, WorkerServer
from hyperscale.logging import Logger
from hyperscale.ui.node_dashboard import (
    ManagerDashboardReader,
    NodeDashboard,
    NodeDashboardConfig,
    WorkerDashboardReader,
)

STDOUT_DESCRIPTOR = 1
FRAME_PREFIX = "\x1b[3J\x1b[H"
WORKER_CORES = 1
# Bounds on what the nodes do on their own: a manager boots and elects
# itself in a few seconds, a worker registers within its registration
# retry budget.
MANAGER_BOOT_SECONDS = 30.0
WORKER_REGISTRATION_SECONDS = 60.0
SHUTDOWN_SECONDS = 30.0
# The dashboard samples once a second (NodeDashboardConfig's default); a
# frame showing a change takes at most a few samples.
FRAME_WAIT_SECONDS = 10.0


@contextlib.asynccontextmanager
async def terminal_pipe() -> AsyncIterator[bytearray]:
    """Stand a pipe in for the terminal on stdout and collect what the
    dashboard writes to it (pytest's capture is a file, which the
    terminal's pipe transport cannot write to)."""
    read_descriptor, write_descriptor = os.pipe()
    saved_stdout_descriptor = os.dup(STDOUT_DESCRIPTOR)
    os.dup2(write_descriptor, STDOUT_DESCRIPTOR)
    os.close(write_descriptor)
    os.set_blocking(read_descriptor, False)
    # The terminal duplicates sys.stdout's descriptor, which pytest's
    # capture points elsewhere: point it at descriptor 1 for the test.
    saved_stdout = sys.stdout
    sys.stdout = open(STDOUT_DESCRIPTOR, "w", closefd=False)
    collected = bytearray()
    loop = asyncio.get_running_loop()
    loop.add_reader(read_descriptor, lambda: collected.extend(os.read(read_descriptor, 1 << 16)))
    try:
        yield collected
    finally:
        sys.stdout.close()
        sys.stdout = saved_stdout
        os.dup2(saved_stdout_descriptor, STDOUT_DESCRIPTOR)
        os.close(saved_stdout_descriptor)
        loop.remove_reader(read_descriptor)
        os.close(read_descriptor)


def latest_frame(collected: bytearray) -> str:
    return collected.decode(errors="replace").rsplit(FRAME_PREFIX, 1)[-1]


async def wait_for_frame(collected: bytearray, *fragments: str, within: float = FRAME_WAIT_SECONDS) -> str:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if all(fragment in (frame := latest_frame(collected)) for fragment in fragments):
            return frame
        await asyncio.sleep(0.1)
    raise AssertionError(f"no frame showed {fragments} within {within}s; the last:\n{latest_frame(collected)}")


@pytest.fixture(autouse=True)
def roomy_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    """The size the dashboard lays out for when stdout is not a terminal
    (``shutil.get_terminal_size`` reads these first): wide enough that no
    asserted line is clipped."""
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setenv("LINES", "60")


def dashboard_env(node_directory: pathlib.Path) -> Env:
    """The nodes' Env. A manager given no WAL data directory keeps its
    idempotency WAL in its logs directory, so that is the test's own
    directory, never the working directory."""
    return Env(
        MERCURY_SYNC_AUTH_SECRET="node-dashboard-test-secret",
        MERCURY_SYNC_LOGS_DIRECTORY=str(node_directory),
    )


def ci_dashboard(reader: ManagerDashboardReader | WorkerDashboardReader, node: ManagerServer | WorkerServer, env: Env, log_path: pathlib.Path) -> NodeDashboard:
    return NodeDashboard(reader, node, "ci", env, log_path, NodeDashboardConfig(), Logger())


def awaited_code_files(coroutine: Coroutine) -> list[str]:
    """The source file of every coroutine in ``coroutine``'s await chain."""
    files: list[str] = []
    awaited: object = coroutine
    while isinstance(awaited, CoroutineType):
        files.append(awaited.cr_code.co_filename)
        awaited = awaited.cr_await
    return files


def dashboard_tasks() -> list[asyncio.Task]:
    """Live tasks running the dashboard's or its terminal's code anywhere
    in their await chain (the sampling loop runs under a TaskRunner run)."""
    return [
        task
        for task in asyncio.all_tasks()
        if not task.done()
        and any("/hyperscale/ui/" in code_file for code_file in awaited_code_files(task.get_coro()))
    ]


async def start_manager(env: Env, tcp_port: int) -> ManagerServer:
    manager = ManagerServer(host="127.0.0.1", tcp_port=tcp_port, udp_port=tcp_port + 1, env=env, dc_id="DC-DASH")
    await asyncio.wait_for(manager.start(), timeout=MANAGER_BOOT_SECONDS)
    return manager


def new_worker(env: Env, tcp_port: int, manager_tcp_port: int) -> WorkerServer:
    return WorkerServer(
        host="127.0.0.1",
        tcp_port=tcp_port,
        udp_port=tcp_port + 1,
        env=env,
        total_cores=WORKER_CORES,
        dc_id="DC-DASH",
        seed_managers=[("127.0.0.1", manager_tcp_port)],
    )

