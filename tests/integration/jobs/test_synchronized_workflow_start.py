"""
Synchronized workflow start across a leader and its worker processes.

Every worker running a workflow sets up -- clients built, targets resolved
and connected -- then reports ready and waits at its start gate; the leader
starts them together once every worker has reported, so one worker's slow
setup no longer staggers the others' load (and their clocks). A worker whose
setup fails reports its results instead, which also counts; past the
workflow's timeout the leader starts whoever is ready, and a straggler starts
as soon as it reports.

Driven through the production RemoteGraphController over loopback UDP, with
the workers in their own spawned processes as the local server pool runs
them, against a local HTTP target. The slow worker's setup is delayed or
failed by wrapping its runner inside its own process; each worker appends
when its load starts to an events file, timed with time.monotonic(), which
is system-wide on one host.
"""

import asyncio
import json
import multiprocessing
import os
import socket
import time
from typing import Any, AsyncIterator

import pytest

from hyperscale.core.jobs.graphs.remote_graph_controller import RemoteGraphController
from hyperscale.core.jobs.models import Env, JobContext, WorkflowReady, WorkflowStopSignal
from hyperscale.core.jobs.runner.local_server_pool import run_server
from hyperscale.graph import Workflow, step
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.testing import URL, HTTPResponse

HOST = "127.0.0.1"
AUTH_SECRET = "synchronized-workflow-start-secret"
WORKER_COUNT = 2
VUS_PER_WORKER = 2
FAST_WORKER_INDEX = 0
SLOW_WORKER_INDEX = 1
HTTP_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"

# Two workers released together start within one loopback release round of
# each other: measured at well under a millisecond, far below any setup skew
# injected here.
TOGETHER_TOLERANCE_SECONDS = 0.05
# Scheduling slack when asserting that a start lands at an expected moment.
TIMING_TOLERANCE_SECONDS = 0.5

# The slow worker's setup for each workflow: (delay in seconds, fail after it).
SLOW_WORKER_SETUP: dict[str, tuple[float, bool]] = {
    "SkewedSetupWorkflow": (2.0, False),
    "FailingSetupWorkflow": (0.3, True),
    "StragglerWorkflow": (2.5, False),
    "CancelledWhileWaitingWorkflow": (3.0, False),
}


def free_port(socket_kind: int) -> int:
    with socket.socket(socket.AF_INET, socket_kind) as port_probe:
        port_probe.bind((HOST, 0))
        return port_probe.getsockname()[1]


def record_event(events_path: str, *fields: str | int | float) -> None:
    with open(events_path, "a") as events_file:
        events_file.write(json.dumps(fields) + "\n")


def read_events(events_path: str, workflow_name: str) -> list[list[Any]]:
    if not os.path.exists(events_path):
        return []

    with open(events_path) as events_file:
        events = [json.loads(line) for line in events_file if line.strip()]

    return [event for event in events if event[2] == workflow_name]


def make_workflow(name: str, target: str, duration: str, timeout: str) -> Workflow:
    async def hit(self, url: URL = target) -> HTTPResponse:
        return await self.client.http.get(url)

    workflow_class = type(
        name,
        (Workflow,),
        {
            "vus": WORKER_COUNT * VUS_PER_WORKER,
            "duration": duration,
            "timeout": timeout,
            "hit": step()(hit),
        },
    )
    return workflow_class()


def instrument_runner(server: RemoteGraphController, worker_index: int, events_path: str) -> None:
    """Delay or fail the slow worker's setup, and record each worker's load
    start and the start gates it still holds once each run returns."""
    runner = server._workflows
    setup_behaviour = SLOW_WORKER_SETUP if worker_index == SLOW_WORKER_INDEX else {}

    original_setup = runner._setup
    original_execute = runner._execute_test_workflow
    original_run = runner.run

    async def adjusted_setup(run_id: int, workflow: Workflow, *args: Any, **kwargs: Any):
        delay_seconds, fail = setup_behaviour.get(workflow.name, (0.0, False))
        await asyncio.sleep(delay_seconds)
        if fail:
            raise RuntimeError("injected setup failure")

        return await original_setup(run_id, workflow, *args, **kwargs)

    async def recording_execute(run_id: int, workflow: Workflow, *args: Any, **kwargs: Any):
        record_event(events_path, "load_start", worker_index, workflow.name, time.monotonic())
        return await original_execute(run_id, workflow, *args, **kwargs)

    async def reporting_run(run_id: int, workflow: Workflow, *args: Any, **kwargs: Any):
        try:
            return await original_run(run_id, workflow, *args, **kwargs)

        finally:
            record_event(
                events_path,
                "gates_after_run",
                worker_index,
                workflow.name,
                len(server._workflow_start_gates),
            )

    runner._setup = adjusted_setup
    runner._execute_test_workflow = recording_execute
    runner.run = reporting_run


async def serve_worker(
    worker_index: int,
    worker_port: int,
    leader_address: tuple[str, int],
    events_path: str,
    log_directory: str,
) -> None:
    LoggingConfig().update(log_directory=log_directory, log_level="error")
    server = RemoteGraphController(
        worker_index + 1,
        HOST,
        worker_port,
        Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET),
    )
    instrument_runner(server, worker_index, events_path)
    await run_server(leader_address, server)


def worker_process_entry(
    worker_index: int,
    worker_port: int,
    leader_address: tuple[str, int],
    events_path: str,
    log_directory: str,
) -> None:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", AUTH_SECRET)
    asyncio.run(serve_worker(worker_index, worker_port, leader_address, events_path, log_directory))


async def answer_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while await reader.readuntil(b"\r\n\r\n"):
            writer.write(HTTP_OK)
            await writer.drain()

    except (asyncio.IncompleteReadError, ConnectionError):
        pass

    finally:
        writer.close()


@pytest.fixture
async def started_cluster(tmp_path) -> AsyncIterator[tuple[RemoteGraphController, str, str]]:
    """A leader controller in this process, ``WORKER_COUNT`` workers in
    spawned processes connected to it, and a local HTTP target. Yields the
    leader, the workers' events file, and the target URL."""
    log_directory = str(tmp_path)
    events_path = str(tmp_path / "events.jsonl")
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", AUTH_SECRET)
    LoggingConfig().update(log_directory=log_directory, log_level="error")

    target_port = free_port(socket.SOCK_STREAM)
    target_server = await asyncio.start_server(answer_http, HOST, target_port)

    leader_port = free_port(socket.SOCK_DGRAM)
    worker_ports = [free_port(socket.SOCK_DGRAM) for _ in range(WORKER_COUNT)]
    leader = RemoteGraphController(None, HOST, leader_port, Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET))
    await leader.start_server()

    spawn_context = multiprocessing.get_context("spawn")
    worker_processes = [
        spawn_context.Process(
            target=worker_process_entry,
            args=(worker_index, worker_port, (HOST, leader_port), events_path, log_directory),
        )
        for worker_index, worker_port in enumerate(worker_ports)
    ]
    for worker_process in worker_processes:
        worker_process.start()

    try:
        assert await leader.wait_for_workers(WORKER_COUNT, timeout=30), "workers never acknowledged"
        await asyncio.gather(*[leader.connect_client((HOST, worker_port)) for worker_port in worker_ports])

        yield leader, events_path, f"http://{HOST}:{target_port}/"

    finally:
        await leader.submit_stop_request()
        for worker_process in worker_processes:
            worker_process.join(timeout=10)
            if worker_process.is_alive():
                worker_process.terminate()

        leader.stop()
        await leader.close()
        target_server.close()
        await target_server.wait_closed()


async def run_workflow_across_workers(
    leader: RemoteGraphController,
    workflow: Workflow,
    events_path: str,
    cancel_after_seconds: float | None = None,
) -> dict[str, Any]:
    """Submit ``workflow`` to every worker as the graph manager does, wait for
    its release and completion, and report what happened -- every moment in
    seconds since submission."""
    run_id = leader.id_generator.generate()
    leader.create_run_contexts(run_id)
    context = leader.assign_context(run_id, workflow.name, WORKER_COUNT)
    completion_state = leader.register_workflow_completion(run_id, workflow.name, WORKER_COUNT)

    submitted_at = time.monotonic()
    submission = asyncio.ensure_future(
        leader.submit_workflow_to_workers(
            run_id,
            workflow,
            context,
            WORKER_COUNT,
            [VUS_PER_WORKER] * WORKER_COUNT,
            sorted(leader.acknowledged_start_node_ids),
        )
    )

    outcome: dict[str, Any] = {}
    try:
        if cancel_after_seconds is not None:
            await asyncio.sleep(cancel_after_seconds)
            cancel_requested_at = time.monotonic()
            await leader.submit_workflow_cancellation(run_id, workflow.name, "10s")
            cancel_settled, cancel_errors = await leader.await_workflow_cancellation(
                run_id,
                workflow.name,
                timeout=20,
            )
            outcome["cancel_settled"] = cancel_settled
            outcome["cancel_errors"] = cancel_errors
            outcome["cancel_settle_seconds"] = time.monotonic() - cancel_requested_at

        await asyncio.wait_for(submission, timeout=40)
        outcome["released_seconds"] = time.monotonic() - submitted_at

        await asyncio.wait_for(completion_state.completion_event.wait(), timeout=40)
        outcome["completed_seconds"] = time.monotonic() - submitted_at

        node_errors = {str(error) for error in leader._errors[run_id][workflow.name].values()}
        outcome["node_errors"] = sorted(node_errors - {"None", ""})

    finally:
        if not submission.done():
            submission.cancel()

        leader.cleanup_workflow_completion(run_id, workflow.name)

    # Each worker records its gates as its run returns, after its results went out.
    await asyncio.sleep(TIMING_TOLERANCE_SECONDS)

    events = read_events(events_path, workflow.name)
    outcome["load_starts"] = {
        worker_index: started_at - submitted_at
        for kind, worker_index, _, started_at in events
        if kind == "load_start"
    }
    outcome["gates_after_runs"] = [gates for kind, _, _, gates in events if kind == "gates_after_run"]
    outcome["leader_barriers"] = dict(leader._workflow_start_barriers)
    return outcome


def assert_nothing_outlives_the_run(outcome: dict[str, Any]) -> None:
    assert outcome["leader_barriers"] == {}
    assert outcome["gates_after_runs"] == [0] * WORKER_COUNT


async def test_workers_with_skewed_setups_start_their_load_together(started_cluster) -> None:
    leader, events_path, target = started_cluster
    slow_setup_seconds, _ = SLOW_WORKER_SETUP["SkewedSetupWorkflow"]

    outcome = await run_workflow_across_workers(
        leader,
        make_workflow("SkewedSetupWorkflow", target, "1s", "10s"),
        events_path,
    )

    load_starts = outcome["load_starts"]
    assert outcome["node_errors"] == []
    assert sorted(load_starts) == list(range(WORKER_COUNT))
    assert max(load_starts.values()) - min(load_starts.values()) <= TOGETHER_TOLERANCE_SECONDS
    # The fast worker waited for the slow one instead of starting alone.
    assert load_starts[FAST_WORKER_INDEX] >= slow_setup_seconds
    assert outcome["released_seconds"] >= slow_setup_seconds
    assert_nothing_outlives_the_run(outcome)


async def test_a_failed_setup_releases_the_other_workers_at_once(started_cluster) -> None:
    leader, events_path, target = started_cluster
    failure_seconds, _ = SLOW_WORKER_SETUP["FailingSetupWorkflow"]

    outcome = await run_workflow_across_workers(
        leader,
        make_workflow("FailingSetupWorkflow", target, "1s", "10s"),
        events_path,
    )

    # Only the healthy worker runs load, from the moment the failure arrives
    # rather than at the workflow's 10s timeout.
    assert outcome["node_errors"] == ["injected setup failure"]
    assert list(outcome["load_starts"]) == [FAST_WORKER_INDEX]
    assert outcome["load_starts"][FAST_WORKER_INDEX] <= failure_seconds + TIMING_TOLERANCE_SECONDS
    assert_nothing_outlives_the_run(outcome)


async def test_a_straggler_past_the_timeout_neither_holds_back_nor_hangs_the_run(started_cluster) -> None:
    leader, events_path, target = started_cluster
    straggler_setup_seconds, _ = SLOW_WORKER_SETUP["StragglerWorkflow"]
    setup_timeout_seconds = 1.0

    outcome = await run_workflow_across_workers(
        leader,
        make_workflow("StragglerWorkflow", target, "1s", f"{setup_timeout_seconds}s"),
        events_path,
    )

    load_starts = outcome["load_starts"]
    assert outcome["node_errors"] == []
    # The ready worker starts when the timeout passes; the straggler, as soon
    # as it reports ready -- and the run still completes.
    assert setup_timeout_seconds <= load_starts[FAST_WORKER_INDEX]
    assert load_starts[FAST_WORKER_INDEX] <= setup_timeout_seconds + TIMING_TOLERANCE_SECONDS
    assert straggler_setup_seconds <= load_starts[SLOW_WORKER_INDEX]
    assert load_starts[SLOW_WORKER_INDEX] <= straggler_setup_seconds + TIMING_TOLERANCE_SECONDS
    assert "completed_seconds" in outcome
    assert_nothing_outlives_the_run(outcome)


async def test_cancelling_a_run_that_waits_to_start_settles_without_its_release(started_cluster) -> None:
    leader, events_path, target = started_cluster
    slow_setup_seconds, _ = SLOW_WORKER_SETUP["CancelledWhileWaitingWorkflow"]
    cancel_after_seconds = 0.8

    outcome = await run_workflow_across_workers(
        leader,
        make_workflow("CancelledWhileWaitingWorkflow", target, "5s", "30s"),
        events_path,
        cancel_after_seconds=cancel_after_seconds,
    )

    # Settles once the slow worker leaves setup: not at the 30s setup timeout,
    # and without running the 5s duration.
    assert outcome["cancel_settled"] is True
    assert outcome["cancel_errors"] == []
    settle_bound_seconds = slow_setup_seconds - cancel_after_seconds + TIMING_TOLERANCE_SECONDS
    assert outcome["cancel_settle_seconds"] <= settle_bound_seconds
    assert outcome["completed_seconds"] <= slow_setup_seconds + TIMING_TOLERANCE_SECONDS
    assert_nothing_outlives_the_run(outcome)


async def test_a_ready_for_a_run_without_a_barrier_is_released_at_once(started_cluster) -> None:
    leader, _, _ = started_cluster

    _, reply = await leader.send(
        "receive_workflow_ready",
        JobContext(WorkflowReady("UnsubmittedWorkflow"), run_id=leader.id_generator.generate()),
        node_id=leader.node_id,
        target_address=(leader.host, leader.port),
    )

    assert isinstance(reply, JobContext)
    assert reply.data.released is True
    assert leader._workflow_start_barriers == {}


async def test_a_release_for_a_run_no_longer_waiting_is_acknowledged(started_cluster) -> None:
    leader, _, _ = started_cluster

    _, reply = await leader.request_workflow_release(
        leader.id_generator.generate(),
        "UnsubmittedWorkflow",
        sorted(leader.acknowledged_start_node_ids)[0],
    )

    assert isinstance(reply, JobContext)
    assert reply.data.workflow == "UnsubmittedWorkflow"


async def test_stop_reports_are_answered_on_the_first_attempt(started_cluster) -> None:
    leader, _, _ = started_cluster

    sent_at = time.monotonic()
    _, reply = await leader.send(
        "receive_stop",
        JobContext(WorkflowStopSignal("UnsubmittedWorkflow", leader.node_id), run_id=leader.id_generator.generate()),
        node_id=leader.node_id,
        target_address=(leader.host, leader.port),
    )

    # Answered rather than retried: a reply inside the first request timeout.
    assert isinstance(reply, JobContext)
    assert reply.data.workflow == "UnsubmittedWorkflow"
    assert time.monotonic() - sent_at < leader._request_timeout
