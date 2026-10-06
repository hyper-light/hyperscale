"""
A workflow run's life on a node's WorkflowRunner, driven through the real
runner with real workflows (load workflows against a local HTTP server).

* Cancellation is per run: cancelling one run leaves a concurrent run of
  another workflow running; a cancel for a run that already ended reaches
  nothing (it used to stop whatever ran next); a cancel made before a run
  starts holds when it does (it used to be lost).
* A hard-cancelled run reports CANCELLED and stops its CPU and memory
  sampling (it used to stay RUNNING and sample until the runner closed).
* Once released, nothing is kept for a run -- in the runner or its monitors
  -- however many runs a long-lived worker serves.
* An error in the run check leaves the check free for the next run (its lock
  used to be held forever), and replacing a run that is not yet set up
  works (it raised KeyError, holding the lock).
* Each run binds its own copies of the workflow class's hooks: a second run
  of a class runs on its own instance (from Python 3.14, binding an already
  bound method kept the first instance), and the class's hooks stay unbound.
"""

import asyncio
from types import MethodType

import pytest

from hyperscale.core.jobs.graphs.workflow_runner import WorkflowRunner
from hyperscale.core.jobs.models.env import Env
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.state import Provide, state
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse

HTTP_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"
LOAD_VUS = 4
LOAD_SECONDS = 3.0
CANCEL_AFTER_SECONDS = 0.5
# Far past anything these runs take: a wait that reaches it is a hang.
HANG_SECONDS = 10.0
RUNNER_STATE = (
    "run_statuses",
    "_completed_counts",
    "_failed_counts",
    "_workflow_step_stats",
    "_workflow_hooks",
    "_active",
    "_max_active",
    "_run_tasks",
    "_running_workflows",
    "_active_waiters",
    "_throttle_base_cap",
    "_concurrency_gated",
    "_run_controls",
)
MONITOR_STATE = ("active", "_running_monitors", "_background_monitors", "_locked_runs")


class LocalTarget:
    """A local HTTP server answering every request 200, keeping connections alive."""

    def __init__(self) -> None:
        self._server: asyncio.Server | None = None

    async def __aenter__(self) -> str:
        self._server = await asyncio.start_server(self._answer, "127.0.0.1", 0)
        return f"http://127.0.0.1:{self._server.sockets[0].getsockname()[1]}/"

    async def __aexit__(self, *_: object) -> None:
        self._server.close()
        # The runs closed their clients as they ended: no connection lingers.
        async with asyncio.timeout(HANG_SECONDS):
            await self._server.wait_closed()

    async def _answer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while await reader.readuntil(b"\r\n\r\n"):
                writer.write(HTTP_OK)
                await writer.drain()

        except (asyncio.IncompleteReadError, ConnectionError):
            # The client closed its connection: the end of its requests.
            pass

        finally:
            writer.close()


class TokenProvider(Workflow):
    @state()
    def token(self) -> Provide[str]:
        return f"instance-{id(self)}"


def load_workflow(class_name: str, target: str, duration_seconds: float) -> Workflow:
    async def hit(self, url: URL = target) -> HTTPResponse:
        return await self.client.http.get(url)

    workflow_class = type(
        class_name,
        (Workflow,),
        {"vus": LOAD_VUS, "duration": f"{duration_seconds}s", "hit": step()(hit)},
    )
    return workflow_class()


def load_action_workflow(class_name: str, sleep_seconds: float = 0.0) -> type[Workflow]:
    async def act(self) -> str:
        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)

        return f"instance-{id(self)}"

    return type(class_name, (Workflow,), {"vus": 1, "duration": "5s", "act": step()(act)})


def make_runner(max_running: int = 1, monitors_enabled: bool = False) -> WorkflowRunner:
    runner = WorkflowRunner(
        Env(MERCURY_SYNC_MAX_RUNNING_WORKFLOWS=max_running),
        1,
        1,
        monitors_enabled=monitors_enabled,
    )
    runner.setup()
    return runner


def kept_state(runner: WorkflowRunner) -> dict[str, int]:
    """Every runner and monitor map still holding something."""
    kept = {name: len(getattr(runner, name)) for name in RUNNER_STATE if getattr(runner, name)}
    for monitor_name in ("_cpu_monitor", "_memory_monitor"):
        monitor = getattr(runner, monitor_name)
        kept.update(
            {f"{monitor_name}.{name}": len(getattr(monitor, name)) for name in MONITOR_STATE if getattr(monitor, name)}
        )

    return kept


def live_samplers() -> int:
    return sum(
        1
        for running_task in asyncio.all_tasks()
        if running_task.get_coro().__qualname__.endswith("_update_background_monitor")
    )


async def test_cancelling_one_run_leaves_a_concurrent_run_running() -> None:
    async with LocalTarget() as target:
        runner = make_runner(max_running=2)
        loop = asyncio.get_running_loop()
        started = loop.time()
        cancelled_run = asyncio.ensure_future(
            runner.run(1, load_workflow("CancelledLoad", target, LOAD_SECONDS), {}, LOAD_VUS)
        )
        kept_run = asyncio.ensure_future(runner.run(2, load_workflow("KeptLoad", target, LOAD_SECONDS), {}, LOAD_VUS))
        await asyncio.sleep(CANCEL_AFTER_SECONDS)

        assert runner.request_cancellation(1, "CancelledLoad") is True

        _, _, _, cancelled_error, cancelled_status = await cancelled_run
        cancelled_ended = loop.time() - started
        _, _, _, kept_error, kept_status = await kept_run
        kept_ended = loop.time() - started

        assert (cancelled_error, cancelled_status) == (None, WorkflowStatus.COMPLETED)
        assert cancelled_ended < LOAD_SECONDS - 1  # its VUs stopped starting iterations
        assert (kept_error, kept_status) == (None, WorkflowStatus.COMPLETED)
        assert kept_ended >= LOAD_SECONDS  # the other run went to its deadline


async def test_a_cancel_for_an_ended_run_reaches_nothing() -> None:
    async with LocalTarget() as target:
        runner = make_runner()
        loop = asyncio.get_running_loop()
        await runner.run(1, load_action_workflow("Ended")(), {}, 1)
        started = loop.time()
        running = asyncio.ensure_future(
            runner.run(2, load_workflow("StillRunning", target, LOAD_SECONDS), {}, LOAD_VUS)
        )
        await asyncio.sleep(CANCEL_AFTER_SECONDS)

        assert runner.request_cancellation(1, "Ended") is False

        await running
        assert loop.time() - started >= LOAD_SECONDS


async def test_a_cancel_made_before_a_run_starts_holds_when_it_does() -> None:
    async with LocalTarget() as target:
        runner = make_runner()
        loop = asyncio.get_running_loop()
        control = runner.register_run(1, "CancelledEarly")

        assert runner.request_cancellation(1, "CancelledEarly") is True

        started = loop.time()
        _, _, _, error, status = await runner.run(
            1,
            load_workflow("CancelledEarly", target, LOAD_SECONDS),
            {},
            LOAD_VUS,
            control=control,
        )

        assert (error, status) == (None, WorkflowStatus.COMPLETED)
        assert loop.time() - started < LOAD_SECONDS - 1  # no VU started an iteration
        assert control.ended.is_set()

        runner.release_run(1, "CancelledEarly", control)
        assert kept_state(runner) == {}


async def test_await_cancellation_returns_once_the_run_ends() -> None:
    async with LocalTarget() as target:
        runner = make_runner()
        running = asyncio.ensure_future(runner.run(1, load_workflow("Awaited", target, LOAD_SECONDS), {}, LOAD_VUS))
        await asyncio.sleep(CANCEL_AFTER_SECONDS)

        assert runner.request_cancellation(1, "Awaited") is True
        async with asyncio.timeout(LOAD_SECONDS):
            await runner.await_cancellation(1, "Awaited")

        await running
        # Released: nothing left to wait for.
        async with asyncio.timeout(HANG_SECONDS):
            await runner.await_cancellation(1, "Awaited")


async def test_nothing_is_kept_for_released_runs() -> None:
    async with LocalTarget() as target:
        runner = make_runner(monitors_enabled=True)
        for index in range(3):
            await runner.run(10 + index, load_workflow(f"Load{index}", target, 0.5), {}, 2)
            await runner.run(20 + index, load_action_workflow(f"Action{index}")(), {}, 1)

        assert kept_state(runner) == {}
        assert live_samplers() == 0


async def test_a_hard_cancelled_run_reports_cancelled_and_stops_its_monitors() -> None:
    runner = make_runner(monitors_enabled=True)
    control = runner.register_run(1, "LongAction")
    running = asyncio.ensure_future(
        runner.run(1, load_action_workflow("LongAction", sleep_seconds=HANG_SECONDS * 6)(), {}, 1, control=control)
    )
    async with asyncio.timeout(HANG_SECONDS):
        while runner.run_statuses.get(1, {}).get("LongAction") != WorkflowStatus.RUNNING:
            await asyncio.sleep(0.01)

    assert runner.hard_cancel(1, "LongAction") is True
    with pytest.raises(asyncio.CancelledError):
        await running

    status, *_ = runner.get_running_workflow_stats(1, "LongAction")
    assert status == WorkflowStatus.CANCELLED
    assert control.ended.is_set()
    assert live_samplers() == 0

    runner.release_run(1, "LongAction", control)
    assert runner.get_running_workflow_stats(1, "LongAction") is None
    assert kept_state(runner) == {}


async def test_an_error_in_the_run_check_leaves_later_runs_free_to_start() -> None:
    runner = make_runner()
    # Stands in for anything that fails inside the check: its comparison raises.
    runner._max_pending_workflows = None
    with pytest.raises(TypeError):
        await runner.run(1, load_action_workflow("BrokenCheck")(), {}, 1)

    runner._max_pending_workflows = 100
    async with asyncio.timeout(HANG_SECONDS):
        _, _, _, error, status = await runner.run(2, load_action_workflow("AfterBrokenCheck")(), {}, 1)

    assert (error, status) == (None, WorkflowStatus.COMPLETED)


async def test_replacing_a_run_not_yet_set_up_works() -> None:
    runner = make_runner()
    # A run of the workflow already RUNNING but not set up yet -- no cap of
    # its own -- as a resubmission under the replace policy can find it.
    runner.run_statuses[1]["Replaced"] = WorkflowStatus.RUNNING
    async with asyncio.timeout(HANG_SECONDS):
        _, _, _, error, status = await runner.run(1, load_action_workflow("Replaced")(), {}, 1)

    assert (error, status) == (None, WorkflowStatus.COMPLETED)


async def test_a_second_run_of_a_workflow_class_runs_on_its_own_instance() -> None:
    runner = make_runner()
    workflow_class = load_action_workflow("SameClass")
    first, second = workflow_class(), workflow_class()

    await runner.run(1, first, {}, 1)
    _, results, _, error, _ = await runner.run(2, second, {}, 1)

    assert error is None
    assert results["act"] == f"instance-{id(second)}"


def test_each_setup_binds_its_own_copies_of_the_class_state_hooks() -> None:
    runner = WorkflowRunner(Env(), 1, 1, monitors_enabled=False)
    first, second = TokenProvider(), TokenProvider()

    first_actions = runner._setup_state_actions(first)
    second_actions = runner._setup_state_actions(second)

    assert first_actions["token"] is not second_actions["token"]
    assert first_actions["token"]._call.__self__ is first
    assert second_actions["token"]._call.__self__ is second
    assert not isinstance(TokenProvider.token._call, MethodType)  # the class's hook stays unbound
    # A repeat setup of an instance still finds its class's hooks.
    assert runner._setup_state_actions(first)["token"]._call.__self__ is first
