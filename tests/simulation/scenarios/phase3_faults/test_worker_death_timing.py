"""
Phase 3 worker lifecycle timing scenarios from ``docs/SCENARIOS.md`` §2.

The tests place worker death at distinct dispatch/result boundaries so the
manager exercises unstarted-dispatch cleanup, orphan reclaim, lost-result
reassignment, and same/new-incarnation rejoin handling.
"""

import asyncio
from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import WorkflowDispatch
from hyperscale.distributed.testing.workflows import LongRunningWorkflow, SimpleWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    ExpectCompletionWithin,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    manager_has_n_workers,
    wait_until,
)


def _l1_spec(base_port: int, workers: int = 2) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(managers=1, workers=workers, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _workload(workflow_factory: type, timeout_seconds: float) -> WorkloadSpec:
    workflow_name = workflow_factory.__name__
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], workflow_factory)],
                dc_count=1,
                timeout_seconds=timeout_seconds,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            ExpectAllWorkflowsComplete(expected_workflow_names=[workflow_name]),
            ExpectCompletionWithin(seconds=timeout_seconds),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_dies_mid_dispatch_before_ack() -> None:
    """TCP dispatch loses the target worker before ack; manager dispatches remaining work."""
    spec = _l1_spec(base_port=36000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_dies_mid_dispatch_before_ack",
    ) as cluster:
        victim = cluster.workers("local")[0]
        original_dispatch_handle = victim.instance._dispatch_handler.handle
        fault_landed = False

        async def cancel_before_ack(
            addr: tuple[str, int],
            data: bytes,
            clock_time: int,
        ) -> bytes:
            nonlocal fault_landed
            if not fault_landed:
                fault_landed = True
                await cluster.faults.kill(victim)
                raise asyncio.CancelledError("harness cancelled dispatch before ack")
            return await original_dispatch_handle(addr, data, clock_time)

        victim.instance._dispatch_handler.handle = cancel_before_ack
        async with cluster.workload(_workload(SimpleWorkflow, 45.0)) as driver:
            await driver.submit_and_wait()

        assert fault_landed is True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_dies_post_ack_before_workload_finishes() -> None:
    """Worker accepts dispatch, then dies before producing a final result."""
    spec = _l1_spec(base_port=37500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_dies_post_ack_before_workload_finishes",
    ) as cluster:
        manager = cluster.managers("local")[0]
        victim = cluster.workers("local")[0]
        victim_id = victim.instance._node_id.full
        dispatcher = manager.instance._workflow_dispatcher
        original_send_dispatch = dispatcher._send_dispatch
        original_task_runner_run = victim.instance._task_runner.run
        fault_landed = False
        execution_deferred = False

        def defer_workflow_execution(
            call: object,
            *args: object,
            alias: str | None = None,
            **kwargs: object,
        ) -> object:
            nonlocal execution_deferred
            if alias is not None and alias.startswith("workflow:") and not execution_deferred:
                execution_deferred = True
                return SimpleNamespace(token=f"harness-deferred:{alias}")

            return original_task_runner_run(call, *args, alias=alias, **kwargs)

        async def send_dispatch_then_kill(
            worker_id: str,
            dispatch: WorkflowDispatch,
        ) -> bool:
            nonlocal fault_landed
            accepted = await original_send_dispatch(worker_id, dispatch)
            if (
                worker_id == victim_id
                and accepted
                and execution_deferred
                and not fault_landed
            ):
                fault_landed = True
                await cluster.faults.kill(victim)
            return accepted

        victim.instance._task_runner.run = defer_workflow_execution
        dispatcher._send_dispatch = send_dispatch_then_kill
        async with cluster.workload(_workload(LongRunningWorkflow, 75.0)) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)
            await driver.wait_for_completion()

        assert fault_landed is True
        assert execution_deferred is True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_dies_post_execute_before_result_push() -> None:
    """Worker finishes execution but dies before pushing its final result."""
    spec = _l1_spec(base_port=39000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_dies_post_execute_before_result_push",
    ) as cluster:
        victim = cluster.workers("local")[0]
        original_send_final = victim.instance._progress_reporter.send_final_result
        fault_landed = False

        async def lose_result_before_send(**kwargs) -> None:
            nonlocal fault_landed
            if not fault_landed:
                fault_landed = True
                await cluster.faults.kill(victim)
                return
            await original_send_final(**kwargs)

        victim.instance._progress_reporter.send_final_result = lose_result_before_send
        async with cluster.workload(_workload(SimpleWorkflow, 60.0)) as driver:
            await driver.submit_and_wait()

        assert fault_landed is True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_rejoins_same_process_refutes_stale_incarnation() -> None:
    """Paused worker resumes with the same identity and clears stale suspicion."""
    spec = _l1_spec(base_port=40500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_rejoins_same_process_refutes_stale_incarnation",
    ) as cluster:
        manager = cluster.managers("local")[0]
        worker = cluster.workers("local")[0]
        original_node_id = worker.instance._node_id.full
        original_incarnation = worker.instance._incarnation_tracker.self_incarnation

        await cluster.faults.pause(worker)
        await wait_until(
            lambda: manager.instance._manager_state.get_worker_count() <= 1,
            timeout=60.0,
            poll=0.5,
            description="paused worker is removed from manager registry",
        )

        await cluster.faults.resume(worker)
        await wait_until(
            manager_has_n_workers(manager, 2),
            timeout=60.0,
            poll=0.5,
            description="resumed worker re-registers with same process identity",
        )

        assert worker.instance._node_id.full == original_node_id
        assert worker.instance._incarnation_tracker.self_incarnation >= original_incarnation


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_rejoins_new_incarnation_after_restart() -> None:
    """Killed worker restarts at the same ports with a fresh node incarnation."""
    spec = _l1_spec(base_port=42000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_rejoins_new_incarnation_after_restart",
    ) as cluster:
        manager = cluster.managers("local")[0]
        worker = cluster.workers("local")[0]
        original_node_id = worker.instance._node_id.full

        await cluster.faults.kill(worker)
        await wait_until(
            lambda: manager.instance._manager_state.get_worker_count() <= 1,
            timeout=60.0,
            poll=0.5,
            description="killed worker is removed from manager registry",
        )

        await cluster.faults.restart(worker)
        await wait_until(
            manager_has_n_workers(manager, 2),
            timeout=60.0,
            poll=0.5,
            description="restarted worker registers with new incarnation",
        )

        assert worker.instance._node_id.full != original_node_id
