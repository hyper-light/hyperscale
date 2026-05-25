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
    ServerHandle,
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


def _worker_handle_by_node_id(
    cluster: ClusterHarness,
    dc_id: str,
    worker_id: str,
) -> ServerHandle | None:
    return next(
        (
            worker
            for worker in cluster.workers(dc_id)
            if worker.instance._node_id.full == worker_id
        ),
        None,
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
        manager = cluster.managers("local")[0]
        dispatcher = manager.instance._workflow_dispatcher
        original_send_dispatch = dispatcher._send_dispatch
        fault_landed = False

        async def reset_dispatch_to_selected_worker(
            worker_id: str,
            dispatch: WorkflowDispatch,
        ) -> bool:
            nonlocal fault_landed
            if not fault_landed:
                selected_worker = _worker_handle_by_node_id(
                    cluster,
                    "local",
                    worker_id,
                )
                if selected_worker is not None:
                    await cluster.faults.tcp_reset(
                        src=manager,
                        dst=selected_worker,
                        action="workflow_dispatch",
                        count=1,
                    )
                    await cluster.faults.kill(selected_worker)
                    fault_landed = True

            return await original_send_dispatch(worker_id, dispatch)

        dispatcher._send_dispatch = reset_dispatch_to_selected_worker
        try:
            async with cluster.workload(_workload(SimpleWorkflow, 45.0)) as driver:
                await driver.submit_and_wait()
        finally:
            dispatcher._send_dispatch = original_send_dispatch

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
        dispatcher = manager.instance._workflow_dispatcher
        original_send_dispatch = dispatcher._send_dispatch
        fault_landed = False
        execution_deferred = False

        async def send_dispatch_then_kill(
            worker_id: str,
            dispatch: WorkflowDispatch,
        ) -> bool:
            nonlocal execution_deferred, fault_landed
            selected_worker = _worker_handle_by_node_id(
                cluster,
                "local",
                worker_id,
            )
            if selected_worker is None or fault_landed:
                return await original_send_dispatch(worker_id, dispatch)

            original_task_runner_run = selected_worker.instance._task_runner.run
            selected_dispatch_deferred = False

            def defer_workflow_execution(
                call: object,
                *args: object,
                alias: str | None = None,
                **kwargs: object,
            ) -> object:
                nonlocal execution_deferred, selected_dispatch_deferred
                if alias is not None and alias.startswith("workflow:"):
                    execution_deferred = True
                    selected_dispatch_deferred = True
                    return SimpleNamespace(token=f"harness-deferred:{alias}")

                return original_task_runner_run(call, *args, alias=alias, **kwargs)

            selected_worker.instance._task_runner.run = defer_workflow_execution
            try:
                accepted = await original_send_dispatch(worker_id, dispatch)
            finally:
                if (
                    selected_worker.instance._task_runner.run
                    is defer_workflow_execution
                ):
                    selected_worker.instance._task_runner.run = (
                        original_task_runner_run
                    )

            if accepted and selected_dispatch_deferred and not fault_landed:
                fault_landed = True
                await cluster.faults.kill(selected_worker)

            return accepted

        dispatcher._send_dispatch = send_dispatch_then_kill
        try:
            async with cluster.workload(_workload(LongRunningWorkflow, 75.0)) as driver:
                await driver.submit()
                await driver.wait_until_running(timeout=30.0)
                await driver.wait_for_completion()
        finally:
            dispatcher._send_dispatch = original_send_dispatch

        assert fault_landed is True
        assert execution_deferred is True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_worker_dies_mid_execute() -> None:
    """Worker is actively executing the workflow (post-ack, before result)
    when it dies; cancellation propagation must surface or redispatch.

    Distinct from ``test_worker_dies_post_ack_before_workload_finishes``
    (which intercepts the workflow task before execute starts) and
    ``test_worker_dies_post_execute_before_result_push`` (which lets
    execute complete and intercepts the result push). Here the workflow
    is genuinely in flight — the ``LongRunningWorkflow`` step is part-
    way through its ``asyncio.sleep(30)`` when the kill lands.

    Targets the same worker the dispatch landed on (captured via the
    ``_send_dispatch`` hook used by the post-ack timing test) so the
    fault is deterministic rather than relying on a 2-worker 50/50
    roll. The peer worker must pick the workflow up; ``LongRunningWorkflow``
    is restart-from-scratch (per SCENARIOS.md §2 contract), so the
    completion budget covers one full re-execution plus dispatch.
    """
    spec = _l1_spec(base_port=38250)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_dies_mid_execute",
    ) as cluster:
        manager = cluster.managers("local")[0]
        dispatcher = manager.instance._workflow_dispatcher
        original_send_dispatch = dispatcher._send_dispatch
        dispatched_worker_id: str | None = None
        dispatch_ack_event = asyncio.Event()

        async def capture_dispatch_target(
            worker_id: str,
            dispatch: WorkflowDispatch,
        ) -> bool:
            nonlocal dispatched_worker_id
            accepted = await original_send_dispatch(worker_id, dispatch)
            if accepted and dispatched_worker_id is None:
                dispatched_worker_id = worker_id
                dispatch_ack_event.set()
            return accepted

        dispatcher._send_dispatch = capture_dispatch_target

        async with cluster.workload(_workload(LongRunningWorkflow, 90.0)) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)
            await asyncio.wait_for(dispatch_ack_event.wait(), timeout=10.0)

            # Wait long enough that execute() is genuinely mid-sleep,
            # not still spinning up. LongRunningWorkflow.duration is
            # 30s, so 5s is ~16% through the execute window — well
            # past startup, well before completion.
            await asyncio.sleep(5.0)

            victim = next(
                worker
                for worker in cluster.workers("local")
                if worker.instance._node_id.full == dispatched_worker_id
            )
            await cluster.faults.kill(victim)

            await driver.wait_for_completion()


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
        manager = cluster.managers("local")[0]
        dispatcher = manager.instance._workflow_dispatcher
        original_send_dispatch = dispatcher._send_dispatch
        fault_landed = False
        patched_worker_id: str | None = None

        async def drop_first_final_result_from_selected_worker(
            worker_id: str,
            dispatch: WorkflowDispatch,
        ) -> bool:
            nonlocal patched_worker_id, fault_landed
            if patched_worker_id is None:
                selected_worker = _worker_handle_by_node_id(
                    cluster,
                    "local",
                    worker_id,
                )
                if selected_worker is not None:
                    patched_worker_id = worker_id
                    original_send_tcp = selected_worker.instance.send_tcp

                    async def reset_final_result_send(
                        address: tuple[str, int],
                        action: str,
                        data: object,
                        timeout: int | float | None = None,
                    ) -> tuple[object, int]:
                        nonlocal fault_landed
                        if action == "workflow_final_result" and not fault_landed:
                            fault_landed = True
                            if selected_worker.instance.send_tcp is reset_final_result_send:
                                selected_worker.instance.send_tcp = original_send_tcp
                            clock = getattr(selected_worker.instance._tcp_clock, "time", 0)
                            await cluster.faults.kill(selected_worker)
                            return (
                                ConnectionResetError(
                                    "harness reset final result before manager ACK"
                                ),
                                clock,
                            )

                        return await original_send_tcp(
                            address,
                            action,
                            data,
                            timeout=timeout,
                        )

                    selected_worker.instance.send_tcp = reset_final_result_send

            return await original_send_dispatch(worker_id, dispatch)

        dispatcher._send_dispatch = drop_first_final_result_from_selected_worker
        try:
            async with cluster.workload(_workload(SimpleWorkflow, 60.0)) as driver:
                await driver.submit_and_wait()
        finally:
            dispatcher._send_dispatch = original_send_dispatch

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
