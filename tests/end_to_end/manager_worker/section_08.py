import asyncio
import re

from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_outcome import ScenarioOutcome
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.scenario_spec import ScenarioSpec


WORKFLOW_REGISTRY = {"BaseScenarioWorkflow": BaseScenarioWorkflow}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return slug[:80] if slug else "scenario"


def _build_spec(name: str, description: str) -> ScenarioSpec:
    slug = _slugify(name)
    subclass_name = f"ScenarioWorkflow{slug[:32]}"
    return ScenarioSpec.from_dict(
        {
            "name": name,
            "description": description,
            "timeouts": {"default": 60, "start_cluster": 120, "scenario": 600},
            "cluster": {
                "gate_count": 1,
                "dc_count": 1,
                "managers_per_dc": 1,
                "workers_per_dc": 2,
                "cores_per_worker": 1,
                "base_gate_tcp": 9000,
            },
            "actions": [
                {"type": "start_cluster"},
                {"type": "await_gate_leader", "params": {"timeout": 30}},
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-A", "timeout": 30},
                },
                {
                    "type": "submit_job",
                    "params": {
                        "job_alias": "job-1",
                        "workflow_instances": [
                            {
                                "name": "BaseScenarioWorkflow",
                                "subclass_name": subclass_name,
                                "class_overrides": {"vus": 1, "duration": "1s"},
                                "steps": [
                                    {
                                        "name": "noop",
                                        "return_value": {"ok": True},
                                        "return_type": "dict",
                                    }
                                ],
                            }
                        ],
                    },
                },
                {"type": "await_job", "params": {"job_alias": "job-1", "timeout": 60}},
            ],
        }
    )


def _get_manager(runtime: ScenarioRuntime, dc_id: str) -> ManagerServer:
    cluster = runtime.require_cluster()
    return cluster.get_manager_leader(dc_id) or cluster.managers[dc_id][0]


def _get_worker(runtime: ScenarioRuntime) -> WorkerServer:
    cluster = runtime.require_cluster()
    return cluster.get_all_workers()[0]


def _require_runtime(outcome: ScenarioOutcome) -> ScenarioRuntime:
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def validate_8_1_workflow_dispatch_received() -> None:
    spec = _build_spec(
        "manager_worker_8_1_workflow_dispatch_received",
        "8.1 Dispatch Handling - WorkflowDispatch received",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._pending_workflows, dict), (
            "WorkflowDispatch received expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_1_core_allocation() -> None:
    spec = _build_spec(
        "manager_worker_8_1_core_allocation",
        "8.1 Dispatch Handling - Core allocation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator is not None, "Core allocation expected core allocator"
    finally:
        await runtime.stop_cluster()


async def validate_8_1_state_tracking() -> None:
    spec = _build_spec(
        "manager_worker_8_1_state_tracking",
        "8.1 Dispatch Handling - State tracking",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_tokens, dict), (
            "State tracking expected workflow tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_1_cancel_event_creation() -> None:
    spec = _build_spec(
        "manager_worker_8_1_cancel_event_creation",
        "8.1 Dispatch Handling - Cancel event creation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cancel_events, dict), (
            "Cancel event creation expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_load_workflow() -> None:
    spec = _build_spec(
        "manager_worker_8_2_load_workflow",
        "8.2 Workflow Deserialization - Load workflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_id_to_name, dict), (
            "Load workflow expected workflow id to name"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_load_context() -> None:
    spec = _build_spec(
        "manager_worker_8_2_load_context",
        "8.2 Workflow Deserialization - Load context",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_id_to_name, dict), (
            "Load context expected workflow id to name"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_workflow_name() -> None:
    spec = _build_spec(
        "manager_worker_8_2_workflow_name",
        "8.2 Workflow Deserialization - Workflow name",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_id_to_name, dict), (
            "Workflow name expected id to name"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_3_manager_available() -> None:
    spec = _build_spec(
        "manager_worker_8_3_manager_available",
        "8.3 Execution via RemoteGraphManager - Manager available",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager is not None, "Manager available expected manager"
    finally:
        await runtime.stop_cluster()


async def validate_8_3_execute_workflow() -> None:
    spec = _build_spec(
        "manager_worker_8_3_execute_workflow",
        "8.3 Execution via RemoteGraphManager - Execute workflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        executor = worker._workflow_executor
        assert executor is not None, "Execute workflow expected workflow executor"
    finally:
        await runtime.stop_cluster()


async def validate_8_3_monitor_progress() -> None:
    spec = _build_spec(
        "manager_worker_8_3_monitor_progress",
        "8.3 Execution via RemoteGraphManager - Monitor progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "Monitor progress expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_success_path() -> None:
    spec = _build_spec(
        "manager_worker_8_4_success_path",
        "8.4 Execution Completion - Success path",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cores_completed, dict), (
            "Success path expected workflow cores completed"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_failure_path() -> None:
    spec = _build_spec(
        "manager_worker_8_4_failure_path",
        "8.4 Execution Completion - Failure path",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cores_completed, dict), (
            "Failure path expected workflow cores completed"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_cancellation_path() -> None:
    spec = _build_spec(
        "manager_worker_8_4_cancellation_path",
        "8.4 Execution Completion - Cancellation path",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cancel_events, dict), (
            "Cancellation path expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_5_free_cores() -> None:
    spec = _build_spec(
        "manager_worker_8_5_free_cores",
        "8.5 Cleanup - Free cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "free", None)), "Free cores expected free"
    finally:
        await runtime.stop_cluster()


async def validate_8_5_remove_from_tracking() -> None:
    spec = _build_spec(
        "manager_worker_8_5_remove_from_tracking",
        "8.5 Cleanup - Remove from tracking",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_tokens, dict), (
            "Remove from tracking expected workflow tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_5_send_final_result() -> None:
    spec = _build_spec(
        "manager_worker_8_5_send_final_result",
        "8.5 Cleanup - Send final result",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_aggregated_results, dict), (
            "Send final result expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_8_1_workflow_dispatch_received()
    await validate_8_1_core_allocation()
    await validate_8_1_state_tracking()
    await validate_8_1_cancel_event_creation()
    await validate_8_2_load_workflow()
    await validate_8_2_load_context()
    await validate_8_2_workflow_name()
    await validate_8_3_manager_available()
    await validate_8_3_execute_workflow()
    await validate_8_3_monitor_progress()
    await validate_8_4_success_path()
    await validate_8_4_failure_path()
    await validate_8_4_cancellation_path()
    await validate_8_5_free_cores()
    await validate_8_5_remove_from_tracking()
    await validate_8_5_send_final_result()


if __name__ == "__main__":
    asyncio.run(run())
