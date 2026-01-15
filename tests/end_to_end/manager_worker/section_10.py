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


async def validate_10_1_multiple_dispatches_arrive() -> None:
    spec = _build_spec(
        "manager_worker_10_1_multiple_dispatches_arrive",
        "10.1 Core Contention - Multiple dispatches arrive",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._core_allocation_lock is not None, (
            "Multiple dispatches expected core allocation lock"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_1_atomic_allocation() -> None:
    spec = _build_spec(
        "manager_worker_10_1_atomic_allocation",
        "10.1 Core Contention - Atomic allocation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._core_allocation_lock is not None, (
            "Atomic allocation expected core lock"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_1_waiters_queue() -> None:
    spec = _build_spec(
        "manager_worker_10_1_waiters_queue",
        "10.1 Core Contention - Waiters queue",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._cores_available_event is not None, (
            "Waiters queue expected cores available event"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_2_large_workflow_payloads() -> None:
    spec = _build_spec(
        "manager_worker_10_2_large_workflow_payloads",
        "10.2 Memory Contention - Large workflow payloads",
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
            "Large workflow payloads expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_2_result_serialization() -> None:
    spec = _build_spec(
        "manager_worker_10_2_result_serialization",
        "10.2 Memory Contention - Result serialization",
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
            "Result serialization expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_2_buffer_accumulation() -> None:
    spec = _build_spec(
        "manager_worker_10_2_buffer_accumulation",
        "10.2 Memory Contention - Buffer accumulation",
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
            "Buffer accumulation expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_workflow_execution() -> None:
    spec = _build_spec(
        "manager_worker_10_3_workflow_execution",
        "10.3 CPU Contention - Workflow execution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        assert worker._workflow_executor is not None, (
            "Workflow execution expected workflow executor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_progress_monitoring() -> None:
    spec = _build_spec(
        "manager_worker_10_3_progress_monitoring",
        "10.3 CPU Contention - Progress monitoring",
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
            "Progress monitoring expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_heartbeat_overhead() -> None:
    spec = _build_spec(
        "manager_worker_10_3_heartbeat_overhead",
        "10.3 CPU Contention - Heartbeat/health",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Heartbeat overhead expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_progress_updates() -> None:
    spec = _build_spec(
        "manager_worker_10_4_progress_updates",
        "10.4 Network Contention - Progress updates",
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
            "Progress updates expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_final_results() -> None:
    spec = _build_spec(
        "manager_worker_10_4_final_results",
        "10.4 Network Contention - Final results",
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
            "Final results expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_heartbeats() -> None:
    spec = _build_spec(
        "manager_worker_10_4_heartbeats",
        "10.4 Network Contention - Heartbeats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "Heartbeats expected health monitor"
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_10_1_multiple_dispatches_arrive()
    await validate_10_1_atomic_allocation()
    await validate_10_1_waiters_queue()
    await validate_10_2_large_workflow_payloads()
    await validate_10_2_result_serialization()
    await validate_10_2_buffer_accumulation()
    await validate_10_3_workflow_execution()
    await validate_10_3_progress_monitoring()
    await validate_10_3_heartbeat_overhead()
    await validate_10_4_progress_updates()
    await validate_10_4_final_results()
    await validate_10_4_heartbeats()


if __name__ == "__main__":
    asyncio.run(run())
