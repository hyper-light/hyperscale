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


async def validate_6_1_detection() -> None:
    spec = _build_spec(
        "manager_worker_6_1_detection",
        "6.1 Worker Dies Mid-Workflow - Detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_deadlines, dict), (
            "Detection expected worker deadlines"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_1_workflow_orphaned() -> None:
    spec = _build_spec(
        "manager_worker_6_1_workflow_orphaned",
        "6.1 Worker Dies Mid-Workflow - Workflow orphaned",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Workflow orphaned expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_1_grace_period() -> None:
    spec = _build_spec(
        "manager_worker_6_1_grace_period",
        "6.1 Worker Dies Mid-Workflow - Grace period",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Grace period expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_1_reschedule() -> None:
    spec = _build_spec(
        "manager_worker_6_1_reschedule",
        "6.1 Worker Dies Mid-Workflow - Reschedule",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Reschedule expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_dispatch_timeout() -> None:
    spec = _build_spec(
        "manager_worker_6_2_dispatch_timeout",
        "6.2 Worker Dies Before ACK - Dispatch timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Dispatch timeout expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_retry_to_another_worker() -> None:
    spec = _build_spec(
        "manager_worker_6_2_retry_to_another_worker",
        "6.2 Worker Dies Before ACK - Retry to another worker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Retry to another worker expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_all_workers_fail() -> None:
    spec = _build_spec(
        "manager_worker_6_2_all_workers_fail",
        "6.2 Worker Dies Before ACK - All workers fail",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "All workers fail expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_3_result_not_received() -> None:
    spec = _build_spec(
        "manager_worker_6_3_result_not_received",
        "6.3 Worker Dies After Completion - Result not received",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_timeout_strategies, dict), (
            "Result not received expected timeout strategies"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_3_timeout_detection() -> None:
    spec = _build_spec(
        "manager_worker_6_3_timeout_detection",
        "6.3 Worker Dies After Completion - Timeout detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_timeout_strategies, dict), (
            "Timeout detection expected timeout strategies"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_3_status_reconciliation() -> None:
    spec = _build_spec(
        "manager_worker_6_3_status_reconciliation",
        "6.3 Worker Dies After Completion - Status reconciliation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_lifecycle_states, dict), (
            "Status reconciliation expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_4_some_cores_fail() -> None:
    spec = _build_spec(
        "manager_worker_6_4_some_cores_fail",
        "6.4 Partial Failure - Some cores fail",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_lifecycle_states, dict), (
            "Some cores fail expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_4_partial_results() -> None:
    spec = _build_spec(
        "manager_worker_6_4_partial_results",
        "6.4 Partial Failure - Partial results",
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
            "Partial results expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_4_core_cleanup() -> None:
    spec = _build_spec(
        "manager_worker_6_4_core_cleanup",
        "6.4 Partial Failure - Core cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert isinstance(allocator._workflow_cores, dict), (
            "Core cleanup expected workflow cores"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_6_1_detection()
    await validate_6_1_workflow_orphaned()
    await validate_6_1_grace_period()
    await validate_6_1_reschedule()
    await validate_6_2_dispatch_timeout()
    await validate_6_2_retry_to_another_worker()
    await validate_6_2_all_workers_fail()
    await validate_6_3_result_not_received()
    await validate_6_3_timeout_detection()
    await validate_6_3_status_reconciliation()
    await validate_6_4_some_cores_fail()
    await validate_6_4_partial_results()
    await validate_6_4_core_cleanup()


if __name__ == "__main__":
    asyncio.run(run())
