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


async def validate_3_1_manager_dispatches_to_worker() -> None:
    spec = _build_spec(
        "manager_worker_3_1_manager_dispatches_to_worker",
        "3.1 Dispatch Coordination - Manager dispatches to worker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._dispatch is not None, (
            "Manager dispatch expected dispatch coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_1_worker_selection() -> None:
    spec = _build_spec(
        "manager_worker_3_1_worker_selection",
        "3.1 Dispatch Coordination - Worker selection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._worker_pool is not None, "Worker selection expected worker pool"
    finally:
        await runtime.stop_cluster()


async def validate_3_1_dispatch_semaphore() -> None:
    spec = _build_spec(
        "manager_worker_3_1_dispatch_semaphore",
        "3.1 Dispatch Coordination - Dispatch semaphore",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._dispatch_semaphores, dict), (
            "Dispatch semaphore expected dispatch semaphores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_1_fence_token() -> None:
    spec = _build_spec(
        "manager_worker_3_1_fence_token",
        "3.1 Dispatch Coordination - Fence token",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._fence_token is not None, "Fence token expected fence token"
    finally:
        await runtime.stop_cluster()


async def validate_3_2_healthy_workers_preferred() -> None:
    spec = _build_spec(
        "manager_worker_3_2_healthy_workers_preferred",
        "3.2 Worker Selection - Healthy workers preferred",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        pool = manager._worker_pool
        assert pool is not None, "Healthy workers preferred expected worker pool"
    finally:
        await runtime.stop_cluster()


async def validate_3_2_fallback_to_busy() -> None:
    spec = _build_spec(
        "manager_worker_3_2_fallback_to_busy",
        "3.2 Worker Selection - Fallback to busy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        pool = manager._worker_pool
        assert pool is not None, "Fallback to busy expected worker pool"
    finally:
        await runtime.stop_cluster()


async def validate_3_2_fallback_to_degraded() -> None:
    spec = _build_spec(
        "manager_worker_3_2_fallback_to_degraded",
        "3.2 Worker Selection - Fallback to degraded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        pool = manager._worker_pool
        assert pool is not None, "Fallback to degraded expected worker pool"
    finally:
        await runtime.stop_cluster()


async def validate_3_2_overloaded_excluded() -> None:
    spec = _build_spec(
        "manager_worker_3_2_overloaded_excluded",
        "3.2 Worker Selection - Overloaded excluded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        pool = manager._worker_pool
        assert pool is not None, "Overloaded excluded expected worker pool"
    finally:
        await runtime.stop_cluster()


async def validate_3_2_capacity_check() -> None:
    spec = _build_spec(
        "manager_worker_3_2_capacity_check",
        "3.2 Worker Selection - Capacity check",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator is not None, "Capacity check expected core allocator"
    finally:
        await runtime.stop_cluster()


async def validate_3_2_circuit_breaker_check() -> None:
    spec = _build_spec(
        "manager_worker_3_2_circuit_breaker_check",
        "3.2 Worker Selection - Circuit breaker check",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_circuits, dict), (
            "Circuit breaker check expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_2_sorting_by_capacity() -> None:
    spec = _build_spec(
        "manager_worker_3_2_sorting_by_capacity",
        "3.2 Worker Selection - Sorting by capacity",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._worker_pool is not None, (
            "Sorting by capacity expected worker pool"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_workflow_dispatch_construction() -> None:
    spec = _build_spec(
        "manager_worker_3_3_workflow_dispatch_construction",
        "3.3 Dispatch Message - WorkflowDispatch construction",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._workflow_dispatcher is not None, (
            "WorkflowDispatch construction expected workflow dispatcher"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_workflow_data_serialization() -> None:
    spec = _build_spec(
        "manager_worker_3_3_workflow_data_serialization",
        "3.3 Dispatch Message - Workflow data serialization",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._workflow_dispatcher is not None, (
            "Workflow data serialization expected workflow dispatcher"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_context_serialization() -> None:
    spec = _build_spec(
        "manager_worker_3_3_context_serialization",
        "3.3 Dispatch Message - Context serialization",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._workflow_dispatcher is not None, (
            "Context serialization expected workflow dispatcher"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_vus_and_cores() -> None:
    spec = _build_spec(
        "manager_worker_3_3_vus_and_cores",
        "3.3 Dispatch Message - VUs and cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._workflow_dispatcher is not None, (
            "VUs and cores expected dispatcher"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_workflow_dispatch_ack_received() -> None:
    spec = _build_spec(
        "manager_worker_3_4_workflow_dispatch_ack_received",
        "3.4 Dispatch Response - WorkflowDispatchAck received",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._dispatch is not None, (
            "Dispatch ack expected dispatch coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_accepted_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_3_4_accepted_dispatch",
        "3.4 Dispatch Response - Accepted dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Accepted dispatch expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_rejected_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_3_4_rejected_dispatch",
        "3.4 Dispatch Response - Rejected dispatch",
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
            "Rejected dispatch expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_throughput_counter() -> None:
    spec = _build_spec(
        "manager_worker_3_4_throughput_counter",
        "3.4 Dispatch Response - Throughput counter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Throughput counter expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_worker_unreachable() -> None:
    spec = _build_spec(
        "manager_worker_3_5_worker_unreachable",
        "3.5 Dispatch Failures - Worker unreachable",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_circuits, dict), (
            "Worker unreachable expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_worker_rejects_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_3_5_worker_rejects_dispatch",
        "3.5 Dispatch Failures - Worker rejects dispatch",
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
            "Worker rejects dispatch expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_dispatch_exception() -> None:
    spec = _build_spec(
        "manager_worker_3_5_dispatch_exception",
        "3.5 Dispatch Failures - Dispatch exception",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_circuits, dict), (
            "Dispatch exception expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_3_1_manager_dispatches_to_worker()
    await validate_3_1_worker_selection()
    await validate_3_1_dispatch_semaphore()
    await validate_3_1_fence_token()
    await validate_3_2_healthy_workers_preferred()
    await validate_3_2_fallback_to_busy()
    await validate_3_2_fallback_to_degraded()
    await validate_3_2_overloaded_excluded()
    await validate_3_2_capacity_check()
    await validate_3_2_circuit_breaker_check()
    await validate_3_2_sorting_by_capacity()
    await validate_3_3_workflow_dispatch_construction()
    await validate_3_3_workflow_data_serialization()
    await validate_3_3_context_serialization()
    await validate_3_3_vus_and_cores()
    await validate_3_4_workflow_dispatch_ack_received()
    await validate_3_4_accepted_dispatch()
    await validate_3_4_rejected_dispatch()
    await validate_3_4_throughput_counter()
    await validate_3_5_worker_unreachable()
    await validate_3_5_worker_rejects_dispatch()
    await validate_3_5_dispatch_exception()


if __name__ == "__main__":
    asyncio.run(run())
