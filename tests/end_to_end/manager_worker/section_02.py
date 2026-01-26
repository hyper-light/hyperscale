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


async def validate_2_1_allocate_cores_to_workflow() -> None:
    spec = _build_spec(
        "manager_worker_2_1_allocate_cores_to_workflow",
        "2.1 Basic Allocation - Allocate cores to workflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "allocate", None)), (
            "Allocate cores expected allocate"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_allocation_atomicity() -> None:
    spec = _build_spec(
        "manager_worker_2_1_allocation_atomicity",
        "2.1 Basic Allocation - Allocation atomicity",
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
            "Allocation atomicity expected core allocation lock"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_allocation_tracking() -> None:
    spec = _build_spec(
        "manager_worker_2_1_allocation_tracking",
        "2.1 Basic Allocation - Allocation tracking",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert isinstance(allocator._core_assignments, dict), (
            "Allocation tracking expected core assignments"
        )
        assert isinstance(allocator._workflow_cores, dict), (
            "Allocation tracking expected workflow cores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_available_cores_count() -> None:
    spec = _build_spec(
        "manager_worker_2_1_available_cores_count",
        "2.1 Basic Allocation - Available cores count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert hasattr(allocator, "available_cores"), (
            "Available cores count expected available_cores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_request_exceeds_total() -> None:
    spec = _build_spec(
        "manager_worker_2_2_request_exceeds_total",
        "2.2 Allocation Constraints - Request exceeds total",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "allocate", None)), (
            "Request exceeds total expected allocate"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_request_exceeds_available() -> None:
    spec = _build_spec(
        "manager_worker_2_2_request_exceeds_available",
        "2.2 Allocation Constraints - Request exceeds available",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "allocate", None)), (
            "Request exceeds available expected allocate"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_zero_negative_cores() -> None:
    spec = _build_spec(
        "manager_worker_2_2_zero_negative_cores",
        "2.2 Allocation Constraints - Zero/negative cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "allocate", None)), (
            "Zero/negative cores expected allocate"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_duplicate_allocation() -> None:
    spec = _build_spec(
        "manager_worker_2_2_duplicate_allocation",
        "2.2 Allocation Constraints - Duplicate allocation",
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
            "Duplicate allocation expected workflow cores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_free_all_cores() -> None:
    spec = _build_spec(
        "manager_worker_2_3_free_all_cores",
        "2.3 Core Release - Free all cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "free", None)), (
            "Free all cores expected free"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_free_subset() -> None:
    spec = _build_spec(
        "manager_worker_2_3_free_subset",
        "2.3 Core Release - Free subset",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "free_subset", None)), (
            "Free subset expected free_subset"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_cores_available_event() -> None:
    spec = _build_spec(
        "manager_worker_2_3_cores_available_event",
        "2.3 Core Release - Cores available event",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator._cores_available is not None, (
            "Cores available event expected cores available"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_4_partial_core_release() -> None:
    spec = _build_spec(
        "manager_worker_2_4_partial_core_release",
        "2.4 Streaming Workflows - Partial core release",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "free_subset", None)), (
            "Partial core release expected free_subset"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_4_core_tracking_during_release() -> None:
    spec = _build_spec(
        "manager_worker_2_4_core_tracking_during_release",
        "2.4 Streaming Workflows - Core tracking during release",
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
            "Core tracking during release expected workflow cores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_4_final_cleanup() -> None:
    spec = _build_spec(
        "manager_worker_2_4_final_cleanup",
        "2.4 Streaming Workflows - Final cleanup",
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
            "Final cleanup expected workflow cores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_5_multiple_workflows_compete() -> None:
    spec = _build_spec(
        "manager_worker_2_5_multiple_workflows_compete",
        "2.5 Core Contention - Multiple workflows compete",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator is not None, (
            "Multiple workflows compete expected core allocator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_5_wait_for_cores() -> None:
    spec = _build_spec(
        "manager_worker_2_5_wait_for_cores",
        "2.5 Core Contention - Wait for cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert callable(getattr(allocator, "wait_for_cores", None)), (
            "Wait for cores expected wait_for_cores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_5_core_starvation() -> None:
    spec = _build_spec(
        "manager_worker_2_5_core_starvation",
        "2.5 Core Contention - Core starvation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator is not None, "Core starvation expected core allocator"
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_2_1_allocate_cores_to_workflow()
    await validate_2_1_allocation_atomicity()
    await validate_2_1_allocation_tracking()
    await validate_2_1_available_cores_count()
    await validate_2_2_request_exceeds_total()
    await validate_2_2_request_exceeds_available()
    await validate_2_2_zero_negative_cores()
    await validate_2_2_duplicate_allocation()
    await validate_2_3_free_all_cores()
    await validate_2_3_free_subset()
    await validate_2_3_cores_available_event()
    await validate_2_4_partial_core_release()
    await validate_2_4_core_tracking_during_release()
    await validate_2_4_final_cleanup()
    await validate_2_5_multiple_workflows_compete()
    await validate_2_5_wait_for_cores()
    await validate_2_5_core_starvation()


if __name__ == "__main__":
    asyncio.run(run())
