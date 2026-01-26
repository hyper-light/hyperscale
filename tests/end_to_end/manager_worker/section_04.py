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


async def validate_4_1_explicit_priority() -> None:
    spec = _build_spec(
        "manager_worker_4_1_explicit_priority",
        "4.1 Priority Classification - Explicit priority",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "Explicit priority expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_auto_priority() -> None:
    spec = _build_spec(
        "manager_worker_4_1_auto_priority",
        "4.1 Priority Classification - AUTO priority",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "AUTO priority expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_exclusive_priority() -> None:
    spec = _build_spec(
        "manager_worker_4_1_exclusive_priority",
        "4.1 Priority Classification - EXCLUSIVE priority",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "EXCLUSIVE priority expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_explicit_priority_first() -> None:
    spec = _build_spec(
        "manager_worker_4_2_explicit_priority_first",
        "4.2 Priority-Based Allocation - Explicit priority first",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "Explicit priority first expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_priority_ordering() -> None:
    spec = _build_spec(
        "manager_worker_4_2_priority_ordering",
        "4.2 Priority-Based Allocation - Priority ordering",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "Priority ordering expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_vus_tiebreaker() -> None:
    spec = _build_spec(
        "manager_worker_4_2_vus_tiebreaker",
        "4.2 Priority-Based Allocation - VUs tiebreaker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "VUs tiebreaker expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_3_proportional_by_vus() -> None:
    spec = _build_spec(
        "manager_worker_4_3_proportional_by_vus",
        "4.3 Core Distribution - Proportional by VUs",
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
            "Proportional by VUs expected core assignments"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_3_minimum_cores() -> None:
    spec = _build_spec(
        "manager_worker_4_3_minimum_cores",
        "4.3 Core Distribution - Minimum cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator is not None, "Minimum cores expected core allocator"
    finally:
        await runtime.stop_cluster()


async def validate_4_3_remaining_cores_to_auto() -> None:
    spec = _build_spec(
        "manager_worker_4_3_remaining_cores_to_auto",
        "4.3 Core Distribution - Remaining cores to AUTO",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        allocator = worker._core_allocator
        assert allocator is not None, "Remaining cores to AUTO expected core allocator"
    finally:
        await runtime.stop_cluster()


async def validate_4_4_exclusive_detection() -> None:
    spec = _build_spec(
        "manager_worker_4_4_exclusive_detection",
        "4.4 EXCLUSIVE Handling - EXCLUSIVE detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "EXCLUSIVE detection expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_4_exclusive_isolation() -> None:
    spec = _build_spec(
        "manager_worker_4_4_exclusive_isolation",
        "4.4 EXCLUSIVE Handling - EXCLUSIVE isolation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "EXCLUSIVE isolation expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_4_exclusive_completion() -> None:
    spec = _build_spec(
        "manager_worker_4_4_exclusive_completion",
        "4.4 EXCLUSIVE Handling - EXCLUSIVE completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_submissions, dict), (
            "EXCLUSIVE completion expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_4_1_explicit_priority()
    await validate_4_1_auto_priority()
    await validate_4_1_exclusive_priority()
    await validate_4_2_explicit_priority_first()
    await validate_4_2_priority_ordering()
    await validate_4_2_vus_tiebreaker()
    await validate_4_3_proportional_by_vus()
    await validate_4_3_minimum_cores()
    await validate_4_3_remaining_cores_to_auto()
    await validate_4_4_exclusive_detection()
    await validate_4_4_exclusive_isolation()
    await validate_4_4_exclusive_completion()


if __name__ == "__main__":
    asyncio.run(run())
