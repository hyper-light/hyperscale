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


async def validate_1_1_worker_registers_with_manager() -> None:
    spec = _build_spec(
        "manager_worker_1_1_worker_registers_with_manager",
        "1.1 Registration Flow - Worker registers with manager",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workers, dict), (
            "Worker registers expected workers state"
        )
        assert isinstance(state._worker_addr_to_id, dict), (
            "Worker registers expected worker addr map"
        )
        assert isinstance(state._worker_circuits, dict), (
            "Worker registers expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_1_registration_with_core_count() -> None:
    spec = _build_spec(
        "manager_worker_1_1_registration_with_core_count",
        "1.1 Registration Flow - Registration with core count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workers, dict), (
            "Registration core count expected workers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_1_registration_with_health_state() -> None:
    spec = _build_spec(
        "manager_worker_1_1_registration_with_health_state",
        "1.1 Registration Flow - Registration with health state",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_health_states, dict), (
            "Registration health state expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_1_reregistration_after_restart() -> None:
    spec = _build_spec(
        "manager_worker_1_1_reregistration_after_restart",
        "1.1 Registration Flow - Re-registration after restart",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workers, dict), (
            "Re-registration expected workers state"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_1_registration_from_unknown_worker() -> None:
    spec = _build_spec(
        "manager_worker_1_1_registration_from_unknown_worker",
        "1.1 Registration Flow - Registration from unknown worker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workers, dict), (
            "Unknown worker registration expected workers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_worker_added_to_pool() -> None:
    spec = _build_spec(
        "manager_worker_1_2_worker_added_to_pool",
        "1.2 Worker Pool Integration - Worker added to pool",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._worker_pool is not None, (
            "Worker added to pool expected worker pool"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_worker_health_state_in_pool() -> None:
    spec = _build_spec(
        "manager_worker_1_2_worker_health_state_in_pool",
        "1.2 Worker Pool Integration - Worker health state in pool",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        pool = manager._worker_pool
        assert callable(getattr(pool, "get_worker_health_state", None)), (
            "Worker health state in pool expected get_worker_health_state"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_worker_health_state_counts() -> None:
    spec = _build_spec(
        "manager_worker_1_2_worker_health_state_counts",
        "1.2 Worker Pool Integration - Worker health state counts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        pool = manager._worker_pool
        assert callable(getattr(pool, "get_worker_health_state_counts", None)), (
            "Worker health state counts expected get_worker_health_state_counts"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_worker_disconnects_gracefully() -> None:
    spec = _build_spec(
        "manager_worker_1_3_worker_disconnects_gracefully",
        "1.3 Worker Unregistration - Worker disconnects gracefully",
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
            "Worker disconnects expected worker deadlines"
        )
        assert isinstance(state._worker_unhealthy_since, dict), (
            "Worker disconnects expected worker unhealthy since"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_worker_dies_unexpectedly() -> None:
    spec = _build_spec(
        "manager_worker_1_3_worker_dies_unexpectedly",
        "1.3 Worker Unregistration - Worker dies unexpectedly",
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
            "Worker dies unexpectedly expected worker deadlines"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_cleanup_includes() -> None:
    spec = _build_spec(
        "manager_worker_1_3_cleanup_includes",
        "1.3 Worker Unregistration - Cleanup includes core state",
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
            "Cleanup expected worker circuits"
        )
        assert isinstance(state._dispatch_semaphores, dict), (
            "Cleanup expected dispatch semaphores"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_1_1_worker_registers_with_manager()
    await validate_1_1_registration_with_core_count()
    await validate_1_1_registration_with_health_state()
    await validate_1_1_reregistration_after_restart()
    await validate_1_1_registration_from_unknown_worker()
    await validate_1_2_worker_added_to_pool()
    await validate_1_2_worker_health_state_in_pool()
    await validate_1_2_worker_health_state_counts()
    await validate_1_3_worker_disconnects_gracefully()
    await validate_1_3_worker_dies_unexpectedly()
    await validate_1_3_cleanup_includes()


if __name__ == "__main__":
    asyncio.run(run())
