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


async def validate_5_1_healthy_state() -> None:
    spec = _build_spec(
        "manager_worker_5_1_healthy_state",
        "5.1 Worker Health States - HEALTHY",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "HEALTHY expected health monitor"
    finally:
        await runtime.stop_cluster()


async def validate_5_1_busy_state() -> None:
    spec = _build_spec(
        "manager_worker_5_1_busy_state",
        "5.1 Worker Health States - BUSY",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "BUSY expected health monitor"
    finally:
        await runtime.stop_cluster()


async def validate_5_1_stressed_state() -> None:
    spec = _build_spec(
        "manager_worker_5_1_stressed_state",
        "5.1 Worker Health States - STRESSED/DEGRADED",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "STRESSED expected health monitor"
    finally:
        await runtime.stop_cluster()


async def validate_5_1_overloaded_state() -> None:
    spec = _build_spec(
        "manager_worker_5_1_overloaded_state",
        "5.1 Worker Health States - OVERLOADED",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "OVERLOADED expected health monitor"
    finally:
        await runtime.stop_cluster()


async def validate_5_2_healthy_to_busy() -> None:
    spec = _build_spec(
        "manager_worker_5_2_healthy_to_busy",
        "5.2 Health State Transitions - HEALTHY → BUSY",
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
            "HEALTHY → BUSY expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_2_busy_to_stressed() -> None:
    spec = _build_spec(
        "manager_worker_5_2_busy_to_stressed",
        "5.2 Health State Transitions - BUSY → STRESSED",
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
            "BUSY → STRESSED expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_2_stressed_to_overloaded() -> None:
    spec = _build_spec(
        "manager_worker_5_2_stressed_to_overloaded",
        "5.2 Health State Transitions - STRESSED → OVERLOADED",
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
            "STRESSED → OVERLOADED expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_2_recovery_path() -> None:
    spec = _build_spec(
        "manager_worker_5_2_recovery_path",
        "5.2 Health State Transitions - Recovery path",
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
            "Recovery path expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_3_error_threshold() -> None:
    spec = _build_spec(
        "manager_worker_5_3_error_threshold",
        "5.3 Circuit Breaker Per-Worker - Error threshold",
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
            "Error threshold expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_3_circuit_open() -> None:
    spec = _build_spec(
        "manager_worker_5_3_circuit_open",
        "5.3 Circuit Breaker Per-Worker - Circuit open",
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
            "Circuit open expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_3_half_open() -> None:
    spec = _build_spec(
        "manager_worker_5_3_half_open",
        "5.3 Circuit Breaker Per-Worker - Half-open",
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
            "Half-open expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_3_circuit_close() -> None:
    spec = _build_spec(
        "manager_worker_5_3_circuit_close",
        "5.3 Circuit Breaker Per-Worker - Circuit close",
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
            "Circuit close expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_4_mark_unhealthy() -> None:
    spec = _build_spec(
        "manager_worker_5_4_mark_unhealthy",
        "5.4 Unhealthy Worker Tracking - Mark unhealthy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_unhealthy_since, dict), (
            "Mark unhealthy expected worker unhealthy since"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_4_dead_worker_reaping() -> None:
    spec = _build_spec(
        "manager_worker_5_4_dead_worker_reaping",
        "5.4 Unhealthy Worker Tracking - Dead worker reaping",
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
            "Dead worker reaping expected worker deadlines"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_4_recovery_detection() -> None:
    spec = _build_spec(
        "manager_worker_5_4_recovery_detection",
        "5.4 Unhealthy Worker Tracking - Recovery detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_unhealthy_since, dict), (
            "Recovery detection expected worker unhealthy since"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_5_1_healthy_state()
    await validate_5_1_busy_state()
    await validate_5_1_stressed_state()
    await validate_5_1_overloaded_state()
    await validate_5_2_healthy_to_busy()
    await validate_5_2_busy_to_stressed()
    await validate_5_2_stressed_to_overloaded()
    await validate_5_2_recovery_path()
    await validate_5_3_error_threshold()
    await validate_5_3_circuit_open()
    await validate_5_3_half_open()
    await validate_5_3_circuit_close()
    await validate_5_4_mark_unhealthy()
    await validate_5_4_dead_worker_reaping()
    await validate_5_4_recovery_detection()


if __name__ == "__main__":
    asyncio.run(run())
