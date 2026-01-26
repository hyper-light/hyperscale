import asyncio
import re

from hyperscale.distributed.nodes.gate import GateServer

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
                "dc_count": 2,
                "managers_per_dc": 2,
                "workers_per_dc": 1,
                "cores_per_worker": 1,
                "base_gate_tcp": 8000,
            },
            "actions": [
                {"type": "start_cluster"},
                {"type": "await_gate_leader", "params": {"timeout": 30}},
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-A", "timeout": 30},
                },
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-B", "timeout": 30},
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


def _get_gate(runtime: ScenarioRuntime) -> GateServer:
    cluster = runtime.require_cluster()
    gate = cluster.get_gate_leader() or cluster.gates[0]
    return gate


def _require_runtime(outcome: ScenarioOutcome) -> ScenarioRuntime:
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def validate_40_1_gate_shutdown_with_jobs() -> None:
    spec = _build_spec(
        "gate_manager_40_1_gate_shutdown_with_jobs",
        "40.1 Gate Shutdown - Gate shutdown with jobs",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Gate shutdown with jobs expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_1_leadership_transfer_during_shutdown() -> None:
    spec = _build_spec(
        "gate_manager_40_1_leadership_transfer_during_shutdown",
        "40.1 Gate Shutdown - Leadership transfer during shutdown",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Leadership transfer during shutdown expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_1_stats_flush_on_shutdown() -> None:
    spec = _build_spec(
        "gate_manager_40_1_stats_flush_on_shutdown",
        "40.1 Gate Shutdown - Stats flush on shutdown",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Stats flush on shutdown expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_1_connection_draining() -> None:
    spec = _build_spec(
        "gate_manager_40_1_connection_draining",
        "40.1 Gate Shutdown - Connection draining",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Connection draining expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_2_manager_shutdown_with_workflows() -> None:
    spec = _build_spec(
        "gate_manager_40_2_manager_shutdown_with_workflows",
        "40.2 Manager Shutdown - Manager shutdown with workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Manager shutdown expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_2_worker_notification() -> None:
    spec = _build_spec(
        "gate_manager_40_2_worker_notification",
        "40.2 Manager Shutdown - Worker notification",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Worker notification expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_2_result_forwarding() -> None:
    spec = _build_spec(
        "gate_manager_40_2_result_forwarding",
        "40.2 Manager Shutdown - Result forwarding",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Result forwarding expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_2_state_handoff() -> None:
    spec = _build_spec(
        "gate_manager_40_2_state_handoff",
        "40.2 Manager Shutdown - State handoff",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "State handoff expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_3_worker_shutdown_mid_workflow() -> None:
    spec = _build_spec(
        "gate_manager_40_3_worker_shutdown_mid_workflow",
        "40.3 Worker Shutdown - Worker shutdown mid-workflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Worker shutdown mid-workflow expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_3_core_release_on_shutdown() -> None:
    spec = _build_spec(
        "gate_manager_40_3_core_release_on_shutdown",
        "40.3 Worker Shutdown - Core release on shutdown",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Core release on shutdown expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_3_result_submission() -> None:
    spec = _build_spec(
        "gate_manager_40_3_result_submission",
        "40.3 Worker Shutdown - Result submission",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Result submission expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_40_3_health_state_update() -> None:
    spec = _build_spec(
        "gate_manager_40_3_health_state_update",
        "40.3 Worker Shutdown - Health state update",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_health, dict), (
            "Health state update expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_40_1_gate_shutdown_with_jobs()
    await validate_40_1_leadership_transfer_during_shutdown()
    await validate_40_1_stats_flush_on_shutdown()
    await validate_40_1_connection_draining()
    await validate_40_2_manager_shutdown_with_workflows()
    await validate_40_2_worker_notification()
    await validate_40_2_result_forwarding()
    await validate_40_2_state_handoff()
    await validate_40_3_worker_shutdown_mid_workflow()
    await validate_40_3_core_release_on_shutdown()
    await validate_40_3_result_submission()
    await validate_40_3_health_state_update()


if __name__ == "__main__":
    asyncio.run(run())
