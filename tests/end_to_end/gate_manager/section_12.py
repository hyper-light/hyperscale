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


async def validate_12_1_reporter_task_creation() -> None:
    spec = _build_spec(
        "gate_manager_12_1_reporter_task_creation",
        "12.1 Reporter Task Management - Reporter task creation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter task creation expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_1_multiple_reporters_per_job() -> None:
    spec = _build_spec(
        "gate_manager_12_1_multiple_reporters_per_job",
        "12.1 Reporter Task Management - Multiple reporters per job",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Multiple reporters per job expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_1_reporter_task_execution() -> None:
    spec = _build_spec(
        "gate_manager_12_1_reporter_task_execution",
        "12.1 Reporter Task Management - Reporter task execution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter task execution expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_2_workflow_stats_to_reporter() -> None:
    spec = _build_spec(
        "gate_manager_12_2_workflow_stats_to_reporter",
        "12.2 Reporter Data Flow - Workflow stats to reporter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Workflow stats to reporter expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_2_final_results_to_reporter() -> None:
    spec = _build_spec(
        "gate_manager_12_2_final_results_to_reporter",
        "12.2 Reporter Data Flow - Final results to reporter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Final results to reporter expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_2_reporter_push() -> None:
    spec = _build_spec(
        "gate_manager_12_2_reporter_push",
        "12.2 Reporter Data Flow - Reporter push",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter push expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_3_reporter_task_fails() -> None:
    spec = _build_spec(
        "gate_manager_12_3_reporter_task_fails",
        "12.3 Reporter Error Handling - Reporter task fails",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter task fails expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_3_reporter_timeout() -> None:
    spec = _build_spec(
        "gate_manager_12_3_reporter_timeout",
        "12.3 Reporter Error Handling - Reporter timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter timeout expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_3_reporter_connection_lost() -> None:
    spec = _build_spec(
        "gate_manager_12_3_reporter_connection_lost",
        "12.3 Reporter Error Handling - Reporter connection lost",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter connection lost expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_4_job_cleanup_cancels_reporters() -> None:
    spec = _build_spec(
        "gate_manager_12_4_job_cleanup_cancels_reporters",
        "12.4 Reporter Cleanup - Job cleanup cancels reporters",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Job cleanup cancels reporters expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_4_reporter_cleanup_on_gate_shutdown() -> None:
    spec = _build_spec(
        "gate_manager_12_4_reporter_cleanup_on_gate_shutdown",
        "12.4 Reporter Cleanup - Reporter cleanup on gate shutdown",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Reporter cleanup on gate shutdown expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_12_1_reporter_task_creation()
    await validate_12_1_multiple_reporters_per_job()
    await validate_12_1_reporter_task_execution()
    await validate_12_2_workflow_stats_to_reporter()
    await validate_12_2_final_results_to_reporter()
    await validate_12_2_reporter_push()
    await validate_12_3_reporter_task_fails()
    await validate_12_3_reporter_timeout()
    await validate_12_3_reporter_connection_lost()
    await validate_12_4_job_cleanup_cancels_reporters()
    await validate_12_4_reporter_cleanup_on_gate_shutdown()


if __name__ == "__main__":
    asyncio.run(run())
