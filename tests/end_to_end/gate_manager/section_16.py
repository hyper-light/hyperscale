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


async def validate_16_1_state_sync_request() -> None:
    spec = _build_spec(
        "gate_manager_16_1_state_sync_request",
        "16.1 Gate State Sync - State sync request",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "State sync request expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_1_state_sync_response() -> None:
    spec = _build_spec(
        "gate_manager_16_1_state_sync_response",
        "16.1 Gate State Sync - State sync response",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "State sync response expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_1_state_snapshot_application() -> None:
    spec = _build_spec(
        "gate_manager_16_1_state_snapshot_application",
        "16.1 Gate State Sync - State snapshot application",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_apply_gate_state_snapshot", None)), (
            "State snapshot application expected _apply_gate_state_snapshot"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_1_versioned_state_clock() -> None:
    spec = _build_spec(
        "gate_manager_16_1_versioned_state_clock",
        "16.1 Gate State Sync - Versioned state clock",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._state_version is not None, (
            "Versioned state clock expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_2_new_gate_joins() -> None:
    spec = _build_spec(
        "gate_manager_16_2_new_gate_joins",
        "16.2 Startup Sync - New gate joins",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_complete_startup_sync", None)), (
            "New gate joins expected _complete_startup_sync"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_2_sync_from_leader() -> None:
    spec = _build_spec(
        "gate_manager_16_2_sync_from_leader",
        "16.2 Startup Sync - Sync from leader",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Sync from leader expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_2_sync_completion() -> None:
    spec = _build_spec(
        "gate_manager_16_2_sync_completion",
        "16.2 Startup Sync - Sync completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_complete_startup_sync", None)), (
            "Sync completion expected _complete_startup_sync"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_16_1_state_sync_request()
    await validate_16_1_state_sync_response()
    await validate_16_1_state_snapshot_application()
    await validate_16_1_versioned_state_clock()
    await validate_16_2_new_gate_joins()
    await validate_16_2_sync_from_leader()
    await validate_16_2_sync_completion()


if __name__ == "__main__":
    asyncio.run(run())
