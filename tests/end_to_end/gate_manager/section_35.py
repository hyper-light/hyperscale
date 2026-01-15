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


async def validate_35_1_high_volume_reporter() -> None:
    spec = _build_spec(
        "gate_manager_35_1_high_volume_reporter",
        "35.1 Reporter Throughput - High-volume reporter",
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
            "High-volume reporter expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_1_reporter_batching() -> None:
    spec = _build_spec(
        "gate_manager_35_1_reporter_batching",
        "35.1 Reporter Throughput - Reporter batching",
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
            "Reporter batching expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_1_reporter_backlog() -> None:
    spec = _build_spec(
        "gate_manager_35_1_reporter_backlog",
        "35.1 Reporter Throughput - Reporter backlog",
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
            "Reporter backlog expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_1_reporter_memory() -> None:
    spec = _build_spec(
        "gate_manager_35_1_reporter_memory",
        "35.1 Reporter Throughput - Reporter memory",
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
            "Reporter memory expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_2_concurrent_reporters() -> None:
    spec = _build_spec(
        "gate_manager_35_2_concurrent_reporters",
        "35.2 Multiple Reporter Types - Concurrent reporters",
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
            "Concurrent reporters expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_2_reporter_priority() -> None:
    spec = _build_spec(
        "gate_manager_35_2_reporter_priority",
        "35.2 Multiple Reporter Types - Reporter priority",
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
            "Reporter priority expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_2_reporter_failure_isolation() -> None:
    spec = _build_spec(
        "gate_manager_35_2_reporter_failure_isolation",
        "35.2 Multiple Reporter Types - Reporter failure isolation",
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
            "Reporter failure isolation expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_2_reporter_resource_limits() -> None:
    spec = _build_spec(
        "gate_manager_35_2_reporter_resource_limits",
        "35.2 Multiple Reporter Types - Reporter resource limits",
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
            "Reporter resource limits expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_3_reporter_unreachable() -> None:
    spec = _build_spec(
        "gate_manager_35_3_reporter_unreachable",
        "35.3 Reporter During Failure - Reporter unreachable",
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
            "Reporter unreachable expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_3_reporter_reconnection() -> None:
    spec = _build_spec(
        "gate_manager_35_3_reporter_reconnection",
        "35.3 Reporter During Failure - Reporter reconnection",
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
            "Reporter reconnection expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_35_3_reporter_timeout() -> None:
    spec = _build_spec(
        "gate_manager_35_3_reporter_timeout",
        "35.3 Reporter During Failure - Reporter timeout",
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


async def validate_35_3_reporter_crash_recovery() -> None:
    spec = _build_spec(
        "gate_manager_35_3_reporter_crash_recovery",
        "35.3 Reporter During Failure - Reporter crash recovery",
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
            "Reporter crash recovery expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_35_1_high_volume_reporter()
    await validate_35_1_reporter_batching()
    await validate_35_1_reporter_backlog()
    await validate_35_1_reporter_memory()
    await validate_35_2_concurrent_reporters()
    await validate_35_2_reporter_priority()
    await validate_35_2_reporter_failure_isolation()
    await validate_35_2_reporter_resource_limits()
    await validate_35_3_reporter_unreachable()
    await validate_35_3_reporter_reconnection()
    await validate_35_3_reporter_timeout()
    await validate_35_3_reporter_crash_recovery()


if __name__ == "__main__":
    asyncio.run(run())
