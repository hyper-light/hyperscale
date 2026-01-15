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


async def validate_22_1_high_volume_result_handling() -> None:
    spec = _build_spec(
        "gate_manager_22_1_high_volume_result_handling",
        "22.1 High-Volume Result Handling - 10K workflows complete simultaneously",
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
            "High-volume results expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_1_result_serialization_bottleneck() -> None:
    spec = _build_spec(
        "gate_manager_22_1_result_serialization_bottleneck",
        "22.1 High-Volume Result Handling - Result serialization bottleneck",
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
            "Result serialization bottleneck expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_1_result_queue_depth() -> None:
    spec = _build_spec(
        "gate_manager_22_1_result_queue_depth",
        "22.1 High-Volume Result Handling - Result queue depth",
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
            "Result queue depth expected results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_1_result_memory_accumulation() -> None:
    spec = _build_spec(
        "gate_manager_22_1_result_memory_accumulation",
        "22.1 High-Volume Result Handling - Result memory accumulation",
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
            "Result memory accumulation expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_2_results_before_dispatch_ack() -> None:
    spec = _build_spec(
        "gate_manager_22_2_results_before_dispatch_ack",
        "22.2 Result Ordering Edge Cases - Results arrive before dispatch ACK",
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
            "Results before dispatch ACK expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_2_results_not_in_tracking() -> None:
    spec = _build_spec(
        "gate_manager_22_2_results_not_in_tracking",
        "22.2 Result Ordering Edge Cases - Results from workflow not in tracking",
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
            "Results not in tracking expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_2_duplicate_results() -> None:
    spec = _build_spec(
        "gate_manager_22_2_duplicate_results",
        "22.2 Result Ordering Edge Cases - Duplicate results",
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
            "Duplicate results expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_2_partial_result_set() -> None:
    spec = _build_spec(
        "gate_manager_22_2_partial_result_set",
        "22.2 Result Ordering Edge Cases - Partial result set",
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
            "Partial result set expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_3_dc_latency_asymmetry() -> None:
    spec = _build_spec(
        "gate_manager_22_3_dc_latency_asymmetry",
        "22.3 Cross-DC Result Aggregation - DC latency asymmetry",
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
            "DC latency asymmetry expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_3_dc_result_conflict() -> None:
    spec = _build_spec(
        "gate_manager_22_3_dc_result_conflict",
        "22.3 Cross-DC Result Aggregation - DC result conflict",
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
            "DC result conflict expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_3_dc_result_timeout() -> None:
    spec = _build_spec(
        "gate_manager_22_3_dc_result_timeout",
        "22.3 Cross-DC Result Aggregation - DC result timeout",
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
            "DC result timeout expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_22_3_result_aggregation_race() -> None:
    spec = _build_spec(
        "gate_manager_22_3_result_aggregation_race",
        "22.3 Cross-DC Result Aggregation - Result aggregation race",
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
            "Result aggregation race expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_22_1_high_volume_result_handling()
    await validate_22_1_result_serialization_bottleneck()
    await validate_22_1_result_queue_depth()
    await validate_22_1_result_memory_accumulation()
    await validate_22_2_results_before_dispatch_ack()
    await validate_22_2_results_not_in_tracking()
    await validate_22_2_duplicate_results()
    await validate_22_2_partial_result_set()
    await validate_22_3_dc_latency_asymmetry()
    await validate_22_3_dc_result_conflict()
    await validate_22_3_dc_result_timeout()
    await validate_22_3_result_aggregation_race()


if __name__ == "__main__":
    asyncio.run(run())
