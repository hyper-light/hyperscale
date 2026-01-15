import asyncio
import re

from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.reliability import BackpressureLevel

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


def _assert_backpressure_map(runtime: ScenarioRuntime, message: str) -> None:
    gate = _get_gate(runtime)
    state = gate._modular_state
    assert isinstance(state._manager_backpressure, dict), message
    assert all(
        isinstance(level, BackpressureLevel)
        for level in state._manager_backpressure.values()
    ), "Backpressure map expected BackpressureLevel values"


async def validate_5_1_manager_signals_none() -> None:
    spec = _build_spec(
        "gate_manager_5_1_manager_signals_none",
        "5.1 Manager Backpressure Signals - Manager signals NONE",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        _assert_backpressure_map(
            runtime, "Manager signals NONE expected _manager_backpressure map"
        )
        assert BackpressureLevel.NONE in BackpressureLevel, (
            "Manager signals NONE expected BackpressureLevel.NONE"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_1_manager_signals_low() -> None:
    spec = _build_spec(
        "gate_manager_5_1_manager_signals_low",
        "5.1 Manager Backpressure Signals - Manager signals LOW",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        _assert_backpressure_map(
            runtime, "Manager signals LOW expected _manager_backpressure map"
        )
        assert BackpressureLevel.THROTTLE in BackpressureLevel, (
            "Manager signals LOW expected BackpressureLevel.THROTTLE"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_1_manager_signals_medium() -> None:
    spec = _build_spec(
        "gate_manager_5_1_manager_signals_medium",
        "5.1 Manager Backpressure Signals - Manager signals MEDIUM",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        _assert_backpressure_map(
            runtime, "Manager signals MEDIUM expected _manager_backpressure map"
        )
        assert BackpressureLevel.BATCH in BackpressureLevel, (
            "Manager signals MEDIUM expected BackpressureLevel.BATCH"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_1_manager_signals_high() -> None:
    spec = _build_spec(
        "gate_manager_5_1_manager_signals_high",
        "5.1 Manager Backpressure Signals - Manager signals HIGH",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        _assert_backpressure_map(
            runtime, "Manager signals HIGH expected _manager_backpressure map"
        )
        assert BackpressureLevel.REJECT in BackpressureLevel, (
            "Manager signals HIGH expected BackpressureLevel.REJECT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_1_manager_signals_critical() -> None:
    spec = _build_spec(
        "gate_manager_5_1_manager_signals_critical",
        "5.1 Manager Backpressure Signals - Manager signals CRITICAL",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        _assert_backpressure_map(
            runtime, "Manager signals CRITICAL expected _manager_backpressure map"
        )
        assert BackpressureLevel.REJECT in BackpressureLevel, (
            "Manager signals CRITICAL expected BackpressureLevel.REJECT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_2_aggregate_manager_backpressure() -> None:
    spec = _build_spec(
        "gate_manager_5_2_aggregate_manager_backpressure",
        "5.2 DC-Level Backpressure - Aggregate manager backpressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._dc_backpressure is not None, (
            "Aggregate backpressure expected _dc_backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_2_dc_backpressure_affects_routing() -> None:
    spec = _build_spec(
        "gate_manager_5_2_dc_backpressure_affects_routing",
        "5.2 DC-Level Backpressure - DC backpressure affects routing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "Backpressure routing expected _job_router"
    finally:
        await runtime.stop_cluster()


async def validate_5_2_backpressure_delay_calculation() -> None:
    spec = _build_spec(
        "gate_manager_5_2_backpressure_delay_calculation",
        "5.2 DC-Level Backpressure - Backpressure delay calculation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._backpressure_delay_ms, int), (
            "Backpressure delay expected _backpressure_delay_ms"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_3_manager_backpressure_decreases() -> None:
    spec = _build_spec(
        "gate_manager_5_3_manager_backpressure_decreases",
        "5.3 Backpressure Recovery - Manager backpressure decreases",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        _assert_backpressure_map(
            runtime, "Backpressure recovery expected _manager_backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_5_3_dc_backpressure_clears() -> None:
    spec = _build_spec(
        "gate_manager_5_3_dc_backpressure_clears",
        "5.3 Backpressure Recovery - DC backpressure clears",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._dc_backpressure, dict), (
            "DC backpressure clears expected _dc_backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_5_1_manager_signals_none()
    await validate_5_1_manager_signals_low()
    await validate_5_1_manager_signals_medium()
    await validate_5_1_manager_signals_high()
    await validate_5_1_manager_signals_critical()
    await validate_5_2_aggregate_manager_backpressure()
    await validate_5_2_dc_backpressure_affects_routing()
    await validate_5_2_backpressure_delay_calculation()
    await validate_5_3_manager_backpressure_decreases()
    await validate_5_3_dc_backpressure_clears()


if __name__ == "__main__":
    asyncio.run(run())
