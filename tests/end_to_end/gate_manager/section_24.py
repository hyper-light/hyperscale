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


async def validate_24_1_us_to_europe_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_24_1_us_to_europe_dispatch",
        "24.1 Latency Asymmetry - US-to-Europe dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "US-to-Europe dispatch expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_1_us_to_asia_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_24_1_us_to_asia_dispatch",
        "24.1 Latency Asymmetry - US-to-Asia dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "US-to-Asia dispatch expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_1_latency_spike() -> None:
    spec = _build_spec(
        "gate_manager_24_1_latency_spike",
        "24.1 Latency Asymmetry - Latency spike",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Latency spike expected observed latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_1_latency_variance() -> None:
    spec = _build_spec(
        "gate_manager_24_1_latency_variance",
        "24.1 Latency Asymmetry - Latency variance",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, (
            "Latency variance expected blended scorer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_2_dc_clocks_differ() -> None:
    spec = _build_spec(
        "gate_manager_24_2_dc_clocks_differ",
        "24.2 Clock Skew - DC clocks differ by 100ms",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._state_version is not None, "Clock skew expected state version"
    finally:
        await runtime.stop_cluster()


async def validate_24_2_clock_jump() -> None:
    spec = _build_spec(
        "gate_manager_24_2_clock_jump",
        "24.2 Clock Skew - Clock jump",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._state_version is not None, "Clock jump expected state version"
    finally:
        await runtime.stop_cluster()


async def validate_24_2_clock_drift() -> None:
    spec = _build_spec(
        "gate_manager_24_2_clock_drift",
        "24.2 Clock Skew - Clock drift",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._state_version is not None, "Clock drift expected state version"
    finally:
        await runtime.stop_cluster()


async def validate_24_2_timestamp_comparison() -> None:
    spec = _build_spec(
        "gate_manager_24_2_timestamp_comparison",
        "24.2 Clock Skew - Timestamp comparison",
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
            "Timestamp comparison expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_3_trans_atlantic_partition() -> None:
    spec = _build_spec(
        "gate_manager_24_3_trans_atlantic_partition",
        "24.3 Continent-Scale Partitions - Trans-Atlantic partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._gate_peer_unhealthy_since, dict), (
            "Trans-Atlantic partition expected gate peer unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_3_trans_pacific_partition() -> None:
    spec = _build_spec(
        "gate_manager_24_3_trans_pacific_partition",
        "24.3 Continent-Scale Partitions - Trans-Pacific partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._gate_peer_unhealthy_since, dict), (
            "Trans-Pacific partition expected gate peer unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_3_partial_partition() -> None:
    spec = _build_spec(
        "gate_manager_24_3_partial_partition",
        "24.3 Continent-Scale Partitions - Partial partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._dead_gate_peers, set), (
            "Partial partition expected dead gate peers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_3_partition_heals() -> None:
    spec = _build_spec(
        "gate_manager_24_3_partition_heals",
        "24.3 Continent-Scale Partitions - Partition heals",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Partition heals expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_4_us_west_region_fails() -> None:
    spec = _build_spec(
        "gate_manager_24_4_us_west_region_fails",
        "24.4 Regional Failure Cascades - US-West region fails",
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
            "Region fails expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_4_gradual_regional_degradation() -> None:
    spec = _build_spec(
        "gate_manager_24_4_gradual_regional_degradation",
        "24.4 Regional Failure Cascades - Gradual regional degradation",
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
            "Regional degradation expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_24_4_regional_recovery() -> None:
    spec = _build_spec(
        "gate_manager_24_4_regional_recovery",
        "24.4 Regional Failure Cascades - Regional recovery",
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
            "Regional recovery expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_24_1_us_to_europe_dispatch()
    await validate_24_1_us_to_asia_dispatch()
    await validate_24_1_latency_spike()
    await validate_24_1_latency_variance()
    await validate_24_2_dc_clocks_differ()
    await validate_24_2_clock_jump()
    await validate_24_2_clock_drift()
    await validate_24_2_timestamp_comparison()
    await validate_24_3_trans_atlantic_partition()
    await validate_24_3_trans_pacific_partition()
    await validate_24_3_partial_partition()
    await validate_24_3_partition_heals()
    await validate_24_4_us_west_region_fails()
    await validate_24_4_gradual_regional_degradation()
    await validate_24_4_regional_recovery()


if __name__ == "__main__":
    asyncio.run(run())
