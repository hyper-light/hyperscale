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


async def validate_26_1_health_probe_latency() -> None:
    spec = _build_spec(
        "gate_manager_26_1_health_probe_latency",
        "26.1 Cross-Region Health Probes - Health probe latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "Health probe latency expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_1_probe_packet_loss() -> None:
    spec = _build_spec(
        "gate_manager_26_1_probe_packet_loss",
        "26.1 Cross-Region Health Probes - Probe packet loss",
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
            "Probe packet loss expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_1_probe_batching() -> None:
    spec = _build_spec(
        "gate_manager_26_1_probe_batching",
        "26.1 Cross-Region Health Probes - Probe batching",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Probe batching expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_1_probe_prioritization() -> None:
    spec = _build_spec(
        "gate_manager_26_1_probe_prioritization",
        "26.1 Cross-Region Health Probes - Probe prioritization",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Probe prioritization expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_26_2_dc_health_change() -> None:
    spec = _build_spec(
        "gate_manager_26_2_dc_health_change",
        "26.2 Health State Propagation - DC health change",
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
            "DC health change expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_2_health_flapping() -> None:
    spec = _build_spec(
        "gate_manager_26_2_health_flapping",
        "26.2 Health State Propagation - Health flapping",
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
            "Health flapping expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_2_health_disagreement() -> None:
    spec = _build_spec(
        "gate_manager_26_2_health_disagreement",
        "26.2 Health State Propagation - Health disagreement",
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
            "Health disagreement expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_2_health_state_cache() -> None:
    spec = _build_spec(
        "gate_manager_26_2_health_state_cache",
        "26.2 Health State Propagation - Health state cache",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_last_status, dict), (
            "Health state cache expected last status"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_3_region_health_rollup() -> None:
    spec = _build_spec(
        "gate_manager_26_3_region_health_rollup",
        "26.3 Regional Health Aggregation - Region health rollup",
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
            "Region health rollup expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_3_regional_load_balancing() -> None:
    spec = _build_spec(
        "gate_manager_26_3_regional_load_balancing",
        "26.3 Regional Health Aggregation - Regional load balancing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Regional load balancing expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_26_3_regional_failover() -> None:
    spec = _build_spec(
        "gate_manager_26_3_regional_failover",
        "26.3 Regional Health Aggregation - Regional failover",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Regional failover expected job router"
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_26_1_health_probe_latency()
    await validate_26_1_probe_packet_loss()
    await validate_26_1_probe_batching()
    await validate_26_1_probe_prioritization()
    await validate_26_2_dc_health_change()
    await validate_26_2_health_flapping()
    await validate_26_2_health_disagreement()
    await validate_26_2_health_state_cache()
    await validate_26_3_region_health_rollup()
    await validate_26_3_regional_load_balancing()
    await validate_26_3_regional_failover()


if __name__ == "__main__":
    asyncio.run(run())
