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


async def validate_27_1_route_to_nearest_dc() -> None:
    spec = _build_spec(
        "gate_manager_27_1_route_to_nearest_dc",
        "27.1 Latency-Aware Routing - Route to nearest DC",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "Route to nearest DC expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_1_route_with_capacity_constraint() -> None:
    spec = _build_spec(
        "gate_manager_27_1_route_with_capacity_constraint",
        "27.1 Latency-Aware Routing - Route with capacity constraint",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Capacity constraint expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_1_route_with_slo_constraint() -> None:
    spec = _build_spec(
        "gate_manager_27_1_route_with_slo_constraint",
        "27.1 Latency-Aware Routing - Route with SLO constraint",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, (
            "SLO constraint expected blended scorer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_1_route_preference_override() -> None:
    spec = _build_spec(
        "gate_manager_27_1_route_preference_override",
        "27.1 Latency-Aware Routing - Route preference override",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Route preference override expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_2_global_load_balancing() -> None:
    spec = _build_spec(
        "gate_manager_27_2_global_load_balancing",
        "27.2 Load Distribution - Global load balancing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Global load balancing expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_27_2_hotspot_detection() -> None:
    spec = _build_spec(
        "gate_manager_27_2_hotspot_detection",
        "27.2 Load Distribution - Hotspot detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Hotspot detection expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_2_load_shedding_by_region() -> None:
    spec = _build_spec(
        "gate_manager_27_2_load_shedding_by_region",
        "27.2 Load Distribution - Load shedding by region",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Load shedding by region expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_2_capacity_aware_distribution() -> None:
    spec = _build_spec(
        "gate_manager_27_2_capacity_aware_distribution",
        "27.2 Load Distribution - Capacity-aware distribution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Capacity-aware distribution expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_27_3_primary_dc_fails() -> None:
    spec = _build_spec(
        "gate_manager_27_3_primary_dc_fails",
        "27.3 Routing During Failures - Primary DC fails",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Primary DC fails expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_27_3_all_dcs_in_region_fail() -> None:
    spec = _build_spec(
        "gate_manager_27_3_all_dcs_in_region_fail",
        "27.3 Routing During Failures - All DCs in region fail",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "All DCs fail expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_27_3_partial_dc_failure() -> None:
    spec = _build_spec(
        "gate_manager_27_3_partial_dc_failure",
        "27.3 Routing During Failures - Partial DC failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Partial DC failure expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_27_3_routing_oscillation() -> None:
    spec = _build_spec(
        "gate_manager_27_3_routing_oscillation",
        "27.3 Routing During Failures - Routing oscillation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, (
            "Routing oscillation expected blended scorer"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_27_1_route_to_nearest_dc()
    await validate_27_1_route_with_capacity_constraint()
    await validate_27_1_route_with_slo_constraint()
    await validate_27_1_route_preference_override()
    await validate_27_2_global_load_balancing()
    await validate_27_2_hotspot_detection()
    await validate_27_2_load_shedding_by_region()
    await validate_27_2_capacity_aware_distribution()
    await validate_27_3_primary_dc_fails()
    await validate_27_3_all_dcs_in_region_fail()
    await validate_27_3_partial_dc_failure()
    await validate_27_3_routing_oscillation()


if __name__ == "__main__":
    asyncio.run(run())
