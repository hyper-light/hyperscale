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


async def validate_29_1_stats_buffer_growth() -> None:
    spec = _build_spec(
        "gate_manager_29_1_stats_buffer_growth",
        "29.1 Memory Pressure - Stats buffer growth",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats buffer growth expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_1_result_accumulation() -> None:
    spec = _build_spec(
        "gate_manager_29_1_result_accumulation",
        "29.1 Memory Pressure - Result accumulation",
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
            "Result accumulation expected results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_1_progress_callback_backlog() -> None:
    spec = _build_spec(
        "gate_manager_29_1_progress_callback_backlog",
        "29.1 Memory Pressure - Progress callback backlog",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._progress_callbacks, dict), (
            "Progress callback backlog expected progress callbacks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_1_hash_ring_memory() -> None:
    spec = _build_spec(
        "gate_manager_29_1_hash_ring_memory",
        "29.1 Memory Pressure - Hash ring memory",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Hash ring memory expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_29_2_tcp_connection_storm() -> None:
    spec = _build_spec(
        "gate_manager_29_2_tcp_connection_storm",
        "29.2 Connection Exhaustion - TCP connection storm",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "TCP connection storm expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_2_connection_per_manager() -> None:
    spec = _build_spec(
        "gate_manager_29_2_connection_per_manager",
        "29.2 Connection Exhaustion - Connection per manager",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "Connection per manager expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_2_udp_socket_buffer_overflow() -> None:
    spec = _build_spec(
        "gate_manager_29_2_udp_socket_buffer_overflow",
        "29.2 Connection Exhaustion - UDP socket buffer overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "UDP socket overflow expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_2_connection_leak_detection() -> None:
    spec = _build_spec(
        "gate_manager_29_2_connection_leak_detection",
        "29.2 Connection Exhaustion - Connection leak detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "Connection leak detection expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_3_stats_aggregation_cpu() -> None:
    spec = _build_spec(
        "gate_manager_29_3_stats_aggregation_cpu",
        "29.3 CPU Pressure - Stats aggregation CPU",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats aggregation CPU expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_3_serialization_cpu() -> None:
    spec = _build_spec(
        "gate_manager_29_3_serialization_cpu",
        "29.3 CPU Pressure - Serialization CPU",
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
            "Serialization CPU expected workflow results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_3_routing_calculation_cpu() -> None:
    spec = _build_spec(
        "gate_manager_29_3_routing_calculation_cpu",
        "29.3 CPU Pressure - Routing calculation CPU",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Routing calculation CPU expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_29_3_event_loop_saturation() -> None:
    spec = _build_spec(
        "gate_manager_29_3_event_loop_saturation",
        "29.3 CPU Pressure - Event loop saturation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Event loop saturation expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_29_1_stats_buffer_growth()
    await validate_29_1_result_accumulation()
    await validate_29_1_progress_callback_backlog()
    await validate_29_1_hash_ring_memory()
    await validate_29_2_tcp_connection_storm()
    await validate_29_2_connection_per_manager()
    await validate_29_2_udp_socket_buffer_overflow()
    await validate_29_2_connection_leak_detection()
    await validate_29_3_stats_aggregation_cpu()
    await validate_29_3_serialization_cpu()
    await validate_29_3_routing_calculation_cpu()
    await validate_29_3_event_loop_saturation()


if __name__ == "__main__":
    asyncio.run(run())
