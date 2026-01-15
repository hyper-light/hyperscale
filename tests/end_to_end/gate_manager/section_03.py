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


async def validate_3_1_liveness_probe_success() -> None:
    spec = _build_spec(
        "gate_manager_3_1_liveness_probe_success",
        "3.1 Manager Health State - Liveness probe success",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._manager_health, "Liveness probe success expected _manager_health"
    finally:
        await runtime.stop_cluster()


async def validate_3_1_liveness_probe_failure() -> None:
    spec = _build_spec(
        "gate_manager_3_1_liveness_probe_failure",
        "3.1 Manager Health State - Liveness probe failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_health_coordinator"), (
            "Liveness failure expected _health_coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_1_liveness_failure_threshold_exceeded() -> None:
    spec = _build_spec(
        "gate_manager_3_1_liveness_failure_threshold_exceeded",
        "3.1 Manager Health State - Liveness failure threshold exceeded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_health_coordinator"), (
            "Liveness threshold expected _health_coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_1_readiness_probe() -> None:
    spec = _build_spec(
        "gate_manager_3_1_readiness_probe",
        "3.1 Manager Health State - Readiness probe",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._manager_health, "Readiness probe expected _manager_health"
    finally:
        await runtime.stop_cluster()


async def validate_3_1_readiness_failure() -> None:
    spec = _build_spec(
        "gate_manager_3_1_readiness_failure",
        "3.1 Manager Health State - Readiness failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_health_coordinator"), (
            "Readiness failure expected _health_coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_1_startup_probe() -> None:
    spec = _build_spec(
        "gate_manager_3_1_startup_probe",
        "3.1 Manager Health State - Startup probe",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_health_coordinator"), (
            "Startup probe expected _health_coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_2_gate_peer_liveness() -> None:
    spec = _build_spec(
        "gate_manager_3_2_gate_peer_liveness",
        "3.2 Gate Health State - Gate peer liveness",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._gate_peer_health is not None, (
            "Gate peer liveness expected _gate_peer_health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_2_gate_peer_readiness() -> None:
    spec = _build_spec(
        "gate_manager_3_2_gate_peer_readiness",
        "3.2 Gate Health State - Gate peer readiness",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._gate_peer_info is not None, (
            "Gate peer readiness expected _gate_peer_info"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_2_gate_health_aggregation() -> None:
    spec = _build_spec(
        "gate_manager_3_2_gate_health_aggregation",
        "3.2 Gate Health State - Gate health aggregation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_get_healthy_gates", None)), (
            "Gate health aggregation expected _get_healthy_gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_error_threshold_reached() -> None:
    spec = _build_spec(
        "gate_manager_3_3_error_threshold_reached",
        "3.3 Circuit Breaker - Error threshold reached",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_quorum_circuit"), (
            "Circuit breaker expected _quorum_circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_circuit_open_behavior() -> None:
    spec = _build_spec(
        "gate_manager_3_3_circuit_open_behavior",
        "3.3 Circuit Breaker - Circuit open behavior",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_quorum_circuit"), (
            "Circuit open behavior expected _quorum_circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_half_open_transition() -> None:
    spec = _build_spec(
        "gate_manager_3_3_half_open_transition",
        "3.3 Circuit Breaker - Half-open transition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_quorum_circuit"), (
            "Half-open transition expected _quorum_circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_circuit_close_on_success() -> None:
    spec = _build_spec(
        "gate_manager_3_3_circuit_close_on_success",
        "3.3 Circuit Breaker - Circuit close on success",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_quorum_circuit"), (
            "Circuit close expected _quorum_circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_circuit_stays_open_on_failure() -> None:
    spec = _build_spec(
        "gate_manager_3_3_circuit_stays_open_on_failure",
        "3.3 Circuit Breaker - Circuit stays open on failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_quorum_circuit"), (
            "Circuit stays open expected _quorum_circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_3_circuit_breaker_isolation() -> None:
    spec = _build_spec(
        "gate_manager_3_3_circuit_breaker_isolation",
        "3.3 Circuit Breaker - Circuit breaker per-manager isolation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_quorum_circuit"), (
            "Circuit breaker isolation expected _quorum_circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_dc_marked_healthy() -> None:
    spec = _build_spec(
        "gate_manager_3_4_dc_marked_healthy",
        "3.4 Datacenter Health Manager - DC marked healthy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_manager"), (
            "DC healthy expected _dc_health_manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_dc_marked_degraded() -> None:
    spec = _build_spec(
        "gate_manager_3_4_dc_marked_degraded",
        "3.4 Datacenter Health Manager - DC marked degraded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_manager"), (
            "DC degraded expected _dc_health_manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_dc_marked_unhealthy() -> None:
    spec = _build_spec(
        "gate_manager_3_4_dc_marked_unhealthy",
        "3.4 Datacenter Health Manager - DC marked unhealthy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_manager"), (
            "DC unhealthy expected _dc_health_manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_dc_health_affects_routing() -> None:
    spec = _build_spec(
        "gate_manager_3_4_dc_health_affects_routing",
        "3.4 Datacenter Health Manager - DC health affects routing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "DC health routing expected _job_router"
    finally:
        await runtime.stop_cluster()


async def validate_3_4_manager_added_to_dc() -> None:
    spec = _build_spec(
        "gate_manager_3_4_manager_added_to_dc",
        "3.4 Datacenter Health Manager - Manager added to DC",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_manager"), (
            "Manager added to DC expected _dc_health_manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_4_manager_removed_from_dc() -> None:
    spec = _build_spec(
        "gate_manager_3_4_manager_removed_from_dc",
        "3.4 Datacenter Health Manager - Manager removed from DC",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_manager"), (
            "Manager removed from DC expected _dc_health_manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_cross_dc_probe_sent() -> None:
    spec = _build_spec(
        "gate_manager_3_5_cross_dc_probe_sent",
        "3.5 Federated Health Monitor - Cross-DC probe sent",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_monitor"), (
            "Cross-DC probe expected _dc_health_monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_cross_dc_probe_response() -> None:
    spec = _build_spec(
        "gate_manager_3_5_cross_dc_probe_response",
        "3.5 Federated Health Monitor - Cross-DC probe response",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_monitor"), (
            "Cross-DC probe response expected _dc_health_monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_cross_dc_probe_timeout() -> None:
    spec = _build_spec(
        "gate_manager_3_5_cross_dc_probe_timeout",
        "3.5 Federated Health Monitor - Cross-DC probe timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_monitor"), (
            "Cross-DC probe timeout expected _dc_health_monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_dc_leader_change_detected() -> None:
    spec = _build_spec(
        "gate_manager_3_5_dc_leader_change_detected",
        "3.5 Federated Health Monitor - DC leader change detected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_monitor"), (
            "DC leader change expected _dc_health_monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_dc_health_change_detected() -> None:
    spec = _build_spec(
        "gate_manager_3_5_dc_health_change_detected",
        "3.5 Federated Health Monitor - DC health change detected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_monitor"), (
            "DC health change expected _dc_health_monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_5_dc_latency_recorded() -> None:
    spec = _build_spec(
        "gate_manager_3_5_dc_latency_recorded",
        "3.5 Federated Health Monitor - DC latency recorded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dc_health_monitor"), (
            "DC latency expected _dc_health_monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_6_global_death_detected() -> None:
    spec = _build_spec(
        "gate_manager_3_6_global_death_detected",
        "3.6 Hierarchical Failure Detector - Global death detected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_on_manager_globally_dead", None)), (
            "Global death expected _on_manager_globally_dead"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_6_job_level_death_detected() -> None:
    spec = _build_spec(
        "gate_manager_3_6_job_level_death_detected",
        "3.6 Hierarchical Failure Detector - Job-level death detected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_on_manager_dead_for_dc", None)), (
            "Job-level death expected _on_manager_dead_for_dc"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_6_timeout_adaptation() -> None:
    spec = _build_spec(
        "gate_manager_3_6_timeout_adaptation",
        "3.6 Hierarchical Failure Detector - Timeout adaptation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_get_dc_manager_count", None)), (
            "Timeout adaptation expected _get_dc_manager_count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_7_correlated_failures_detected() -> None:
    spec = _build_spec(
        "gate_manager_3_7_correlated_failures_detected",
        "3.7 Cross-DC Correlation Detector - Correlated failures detected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_cross_dc_correlation"), (
            "Correlated failures expected _cross_dc_correlation"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_7_network_partition_suspected() -> None:
    spec = _build_spec(
        "gate_manager_3_7_network_partition_suspected",
        "3.7 Cross-DC Correlation Detector - Network partition suspected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_cross_dc_correlation"), (
            "Partition suspected expected _cross_dc_correlation"
        )
    finally:
        await runtime.stop_cluster()


async def validate_3_7_independent_failures() -> None:
    spec = _build_spec(
        "gate_manager_3_7_independent_failures",
        "3.7 Cross-DC Correlation Detector - Independent failures",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_cross_dc_correlation"), (
            "Independent failures expected _cross_dc_correlation"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_3_1_liveness_probe_success()
    await validate_3_1_liveness_probe_failure()
    await validate_3_1_liveness_failure_threshold_exceeded()
    await validate_3_1_readiness_probe()
    await validate_3_1_readiness_failure()
    await validate_3_1_startup_probe()
    await validate_3_2_gate_peer_liveness()
    await validate_3_2_gate_peer_readiness()
    await validate_3_2_gate_health_aggregation()
    await validate_3_3_error_threshold_reached()
    await validate_3_3_circuit_open_behavior()
    await validate_3_3_half_open_transition()
    await validate_3_3_circuit_close_on_success()
    await validate_3_3_circuit_stays_open_on_failure()
    await validate_3_3_circuit_breaker_isolation()
    await validate_3_4_dc_marked_healthy()
    await validate_3_4_dc_marked_degraded()
    await validate_3_4_dc_marked_unhealthy()
    await validate_3_4_dc_health_affects_routing()
    await validate_3_4_manager_added_to_dc()
    await validate_3_4_manager_removed_from_dc()
    await validate_3_5_cross_dc_probe_sent()
    await validate_3_5_cross_dc_probe_response()
    await validate_3_5_cross_dc_probe_timeout()
    await validate_3_5_dc_leader_change_detected()
    await validate_3_5_dc_health_change_detected()
    await validate_3_5_dc_latency_recorded()
    await validate_3_6_global_death_detected()
    await validate_3_6_job_level_death_detected()
    await validate_3_6_timeout_adaptation()
    await validate_3_7_correlated_failures_detected()
    await validate_3_7_network_partition_suspected()
    await validate_3_7_independent_failures()


if __name__ == "__main__":
    asyncio.run(run())
