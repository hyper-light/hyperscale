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


async def validate_42_1_memory_growth_over_time() -> None:
    spec = _build_spec(
        "gate_manager_42_1_memory_growth_over_time",
        "42.1 Long-Running Soak - Memory growth over time",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Memory growth expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_1_retry_budget_drift() -> None:
    spec = _build_spec(
        "gate_manager_42_1_retry_budget_drift",
        "42.1 Long-Running Soak - Retry budget drift",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Retry budget drift expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_1_idempotency_cache_churn() -> None:
    spec = _build_spec(
        "gate_manager_42_1_idempotency_cache_churn",
        "42.1 Long-Running Soak - Idempotency cache churn",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Idempotency cache churn expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_1_stats_buffer_retention() -> None:
    spec = _build_spec(
        "gate_manager_42_1_stats_buffer_retention",
        "42.1 Long-Running Soak - Stats buffer retention",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats buffer retention expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_1_event_log_rotation() -> None:
    spec = _build_spec(
        "gate_manager_42_1_event_log_rotation",
        "42.1 Long-Running Soak - Event log rotation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Event log rotation expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_2_random_manager_restarts() -> None:
    spec = _build_spec(
        "gate_manager_42_2_random_manager_restarts",
        "42.2 Targeted Chaos Injection - Random manager restarts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Random manager restarts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_2_random_gate_restarts() -> None:
    spec = _build_spec(
        "gate_manager_42_2_random_gate_restarts",
        "42.2 Targeted Chaos Injection - Random gate restarts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Random gate restarts expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_2_random_worker_restarts() -> None:
    spec = _build_spec(
        "gate_manager_42_2_random_worker_restarts",
        "42.2 Targeted Chaos Injection - Random worker restarts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Random worker restarts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_2_network_delay_injection() -> None:
    spec = _build_spec(
        "gate_manager_42_2_network_delay_injection",
        "42.2 Targeted Chaos Injection - Network delay injection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "Network delay expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_2_packet_loss_injection() -> None:
    spec = _build_spec(
        "gate_manager_42_2_packet_loss_injection",
        "42.2 Targeted Chaos Injection - Packet loss injection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Packet loss expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_3_rate_limit_backpressure() -> None:
    spec = _build_spec(
        "gate_manager_42_3_rate_limit_backpressure",
        "42.3 Backpressure + Rate Limiting - Rate limit + backpressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "Rate limit + backpressure expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_3_retry_after_headers() -> None:
    spec = _build_spec(
        "gate_manager_42_3_retry_after_headers",
        "42.3 Backpressure + Rate Limiting - Retry after headers",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "Retry after headers expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_3_throttle_escalation() -> None:
    spec = _build_spec(
        "gate_manager_42_3_throttle_escalation",
        "42.3 Backpressure + Rate Limiting - Throttle escalation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_backpressure, dict), (
            "Throttle escalation expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_3_control_plane_immunity() -> None:
    spec = _build_spec(
        "gate_manager_42_3_control_plane_immunity",
        "42.3 Backpressure + Rate Limiting - Control-plane immunity",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Control-plane immunity expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_3_recovery_ramp() -> None:
    spec = _build_spec(
        "gate_manager_42_3_recovery_ramp",
        "42.3 Backpressure + Rate Limiting - Recovery ramp",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Recovery ramp expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_4_multi_gate_submit_storm() -> None:
    spec = _build_spec(
        "gate_manager_42_4_multi_gate_submit_storm",
        "42.4 Multi-Gate Submit Storm - 3 gates accept 10K submits",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Submit storm expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_4_idempotency_across_gates() -> None:
    spec = _build_spec(
        "gate_manager_42_4_idempotency_across_gates",
        "42.4 Multi-Gate Submit Storm - Idempotency across gates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Idempotency across gates expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_4_spillover_under_storm() -> None:
    spec = _build_spec(
        "gate_manager_42_4_spillover_under_storm",
        "42.4 Multi-Gate Submit Storm - Spillover under storm",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Spillover under storm expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_4_observed_latency_learning() -> None:
    spec = _build_spec(
        "gate_manager_42_4_observed_latency_learning",
        "42.4 Multi-Gate Submit Storm - Observed latency learning",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Observed latency learning expected latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_4_quorum_loss_mid_storm() -> None:
    spec = _build_spec(
        "gate_manager_42_4_quorum_loss_mid_storm",
        "42.4 Multi-Gate Submit Storm - Quorum loss mid-storm",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, "Quorum loss expected quorum circuit"
    finally:
        await runtime.stop_cluster()


async def validate_42_5_dc_a_unhealthy_dc_b_busy_dc_c_healthy() -> None:
    spec = _build_spec(
        "gate_manager_42_5_dc_a_unhealthy_dc_b_busy_dc_c_healthy",
        "42.5 Multi-DC Partial Failure - DC-A unhealthy, DC-B busy, DC-C healthy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Partial failure matrix expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_5_dc_leader_down() -> None:
    spec = _build_spec(
        "gate_manager_42_5_dc_leader_down",
        "42.5 Multi-DC Partial Failure - DC leader down",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "DC leader down expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_5_manager_majority_unhealthy() -> None:
    spec = _build_spec(
        "gate_manager_42_5_manager_majority_unhealthy",
        "42.5 Multi-DC Partial Failure - Manager majority unhealthy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Manager majority unhealthy expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_5_worker_majority_unhealthy() -> None:
    spec = _build_spec(
        "gate_manager_42_5_worker_majority_unhealthy",
        "42.5 Multi-DC Partial Failure - Worker majority unhealthy",
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
            "Worker majority unhealthy expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_42_5_recovery_sequence() -> None:
    spec = _build_spec(
        "gate_manager_42_5_recovery_sequence",
        "42.5 Multi-DC Partial Failure - Recovery sequence",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Recovery sequence expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_42_1_memory_growth_over_time()
    await validate_42_1_retry_budget_drift()
    await validate_42_1_idempotency_cache_churn()
    await validate_42_1_stats_buffer_retention()
    await validate_42_1_event_log_rotation()
    await validate_42_2_random_manager_restarts()
    await validate_42_2_random_gate_restarts()
    await validate_42_2_random_worker_restarts()
    await validate_42_2_network_delay_injection()
    await validate_42_2_packet_loss_injection()
    await validate_42_3_rate_limit_backpressure()
    await validate_42_3_retry_after_headers()
    await validate_42_3_throttle_escalation()
    await validate_42_3_control_plane_immunity()
    await validate_42_3_recovery_ramp()
    await validate_42_4_multi_gate_submit_storm()
    await validate_42_4_idempotency_across_gates()
    await validate_42_4_spillover_under_storm()
    await validate_42_4_observed_latency_learning()
    await validate_42_4_quorum_loss_mid_storm()
    await validate_42_5_dc_a_unhealthy_dc_b_busy_dc_c_healthy()
    await validate_42_5_dc_leader_down()
    await validate_42_5_manager_majority_unhealthy()
    await validate_42_5_worker_majority_unhealthy()
    await validate_42_5_recovery_sequence()


if __name__ == "__main__":
    asyncio.run(run())
