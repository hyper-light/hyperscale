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


async def validate_41_1_all_gates_start_concurrently() -> None:
    spec = _build_spec(
        "gate_manager_41_1_all_gates_start_concurrently",
        "41.1 Topology Bootstrap - All 3 gates start concurrently",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Startup confirmation expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_1_managers_start_before_gates() -> None:
    spec = _build_spec(
        "gate_manager_41_1_managers_start_before_gates",
        "41.1 Topology Bootstrap - Managers start before gates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Manager startup expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_1_unconfirmed_peer_never_responds() -> None:
    spec = _build_spec(
        "gate_manager_41_1_unconfirmed_peer_never_responds",
        "41.1 Topology Bootstrap - Unconfirmed peer never responds",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Unconfirmed peer expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_1_gossip_unconfirmed_peer() -> None:
    spec = _build_spec(
        "gate_manager_41_1_gossip_unconfirmed_peer",
        "41.1 Topology Bootstrap - Gossip about unconfirmed peer",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Gossip about unconfirmed peer expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_1_node_state_memory_bound() -> None:
    spec = _build_spec(
        "gate_manager_41_1_node_state_memory_bound",
        "41.1 Topology Bootstrap - NodeState memory bound",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "NodeState memory bound expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_2_retry_dispatch_uses_original_bytes() -> None:
    spec = _build_spec(
        "gate_manager_41_2_retry_dispatch_uses_original_bytes",
        "41.2 Dispatch Retry Data Preservation - Retry dispatch uses original bytes",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Retry dispatch expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_41_2_failed_worker_exclusion() -> None:
    spec = _build_spec(
        "gate_manager_41_2_failed_worker_exclusion",
        "41.2 Dispatch Retry Data Preservation - Failed worker exclusion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Failed worker exclusion expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_2_retry_after_partial_ack() -> None:
    spec = _build_spec(
        "gate_manager_41_2_retry_after_partial_ack",
        "41.2 Dispatch Retry Data Preservation - Retry after partial ACK",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Retry after partial ACK expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_2_corrupted_original_bytes() -> None:
    spec = _build_spec(
        "gate_manager_41_2_corrupted_original_bytes",
        "41.2 Dispatch Retry Data Preservation - Corrupted original bytes",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Corrupted original bytes expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_2_concurrent_retries() -> None:
    spec = _build_spec(
        "gate_manager_41_2_concurrent_retries",
        "41.2 Dispatch Retry Data Preservation - Concurrent retries",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Concurrent retries expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_41_3_leader_dispatches_current_term() -> None:
    spec = _build_spec(
        "gate_manager_41_3_leader_dispatches_current_term",
        "41.3 Fencing Tokens - Leader gate dispatches with current term",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Leader dispatch expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_3_stale_leader_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_41_3_stale_leader_dispatch",
        "41.3 Fencing Tokens - Stale leader dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Stale leader dispatch expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_3_leadership_transfer_mid_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_41_3_leadership_transfer_mid_dispatch",
        "41.3 Fencing Tokens - Leadership transfer mid-dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Leadership transfer mid-dispatch expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_3_split_brain_partition() -> None:
    spec = _build_spec(
        "gate_manager_41_3_split_brain_partition",
        "41.3 Fencing Tokens - Split-brain partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, "Split-brain expected quorum circuit"
    finally:
        await runtime.stop_cluster()


async def validate_41_3_cancellation_from_stale_leader() -> None:
    spec = _build_spec(
        "gate_manager_41_3_cancellation_from_stale_leader",
        "41.3 Fencing Tokens - Cancellation from stale leader",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Cancellation from stale leader expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_4_leader_change_state_sync_backoff() -> None:
    spec = _build_spec(
        "gate_manager_41_4_leader_change_state_sync_backoff",
        "41.4 State Sync Retries - Leader change",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Leader change expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_4_peer_manager_unreachable() -> None:
    spec = _build_spec(
        "gate_manager_41_4_peer_manager_unreachable",
        "41.4 State Sync Retries - Peer manager unreachable",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Peer manager unreachable expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_4_backoff_jitter() -> None:
    spec = _build_spec(
        "gate_manager_41_4_backoff_jitter",
        "41.4 State Sync Retries - Backoff jitter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Backoff jitter expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_4_sync_race_with_shutdown() -> None:
    spec = _build_spec(
        "gate_manager_41_4_sync_race_with_shutdown",
        "41.4 State Sync Retries - Sync race with shutdown",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Sync race with shutdown expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_4_sync_after_partial_state() -> None:
    spec = _build_spec(
        "gate_manager_41_4_sync_after_partial_state",
        "41.4 State Sync Retries - Sync after partial state",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Sync after partial state expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_5_same_idempotency_key_two_gates() -> None:
    spec = _build_spec(
        "gate_manager_41_5_same_idempotency_key_two_gates",
        "41.5 Idempotent Job Submission - Same idempotency key to two gates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Idempotent job submission expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_5_pending_entry_wait() -> None:
    spec = _build_spec(
        "gate_manager_41_5_pending_entry_wait",
        "41.5 Idempotent Job Submission - Pending entry wait",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Pending entry wait expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_5_key_expiry_during_retry() -> None:
    spec = _build_spec(
        "gate_manager_41_5_key_expiry_during_retry",
        "41.5 Idempotent Job Submission - Key expiry during retry",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Key expiry during retry expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_5_same_key_different_payload() -> None:
    spec = _build_spec(
        "gate_manager_41_5_same_key_different_payload",
        "41.5 Idempotent Job Submission - Same key, different payload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Same key different payload expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_5_idempotency_cache_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_41_5_idempotency_cache_cleanup",
        "41.5 Idempotent Job Submission - Idempotency cache cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Idempotency cache cleanup expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_6_primary_dc_lacks_cores() -> None:
    spec = _build_spec(
        "gate_manager_41_6_primary_dc_lacks_cores",
        "41.6 Capacity-Aware Spillover - Primary DC lacks cores",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Primary DC lacks cores expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_6_primary_wait_below_threshold() -> None:
    spec = _build_spec(
        "gate_manager_41_6_primary_wait_below_threshold",
        "41.6 Capacity-Aware Spillover - Primary wait time below threshold",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Primary wait below threshold expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_6_spillover_latency_penalty() -> None:
    spec = _build_spec(
        "gate_manager_41_6_spillover_latency_penalty",
        "41.6 Capacity-Aware Spillover - Spillover latency penalty too high",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Spillover latency penalty expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_6_stale_capacity_heartbeat() -> None:
    spec = _build_spec(
        "gate_manager_41_6_stale_capacity_heartbeat",
        "41.6 Capacity-Aware Spillover - Stale capacity heartbeat",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Stale capacity heartbeat expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_6_core_freeing_schedule() -> None:
    spec = _build_spec(
        "gate_manager_41_6_core_freeing_schedule",
        "41.6 Capacity-Aware Spillover - Core freeing schedule",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "Core freeing schedule expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_7_initial_routing_uses_rtt_ucb() -> None:
    spec = _build_spec(
        "gate_manager_41_7_initial_routing_uses_rtt_ucb",
        "41.7 Adaptive Route Learning - Initial routing uses RTT UCB",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "Initial routing expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_7_observed_latency_samples_accumulate() -> None:
    spec = _build_spec(
        "gate_manager_41_7_observed_latency_samples_accumulate",
        "41.7 Adaptive Route Learning - Observed latency samples accumulate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Observed latency samples expected latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_7_stale_observations() -> None:
    spec = _build_spec(
        "gate_manager_41_7_stale_observations",
        "41.7 Adaptive Route Learning - Stale observations",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Stale observations expected latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_7_late_latency_sample() -> None:
    spec = _build_spec(
        "gate_manager_41_7_late_latency_sample",
        "41.7 Adaptive Route Learning - Late latency sample",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Late latency sample expected latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_7_routing_hysteresis() -> None:
    spec = _build_spec(
        "gate_manager_41_7_routing_hysteresis",
        "41.7 Adaptive Route Learning - Routing hysteresis",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, (
            "Routing hysteresis expected blended scorer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_8_job_retry_budget_shared() -> None:
    spec = _build_spec(
        "gate_manager_41_8_job_retry_budget_shared",
        "41.8 Retry Budgets - Job retry budget shared",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Retry budget expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_8_per_workflow_cap_enforced() -> None:
    spec = _build_spec(
        "gate_manager_41_8_per_workflow_cap_enforced",
        "41.8 Retry Budgets - Per-workflow cap enforced",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Per-workflow cap expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_8_budget_exhausted() -> None:
    spec = _build_spec(
        "gate_manager_41_8_budget_exhausted",
        "41.8 Retry Budgets - Budget exhausted",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Budget exhausted expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_8_best_effort_min_dcs_met() -> None:
    spec = _build_spec(
        "gate_manager_41_8_best_effort_min_dcs_met",
        "41.8 Retry Budgets - Best-effort min_dcs met",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Best-effort min_dcs expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_8_best_effort_deadline_hit() -> None:
    spec = _build_spec(
        "gate_manager_41_8_best_effort_deadline_hit",
        "41.8 Retry Budgets - Best-effort deadline hit",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Best-effort deadline hit expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_9_manager_signals_throttle() -> None:
    spec = _build_spec(
        "gate_manager_41_9_manager_signals_throttle",
        "41.9 Explicit Backpressure - Manager signals THROTTLE",
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
            "Manager throttle expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_9_manager_signals_batch() -> None:
    spec = _build_spec(
        "gate_manager_41_9_manager_signals_batch",
        "41.9 Explicit Backpressure - Manager signals BATCH",
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
            "Manager batch expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_9_manager_signals_reject() -> None:
    spec = _build_spec(
        "gate_manager_41_9_manager_signals_reject",
        "41.9 Explicit Backpressure - Manager signals REJECT",
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
            "Manager reject expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_9_critical_messages_never_shed() -> None:
    spec = _build_spec(
        "gate_manager_41_9_critical_messages_never_shed",
        "41.9 Explicit Backpressure - CRITICAL messages under overload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, "Critical messages expected load shedder"
    finally:
        await runtime.stop_cluster()


async def validate_41_9_stats_buffer_bounds() -> None:
    spec = _build_spec(
        "gate_manager_41_9_stats_buffer_bounds",
        "41.9 Explicit Backpressure - Stats buffer bounds",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats buffer bounds expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_10_job_create_cancel_committed() -> None:
    spec = _build_spec(
        "gate_manager_41_10_job_create_cancel_committed",
        "41.10 Durability - Job create/cancel committed globally",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Job create/cancel expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_41_10_workflow_dispatch_committed() -> None:
    spec = _build_spec(
        "gate_manager_41_10_workflow_dispatch_committed",
        "41.10 Durability - Workflow dispatch committed regionally",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Workflow dispatch committed expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_10_wal_backpressure() -> None:
    spec = _build_spec(
        "gate_manager_41_10_wal_backpressure",
        "41.10 Durability - WAL backpressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, "WAL backpressure expected load shedder"
    finally:
        await runtime.stop_cluster()


async def validate_41_10_wal_recovery() -> None:
    spec = _build_spec(
        "gate_manager_41_10_wal_recovery",
        "41.10 Durability - WAL recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "WAL recovery expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_10_data_plane_stats() -> None:
    spec = _build_spec(
        "gate_manager_41_10_data_plane_stats",
        "41.10 Durability - Data-plane stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Data-plane stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_11_context_from_workflow_a_to_b() -> None:
    spec = _build_spec(
        "gate_manager_41_11_context_from_workflow_a_to_b",
        "41.11 Workflow Context - Context from workflow A to B across DCs",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Context propagation expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_41_11_worker_dies_mid_workflow() -> None:
    spec = _build_spec(
        "gate_manager_41_11_worker_dies_mid_workflow",
        "41.11 Workflow Context - Worker dies mid-workflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Worker dies mid-workflow expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_11_context_update_arrives_late() -> None:
    spec = _build_spec(
        "gate_manager_41_11_context_update_arrives_late",
        "41.11 Workflow Context - Context update arrives late",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Context update arrives late expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_11_context_snapshot_during_transfer() -> None:
    spec = _build_spec(
        "gate_manager_41_11_context_snapshot_during_transfer",
        "41.11 Workflow Context - Context snapshot during leader transfer",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Context snapshot expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_11_empty_context() -> None:
    spec = _build_spec(
        "gate_manager_41_11_empty_context",
        "41.11 Workflow Context - Empty context",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Empty context expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_41_12_worker_registers_with_manager_a() -> None:
    spec = _build_spec(
        "gate_manager_41_12_worker_registers_with_manager_a",
        "41.12 Cross-Manager Worker Visibility - Worker registers with Manager A",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Worker registration expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_12_missed_broadcast_gossip_converges() -> None:
    spec = _build_spec(
        "gate_manager_41_12_missed_broadcast_gossip_converges",
        "41.12 Cross-Manager Worker Visibility - Missed broadcast",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Missed broadcast expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_12_stale_incarnation_update() -> None:
    spec = _build_spec(
        "gate_manager_41_12_stale_incarnation_update",
        "41.12 Cross-Manager Worker Visibility - Stale incarnation update",
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
            "Stale incarnation update expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_12_owner_manager_down() -> None:
    spec = _build_spec(
        "gate_manager_41_12_owner_manager_down",
        "41.12 Cross-Manager Worker Visibility - Owner manager down",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Owner manager down expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_12_manager_joins_late() -> None:
    spec = _build_spec(
        "gate_manager_41_12_manager_joins_late",
        "41.12 Cross-Manager Worker Visibility - Manager joins late",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Manager joins late expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_13_cpu_warn_threshold() -> None:
    spec = _build_spec(
        "gate_manager_41_13_cpu_warn_threshold",
        "41.13 Resource Guards - CPU exceeds warn threshold",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "CPU warn threshold expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_13_cpu_throttle_threshold() -> None:
    spec = _build_spec(
        "gate_manager_41_13_cpu_throttle_threshold",
        "41.13 Resource Guards - CPU exceeds throttle threshold",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "CPU throttle threshold expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_13_memory_kill_threshold() -> None:
    spec = _build_spec(
        "gate_manager_41_13_memory_kill_threshold",
        "41.13 Resource Guards - Memory exceeds kill threshold",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Memory kill threshold expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_13_process_tree_monitoring() -> None:
    spec = _build_spec(
        "gate_manager_41_13_process_tree_monitoring",
        "41.13 Resource Guards - Process tree monitoring",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Process tree monitoring expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_13_high_uncertainty_enforcement_delay() -> None:
    spec = _build_spec(
        "gate_manager_41_13_high_uncertainty_enforcement_delay",
        "41.13 Resource Guards - High uncertainty enforcement delay",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "High uncertainty enforcement expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_14_p95_exceeds_threshold() -> None:
    spec = _build_spec(
        "gate_manager_41_14_p95_exceeds_threshold",
        "41.14 SLO-Aware Health - p95 exceeds threshold",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, "SLO routing expected blended scorer"
    finally:
        await runtime.stop_cluster()


async def validate_41_14_t_digest_merge_across_managers() -> None:
    spec = _build_spec(
        "gate_manager_41_14_t_digest_merge_across_managers",
        "41.14 SLO-Aware Health - T-Digest merge across managers",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "T-Digest merge expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_14_sparse_samples() -> None:
    spec = _build_spec(
        "gate_manager_41_14_sparse_samples",
        "41.14 SLO-Aware Health - Sparse samples",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._coordinate_tracker is not None, (
            "Sparse samples expected coordinate tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_14_slo_data_stale() -> None:
    spec = _build_spec(
        "gate_manager_41_14_slo_data_stale",
        "41.14 SLO-Aware Health - SLO data stale",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, (
            "SLO data stale expected blended scorer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_14_slo_violation_with_good_rtt() -> None:
    spec = _build_spec(
        "gate_manager_41_14_slo_violation_with_good_rtt",
        "41.14 SLO-Aware Health - SLO violation with good RTT",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, "SLO violation expected blended scorer"
    finally:
        await runtime.stop_cluster()


async def validate_41_15_leader_manager_overloaded_alert() -> None:
    spec = _build_spec(
        "gate_manager_41_15_leader_manager_overloaded_alert",
        "41.15 Manager Health Aggregation Alerts - Leader manager overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Leader manager overloaded expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_15_majority_overloaded_alert() -> None:
    spec = _build_spec(
        "gate_manager_41_15_majority_overloaded_alert",
        "41.15 Manager Health Aggregation Alerts - Majority overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Majority overloaded expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_15_high_non_healthy_ratio_warning() -> None:
    spec = _build_spec(
        "gate_manager_41_15_high_non_healthy_ratio_warning",
        "41.15 Manager Health Aggregation Alerts - High non-healthy ratio",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "High non-healthy ratio expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_15_peer_recovery_info() -> None:
    spec = _build_spec(
        "gate_manager_41_15_peer_recovery_info",
        "41.15 Manager Health Aggregation Alerts - Peer recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Peer recovery expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_15_no_peers_aggregation_skipped() -> None:
    spec = _build_spec(
        "gate_manager_41_15_no_peers_aggregation_skipped",
        "41.15 Manager Health Aggregation Alerts - No peers",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "No peers aggregation expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_16_worker_lifecycle_events_logged() -> None:
    spec = _build_spec(
        "gate_manager_41_16_worker_lifecycle_events_logged",
        "41.16 Worker Event Logging - Worker job lifecycle events logged",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Worker lifecycle logging expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_16_action_events_under_load() -> None:
    spec = _build_spec(
        "gate_manager_41_16_action_events_under_load",
        "41.16 Worker Event Logging - Action events under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Action events expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_16_event_log_overflow() -> None:
    spec = _build_spec(
        "gate_manager_41_16_event_log_overflow",
        "41.16 Worker Event Logging - Event log overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Event log overflow expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_16_log_rotation() -> None:
    spec = _build_spec(
        "gate_manager_41_16_log_rotation",
        "41.16 Worker Event Logging - Log rotation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Log rotation expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_16_crash_forensics() -> None:
    spec = _build_spec(
        "gate_manager_41_16_crash_forensics",
        "41.16 Worker Event Logging - Crash forensics",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Crash forensics expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_17_gossip_informed_death() -> None:
    spec = _build_spec(
        "gate_manager_41_17_gossip_informed_death",
        "41.17 Failure Detection - Gossip-informed death",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Gossip-informed death expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_17_timer_starvation_case() -> None:
    spec = _build_spec(
        "gate_manager_41_17_timer_starvation_case",
        "41.17 Failure Detection - Timer starvation case",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Timer starvation expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_17_job_layer_suspicion() -> None:
    spec = _build_spec(
        "gate_manager_41_17_job_layer_suspicion",
        "41.17 Failure Detection - Job-layer suspicion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Job-layer suspicion expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_17_refutation_race() -> None:
    spec = _build_spec(
        "gate_manager_41_17_refutation_race",
        "41.17 Failure Detection - Refutation race",
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
            "Refutation race expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_17_global_death_clears_job_suspicions() -> None:
    spec = _build_spec(
        "gate_manager_41_17_global_death_clears_job_suspicions",
        "41.17 Failure Detection - Global death clears job suspicions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Global death expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_18_client_rate_limit_exceeded() -> None:
    spec = _build_spec(
        "gate_manager_41_18_client_rate_limit_exceeded",
        "41.18 Rate Limiting - Client rate limit exceeded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, "Client rate limit expected rate limiter"
    finally:
        await runtime.stop_cluster()


async def validate_41_18_server_side_limit_enforced() -> None:
    spec = _build_spec(
        "gate_manager_41_18_server_side_limit_enforced",
        "41.18 Rate Limiting - Server-side limit enforced",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, "Server-side limit expected rate limiter"
    finally:
        await runtime.stop_cluster()


async def validate_41_18_mixed_protocol_versions() -> None:
    spec = _build_spec(
        "gate_manager_41_18_mixed_protocol_versions",
        "41.18 Rate Limiting - Mixed protocol versions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Mixed protocol versions expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_18_unknown_fields_ignored() -> None:
    spec = _build_spec(
        "gate_manager_41_18_unknown_fields_ignored",
        "41.18 Rate Limiting - Unknown fields ignored",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Unknown fields expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_18_major_version_mismatch() -> None:
    spec = _build_spec(
        "gate_manager_41_18_major_version_mismatch",
        "41.18 Rate Limiting - Major version mismatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Major version mismatch expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_19_leadership_transfer_state_sync_no_deadlock() -> None:
    spec = _build_spec(
        "gate_manager_41_19_leadership_transfer_state_sync_no_deadlock",
        "41.19 Deadlock and Lock Ordering - Gate leadership transfer + state sync",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Leadership transfer state sync expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_19_manager_job_lock_context_update() -> None:
    spec = _build_spec(
        "gate_manager_41_19_manager_job_lock_context_update",
        "41.19 Deadlock and Lock Ordering - Manager job lock + context update",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Manager job lock expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_41_19_retry_budget_update_cleanup_loop() -> None:
    spec = _build_spec(
        "gate_manager_41_19_retry_budget_update_cleanup_loop",
        "41.19 Deadlock and Lock Ordering - Retry budget update + cleanup loop",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Retry budget update expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_19_wal_backpressure_shutdown() -> None:
    spec = _build_spec(
        "gate_manager_41_19_wal_backpressure_shutdown",
        "41.19 Deadlock and Lock Ordering - WAL backpressure + shutdown",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "WAL backpressure shutdown expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_19_cancellation_timeout_loops() -> None:
    spec = _build_spec(
        "gate_manager_41_19_cancellation_timeout_loops",
        "41.19 Deadlock and Lock Ordering - Cancellation + timeout loops",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Cancellation timeout loops expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_20_cross_dc_probe_timeout_scaled() -> None:
    spec = _build_spec(
        "gate_manager_41_20_cross_dc_probe_timeout_scaled",
        "41.20 Federated Health Monitoring - Cross-DC probe timeout scaled",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Cross-DC probe timeout expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_20_dc_leader_change_mid_probe() -> None:
    spec = _build_spec(
        "gate_manager_41_20_dc_leader_change_mid_probe",
        "41.20 Federated Health Monitoring - DC leader change mid-probe",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "DC leader change expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_20_stale_cross_dc_incarnation() -> None:
    spec = _build_spec(
        "gate_manager_41_20_stale_cross_dc_incarnation",
        "41.20 Federated Health Monitoring - Stale cross-DC incarnation",
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
            "Stale cross-DC incarnation expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_20_probe_jitter_distribution() -> None:
    spec = _build_spec(
        "gate_manager_41_20_probe_jitter_distribution",
        "41.20 Federated Health Monitoring - Probe jitter distribution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Probe jitter expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_20_correlation_detector_gating() -> None:
    spec = _build_spec(
        "gate_manager_41_20_correlation_detector_gating",
        "41.20 Federated Health Monitoring - Correlation detector gating",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Correlation detector expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_21_pre_vote_prevents_split_brain() -> None:
    spec = _build_spec(
        "gate_manager_41_21_pre_vote_prevents_split_brain",
        "41.21 Pre-Voting and Quorum - Pre-vote prevents split-brain",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, "Pre-vote expected quorum circuit"
    finally:
        await runtime.stop_cluster()


async def validate_41_21_quorum_size_from_config() -> None:
    spec = _build_spec(
        "gate_manager_41_21_quorum_size_from_config",
        "41.21 Pre-Voting and Quorum - Quorum size from config",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_quorum_size", None)), (
            "Quorum size expected quorum calculation"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_21_quorum_circuit_breaker() -> None:
    spec = _build_spec(
        "gate_manager_41_21_quorum_circuit_breaker",
        "41.21 Pre-Voting and Quorum - Quorum circuit breaker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Quorum circuit breaker expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_21_quorum_recovery() -> None:
    spec = _build_spec(
        "gate_manager_41_21_quorum_recovery",
        "41.21 Pre-Voting and Quorum - Quorum recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Quorum recovery expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_21_minority_partition() -> None:
    spec = _build_spec(
        "gate_manager_41_21_minority_partition",
        "41.21 Pre-Voting and Quorum - Minority partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Minority partition expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_22_extension_granted_with_progress() -> None:
    spec = _build_spec(
        "gate_manager_41_22_extension_granted_with_progress",
        "41.22 Adaptive Healthcheck Extensions - Extension granted with progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Extension granted expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_22_extension_denied_without_progress() -> None:
    spec = _build_spec(
        "gate_manager_41_22_extension_denied_without_progress",
        "41.22 Adaptive Healthcheck Extensions - Extension denied without progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Extension denied expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_22_extension_cap_reached() -> None:
    spec = _build_spec(
        "gate_manager_41_22_extension_cap_reached",
        "41.22 Adaptive Healthcheck Extensions - Extension cap reached",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Extension cap reached expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_22_extension_global_timeout() -> None:
    spec = _build_spec(
        "gate_manager_41_22_extension_global_timeout",
        "41.22 Adaptive Healthcheck Extensions - Extension + global timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Extension + global timeout expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_22_extension_during_overload() -> None:
    spec = _build_spec(
        "gate_manager_41_22_extension_during_overload",
        "41.22 Adaptive Healthcheck Extensions - Extension during overload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Extension during overload expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_23_cluster_env_mismatch() -> None:
    spec = _build_spec(
        "gate_manager_41_23_cluster_env_mismatch",
        "41.23 DNS Discovery and Role Validation - Cluster/env mismatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Cluster/env mismatch expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_23_role_based_connection_matrix() -> None:
    spec = _build_spec(
        "gate_manager_41_23_role_based_connection_matrix",
        "41.23 DNS Discovery and Role Validation - Role-based connection matrix",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Role-based connection matrix expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_23_rendezvous_hash_stability() -> None:
    spec = _build_spec(
        "gate_manager_41_23_rendezvous_hash_stability",
        "41.23 DNS Discovery and Role Validation - Rendezvous hash stability",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Rendezvous hash stability expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_23_power_of_two_choice() -> None:
    spec = _build_spec(
        "gate_manager_41_23_power_of_two_choice",
        "41.23 DNS Discovery and Role Validation - Power-of-two choice",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Power-of-two choice expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_41_23_sticky_pool_eviction() -> None:
    spec = _build_spec(
        "gate_manager_41_23_sticky_pool_eviction",
        "41.23 DNS Discovery and Role Validation - Sticky pool eviction",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Sticky pool eviction expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_41_24_full_jitter_distribution() -> None:
    spec = _build_spec(
        "gate_manager_41_24_full_jitter_distribution",
        "41.24 Retry Framework Jitter - Full jitter distribution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Full jitter distribution expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_24_decorrelated_jitter() -> None:
    spec = _build_spec(
        "gate_manager_41_24_decorrelated_jitter",
        "41.24 Retry Framework Jitter - Decorrelated jitter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Decorrelated jitter expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_24_jitter_backoff_cap() -> None:
    spec = _build_spec(
        "gate_manager_41_24_jitter_backoff_cap",
        "41.24 Retry Framework Jitter - Jitter + backoff cap",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Jitter backoff cap expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_24_retryable_exception_filter() -> None:
    spec = _build_spec(
        "gate_manager_41_24_retryable_exception_filter",
        "41.24 Retry Framework Jitter - Retryable exception filter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Retryable exception filter expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_24_backoff_under_recovery() -> None:
    spec = _build_spec(
        "gate_manager_41_24_backoff_under_recovery",
        "41.24 Retry Framework Jitter - Backoff under recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Backoff under recovery expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_25_cancellation_beats_completion() -> None:
    spec = _build_spec(
        "gate_manager_41_25_cancellation_beats_completion",
        "41.25 Global Job Ledger Consistency - Cancellation beats completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Cancellation beats completion expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_25_higher_fence_token_wins() -> None:
    spec = _build_spec(
        "gate_manager_41_25_higher_fence_token_wins",
        "41.25 Global Job Ledger Consistency - Higher fence token wins",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Higher fence token expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_25_hlc_ordering() -> None:
    spec = _build_spec(
        "gate_manager_41_25_hlc_ordering",
        "41.25 Global Job Ledger Consistency - HLC ordering",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._state_version is not None, "HLC ordering expected state version"
    finally:
        await runtime.stop_cluster()


async def validate_41_25_regional_vs_global_durability() -> None:
    spec = _build_spec(
        "gate_manager_41_25_regional_vs_global_durability",
        "41.25 Global Job Ledger Consistency - Regional vs global durability",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Regional durability expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_41_25_ledger_repair() -> None:
    spec = _build_spec(
        "gate_manager_41_25_ledger_repair",
        "41.25 Global Job Ledger Consistency - Ledger repair",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Ledger repair expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_26_fsync_batch_overflow() -> None:
    spec = _build_spec(
        "gate_manager_41_26_fsync_batch_overflow",
        "41.26 Logger WAL Extensions - FSYNC batch overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "FSYNC batch overflow expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_26_read_back_recovery() -> None:
    spec = _build_spec(
        "gate_manager_41_26_read_back_recovery",
        "41.26 Logger WAL Extensions - Read-back recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Read-back recovery expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_26_file_lock_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_41_26_file_lock_cleanup",
        "41.26 Logger WAL Extensions - File lock cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "File lock cleanup expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_26_sequence_number_monotonic() -> None:
    spec = _build_spec(
        "gate_manager_41_26_sequence_number_monotonic",
        "41.26 Logger WAL Extensions - Sequence number monotonic",
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
            "Sequence number monotonic expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_26_data_plane_mode() -> None:
    spec = _build_spec(
        "gate_manager_41_26_data_plane_mode",
        "41.26 Logger WAL Extensions - Data-plane mode",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Data-plane mode expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_27_healthcheck_events_logged() -> None:
    spec = _build_spec(
        "gate_manager_41_27_healthcheck_events_logged",
        "41.27 Worker Event Log Fidelity - Healthcheck events",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Healthcheck events expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_27_action_failure_logging() -> None:
    spec = _build_spec(
        "gate_manager_41_27_action_failure_logging",
        "41.27 Worker Event Log Fidelity - Action failure logging",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Action failure logging expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_27_log_buffer_saturation() -> None:
    spec = _build_spec(
        "gate_manager_41_27_log_buffer_saturation",
        "41.27 Worker Event Log Fidelity - Log buffer saturation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Log buffer saturation expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_27_log_retention() -> None:
    spec = _build_spec(
        "gate_manager_41_27_log_retention",
        "41.27 Worker Event Log Fidelity - Log retention",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Log retention expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_27_shutdown_event_ordering() -> None:
    spec = _build_spec(
        "gate_manager_41_27_shutdown_event_ordering",
        "41.27 Worker Event Log Fidelity - Shutdown event ordering",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Shutdown event ordering expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_28_context_update_on_completion() -> None:
    spec = _build_spec(
        "gate_manager_41_28_context_update_on_completion",
        "41.28 Context Consistency - Context update on completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Context update expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_41_28_concurrent_providers_conflict() -> None:
    spec = _build_spec(
        "gate_manager_41_28_concurrent_providers_conflict",
        "41.28 Context Consistency - Concurrent providers",
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
            "Concurrent providers expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_28_redispatch_with_stored_context() -> None:
    spec = _build_spec(
        "gate_manager_41_28_redispatch_with_stored_context",
        "41.28 Context Consistency - Re-dispatch with stored context",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Re-dispatch with stored context expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_28_context_snapshot_during_state_sync() -> None:
    spec = _build_spec(
        "gate_manager_41_28_context_snapshot_during_state_sync",
        "41.28 Context Consistency - Context snapshot during state sync",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Context snapshot expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_28_context_for_unknown_workflow() -> None:
    spec = _build_spec(
        "gate_manager_41_28_context_for_unknown_workflow",
        "41.28 Context Consistency - Context for unknown workflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Context for unknown workflow expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_29_slo_violation_low_rtt() -> None:
    spec = _build_spec(
        "gate_manager_41_29_slo_violation_low_rtt",
        "41.29 SLO and Resource Correlation - SLO violation with low RTT",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._blended_scorer is not None, "SLO violation expected blended scorer"
    finally:
        await runtime.stop_cluster()


async def validate_41_29_cpu_pressure_predicts_latency() -> None:
    spec = _build_spec(
        "gate_manager_41_29_cpu_pressure_predicts_latency",
        "41.29 SLO and Resource Correlation - CPU pressure predicts latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "CPU pressure expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_29_memory_pressure_spikes() -> None:
    spec = _build_spec(
        "gate_manager_41_29_memory_pressure_spikes",
        "41.29 SLO and Resource Correlation - Memory pressure spikes",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Memory pressure expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_29_percentile_window_rotation() -> None:
    spec = _build_spec(
        "gate_manager_41_29_percentile_window_rotation",
        "41.29 SLO and Resource Correlation - Percentile window rotation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Percentile window rotation expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_29_t_digest_merge_ordering() -> None:
    spec = _build_spec(
        "gate_manager_41_29_t_digest_merge_ordering",
        "41.29 SLO and Resource Correlation - T-Digest merge ordering",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "T-Digest merge ordering expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_30_global_in_flight_limit_reached() -> None:
    spec = _build_spec(
        "gate_manager_41_30_global_in_flight_limit_reached",
        "41.30 Bounded Execution - Global in-flight limit reached",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Global in-flight limit expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_30_per_priority_limits_enforced() -> None:
    spec = _build_spec(
        "gate_manager_41_30_per_priority_limits_enforced",
        "41.30 Bounded Execution - Per-priority limits enforced",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Per-priority limits expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_30_destination_queue_overflow() -> None:
    spec = _build_spec(
        "gate_manager_41_30_destination_queue_overflow",
        "41.30 Bounded Execution - Destination queue overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Destination queue overflow expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_30_slow_destination_isolation() -> None:
    spec = _build_spec(
        "gate_manager_41_30_slow_destination_isolation",
        "41.30 Bounded Execution - Slow destination isolation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Slow destination isolation expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_41_30_queue_state_recovery() -> None:
    spec = _build_spec(
        "gate_manager_41_30_queue_state_recovery",
        "41.30 Bounded Execution - Queue state recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Queue state recovery expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_41_1_all_gates_start_concurrently()
    await validate_41_1_managers_start_before_gates()
    await validate_41_1_unconfirmed_peer_never_responds()
    await validate_41_1_gossip_unconfirmed_peer()
    await validate_41_1_node_state_memory_bound()
    await validate_41_2_retry_dispatch_uses_original_bytes()
    await validate_41_2_failed_worker_exclusion()
    await validate_41_2_retry_after_partial_ack()
    await validate_41_2_corrupted_original_bytes()
    await validate_41_2_concurrent_retries()
    await validate_41_3_leader_dispatches_current_term()
    await validate_41_3_stale_leader_dispatch()
    await validate_41_3_leadership_transfer_mid_dispatch()
    await validate_41_3_split_brain_partition()
    await validate_41_3_cancellation_from_stale_leader()
    await validate_41_4_leader_change_state_sync_backoff()
    await validate_41_4_peer_manager_unreachable()
    await validate_41_4_backoff_jitter()
    await validate_41_4_sync_race_with_shutdown()
    await validate_41_4_sync_after_partial_state()
    await validate_41_5_same_idempotency_key_two_gates()
    await validate_41_5_pending_entry_wait()
    await validate_41_5_key_expiry_during_retry()
    await validate_41_5_same_key_different_payload()
    await validate_41_5_idempotency_cache_cleanup()
    await validate_41_6_primary_dc_lacks_cores()
    await validate_41_6_primary_wait_below_threshold()
    await validate_41_6_spillover_latency_penalty()
    await validate_41_6_stale_capacity_heartbeat()
    await validate_41_6_core_freeing_schedule()
    await validate_41_7_initial_routing_uses_rtt_ucb()
    await validate_41_7_observed_latency_samples_accumulate()
    await validate_41_7_stale_observations()
    await validate_41_7_late_latency_sample()
    await validate_41_7_routing_hysteresis()
    await validate_41_8_job_retry_budget_shared()
    await validate_41_8_per_workflow_cap_enforced()
    await validate_41_8_budget_exhausted()
    await validate_41_8_best_effort_min_dcs_met()
    await validate_41_8_best_effort_deadline_hit()
    await validate_41_9_manager_signals_throttle()
    await validate_41_9_manager_signals_batch()
    await validate_41_9_manager_signals_reject()
    await validate_41_9_critical_messages_never_shed()
    await validate_41_9_stats_buffer_bounds()
    await validate_41_10_job_create_cancel_committed()
    await validate_41_10_workflow_dispatch_committed()
    await validate_41_10_wal_backpressure()
    await validate_41_10_wal_recovery()
    await validate_41_10_data_plane_stats()
    await validate_41_11_context_from_workflow_a_to_b()
    await validate_41_11_worker_dies_mid_workflow()
    await validate_41_11_context_update_arrives_late()
    await validate_41_11_context_snapshot_during_transfer()
    await validate_41_11_empty_context()
    await validate_41_12_worker_registers_with_manager_a()
    await validate_41_12_missed_broadcast_gossip_converges()
    await validate_41_12_stale_incarnation_update()
    await validate_41_12_owner_manager_down()
    await validate_41_12_manager_joins_late()
    await validate_41_13_cpu_warn_threshold()
    await validate_41_13_cpu_throttle_threshold()
    await validate_41_13_memory_kill_threshold()
    await validate_41_13_process_tree_monitoring()
    await validate_41_13_high_uncertainty_enforcement_delay()
    await validate_41_14_p95_exceeds_threshold()
    await validate_41_14_t_digest_merge_across_managers()
    await validate_41_14_sparse_samples()
    await validate_41_14_slo_data_stale()
    await validate_41_14_slo_violation_with_good_rtt()
    await validate_41_15_leader_manager_overloaded_alert()
    await validate_41_15_majority_overloaded_alert()
    await validate_41_15_high_non_healthy_ratio_warning()
    await validate_41_15_peer_recovery_info()
    await validate_41_15_no_peers_aggregation_skipped()
    await validate_41_16_worker_lifecycle_events_logged()
    await validate_41_16_action_events_under_load()
    await validate_41_16_event_log_overflow()
    await validate_41_16_log_rotation()
    await validate_41_16_crash_forensics()
    await validate_41_17_gossip_informed_death()
    await validate_41_17_timer_starvation_case()
    await validate_41_17_job_layer_suspicion()
    await validate_41_17_refutation_race()
    await validate_41_17_global_death_clears_job_suspicions()
    await validate_41_18_client_rate_limit_exceeded()
    await validate_41_18_server_side_limit_enforced()
    await validate_41_18_mixed_protocol_versions()
    await validate_41_18_unknown_fields_ignored()
    await validate_41_18_major_version_mismatch()
    await validate_41_19_leadership_transfer_state_sync_no_deadlock()
    await validate_41_19_manager_job_lock_context_update()
    await validate_41_19_retry_budget_update_cleanup_loop()
    await validate_41_19_wal_backpressure_shutdown()
    await validate_41_19_cancellation_timeout_loops()
    await validate_41_20_cross_dc_probe_timeout_scaled()
    await validate_41_20_dc_leader_change_mid_probe()
    await validate_41_20_stale_cross_dc_incarnation()
    await validate_41_20_probe_jitter_distribution()
    await validate_41_20_correlation_detector_gating()
    await validate_41_21_pre_vote_prevents_split_brain()
    await validate_41_21_quorum_size_from_config()
    await validate_41_21_quorum_circuit_breaker()
    await validate_41_21_quorum_recovery()
    await validate_41_21_minority_partition()
    await validate_41_22_extension_granted_with_progress()
    await validate_41_22_extension_denied_without_progress()
    await validate_41_22_extension_cap_reached()
    await validate_41_22_extension_global_timeout()
    await validate_41_22_extension_during_overload()
    await validate_41_23_cluster_env_mismatch()
    await validate_41_23_role_based_connection_matrix()
    await validate_41_23_rendezvous_hash_stability()
    await validate_41_23_power_of_two_choice()
    await validate_41_23_sticky_pool_eviction()
    await validate_41_24_full_jitter_distribution()
    await validate_41_24_decorrelated_jitter()
    await validate_41_24_jitter_backoff_cap()
    await validate_41_24_retryable_exception_filter()
    await validate_41_24_backoff_under_recovery()
    await validate_41_25_cancellation_beats_completion()
    await validate_41_25_higher_fence_token_wins()
    await validate_41_25_hlc_ordering()
    await validate_41_25_regional_vs_global_durability()
    await validate_41_25_ledger_repair()
    await validate_41_26_fsync_batch_overflow()
    await validate_41_26_read_back_recovery()
    await validate_41_26_file_lock_cleanup()
    await validate_41_26_sequence_number_monotonic()
    await validate_41_26_data_plane_mode()
    await validate_41_27_healthcheck_events_logged()
    await validate_41_27_action_failure_logging()
    await validate_41_27_log_buffer_saturation()
    await validate_41_27_log_retention()
    await validate_41_27_shutdown_event_ordering()
    await validate_41_28_context_update_on_completion()
    await validate_41_28_concurrent_providers_conflict()
    await validate_41_28_redispatch_with_stored_context()
    await validate_41_28_context_snapshot_during_state_sync()
    await validate_41_28_context_for_unknown_workflow()
    await validate_41_29_slo_violation_low_rtt()
    await validate_41_29_cpu_pressure_predicts_latency()
    await validate_41_29_memory_pressure_spikes()
    await validate_41_29_percentile_window_rotation()
    await validate_41_29_t_digest_merge_ordering()
    await validate_41_30_global_in_flight_limit_reached()
    await validate_41_30_per_priority_limits_enforced()
    await validate_41_30_destination_queue_overflow()
    await validate_41_30_slow_destination_isolation()
    await validate_41_30_queue_state_recovery()


if __name__ == "__main__":
    asyncio.run(run())
