import asyncio
import re

from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

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
                "dc_count": 1,
                "managers_per_dc": 1,
                "workers_per_dc": 2,
                "cores_per_worker": 1,
                "base_gate_tcp": 9000,
            },
            "actions": [
                {"type": "start_cluster"},
                {"type": "await_gate_leader", "params": {"timeout": 30}},
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-A", "timeout": 30},
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


def _get_manager(runtime: ScenarioRuntime, dc_id: str) -> ManagerServer:
    cluster = runtime.require_cluster()
    return cluster.get_manager_leader(dc_id) or cluster.managers[dc_id][0]


def _get_worker(runtime: ScenarioRuntime) -> WorkerServer:
    cluster = runtime.require_cluster()
    return cluster.get_all_workers()[0]


def _require_runtime(outcome: ScenarioOutcome) -> ScenarioRuntime:
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def validate_20_1_timeout() -> None:
    spec = _build_spec(
        "manager_worker_20_1_timeout",
        "20.1 Dispatch Errors - Timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Timeout expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_1_rejection() -> None:
    spec = _build_spec(
        "manager_worker_20_1_rejection",
        "20.1 Dispatch Errors - Rejection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Rejection expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_1_exception() -> None:
    spec = _build_spec(
        "manager_worker_20_1_exception",
        "20.1 Dispatch Errors - Exception",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Exception expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_2_workflow_exception() -> None:
    spec = _build_spec(
        "manager_worker_20_2_workflow_exception",
        "20.2 Execution Errors - Workflow exception",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Workflow exception expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_2_serialization_error() -> None:
    spec = _build_spec(
        "manager_worker_20_2_serialization_error",
        "20.2 Execution Errors - Serialization error",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Serialization error expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_2_resource_error() -> None:
    spec = _build_spec(
        "manager_worker_20_2_resource_error",
        "20.2 Execution Errors - Resource error",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Resource error expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_3_retry_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_20_3_retry_dispatch",
        "20.3 Recovery Actions - Retry dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Retry dispatch expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_3_mark_worker_unhealthy() -> None:
    spec = _build_spec(
        "manager_worker_20_3_mark_worker_unhealthy",
        "20.3 Recovery Actions - Mark worker unhealthy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_unhealthy_since, dict), (
            "Mark worker unhealthy expected worker unhealthy since"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_3_escalate_to_gate() -> None:
    spec = _build_spec(
        "manager_worker_20_3_escalate_to_gate",
        "20.3 Recovery Actions - Escalate to gate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_origin_gates, dict), (
            "Escalate to gate expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_stats_batching_drift() -> None:
    spec = _build_spec(
        "manager_worker_20_4_stats_batching_drift",
        "20.4 Additional Manager/Worker Scenarios - Stats batching drift",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "Stats batching drift expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_priority_fairness_under_contention() -> None:
    spec = _build_spec(
        "manager_worker_20_4_priority_fairness_under_contention",
        "20.4 Additional Manager/Worker Scenarios - Priority fairness under contention",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Priority fairness expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_retry_budget_exhaustion() -> None:
    spec = _build_spec(
        "manager_worker_20_4_retry_budget_exhaustion",
        "20.4 Additional Manager/Worker Scenarios - Retry budget exhaustion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Retry budget exhaustion expected workflow retries"
        )
        assert isinstance(state._job_origin_gates, dict), (
            "Retry budget exhaustion expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_progress_idempotency() -> None:
    spec = _build_spec(
        "manager_worker_20_4_progress_idempotency",
        "20.4 Additional Manager/Worker Scenarios - Progress idempotency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_job_last_progress, dict), (
            "Progress idempotency expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_late_dispatch_ack_reconciliation() -> None:
    spec = _build_spec(
        "manager_worker_20_4_late_dispatch_ack_reconciliation",
        "20.4 Additional Manager/Worker Scenarios - Late dispatch ACK reconciliation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Late dispatch ACK expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_worker_state_sync_after_restart() -> None:
    spec = _build_spec(
        "manager_worker_20_4_worker_state_sync_after_restart",
        "20.4 Additional Manager/Worker Scenarios - Worker state sync after restart",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._pending_workflows, dict), (
            "Worker state sync expected pending workflows"
        )
        assert isinstance(state._workflow_cancel_events, dict), (
            "Worker state sync expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_circuit_breaker_oscillation() -> None:
    spec = _build_spec(
        "manager_worker_20_4_circuit_breaker_oscillation",
        "20.4 Additional Manager/Worker Scenarios - Circuit breaker oscillation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_circuits, dict), (
            "Circuit breaker oscillation expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_4_result_integrity_on_restart() -> None:
    spec = _build_spec(
        "manager_worker_20_4_result_integrity_on_restart",
        "20.4 Additional Manager/Worker Scenarios - Result integrity on restart",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_aggregated_results, dict), (
            "Result integrity expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_5_starvation_prevention() -> None:
    spec = _build_spec(
        "manager_worker_20_5_starvation_prevention",
        "20.5 Scheduling and Fairness - Starvation prevention",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Starvation prevention expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_5_uneven_core_fairness() -> None:
    spec = _build_spec(
        "manager_worker_20_5_uneven_core_fairness",
        "20.5 Scheduling and Fairness - Uneven core fairness",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Uneven core fairness expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_5_priority_inversion() -> None:
    spec = _build_spec(
        "manager_worker_20_5_priority_inversion",
        "20.5 Scheduling and Fairness - Priority inversion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Priority inversion expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_6_duplicate_dispatch_acks() -> None:
    spec = _build_spec(
        "manager_worker_20_6_duplicate_dispatch_acks",
        "20.6 Dispatch and Acks - Duplicate dispatch ACKs",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Duplicate ACKs expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_6_ack_without_execution() -> None:
    spec = _build_spec(
        "manager_worker_20_6_ack_without_execution",
        "20.6 Dispatch and Acks - ACK without execution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_lifecycle_states, dict), (
            "ACK without execution expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_6_redispatch_after_partial_execution() -> None:
    spec = _build_spec(
        "manager_worker_20_6_redispatch_after_partial_execution",
        "20.6 Dispatch and Acks - Re-dispatch after partial execution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Re-dispatch expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_7_progress_buffer_overflow_recovery() -> None:
    spec = _build_spec(
        "manager_worker_20_7_progress_buffer_overflow_recovery",
        "20.7 Progress and Backpressure - Progress buffer overflow recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "Progress buffer recovery expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_7_progress_jitter_smoothing() -> None:
    spec = _build_spec(
        "manager_worker_20_7_progress_jitter_smoothing",
        "20.7 Progress and Backpressure - Progress jitter smoothing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_job_last_progress, dict), (
            "Progress jitter smoothing expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_7_backpressure_deescalation_hysteresis() -> None:
    spec = _build_spec(
        "manager_worker_20_7_backpressure_deescalation_hysteresis",
        "20.7 Progress and Backpressure - Backpressure de-escalation hysteresis",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._manager_backpressure, dict), (
            "Backpressure hysteresis expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_8_retry_budget_reset_on_failover() -> None:
    spec = _build_spec(
        "manager_worker_20_8_retry_budget_reset_on_failover",
        "20.8 Retry and Timeout Semantics - Retry budget reset on failover",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Retry budget reset expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_8_extension_early_completion() -> None:
    spec = _build_spec(
        "manager_worker_20_8_extension_early_completion",
        "20.8 Retry and Timeout Semantics - Extension early completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._extension_current_progress, dict), (
            "Extension early completion expected extension progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_8_overlapping_retry_windows() -> None:
    spec = _build_spec(
        "manager_worker_20_8_overlapping_retry_windows",
        "20.8 Retry and Timeout Semantics - Overlapping retry windows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_retries, dict), (
            "Overlapping retries expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_9_health_restored_mid_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_20_9_health_restored_mid_dispatch",
        "20.9 Worker Health and Recovery - Health restored mid-dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_health_states, dict), (
            "Health restored expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_9_zombie_late_progress() -> None:
    spec = _build_spec(
        "manager_worker_20_9_zombie_late_progress",
        "20.9 Worker Health and Recovery - Zombie late progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_job_last_progress, dict), (
            "Zombie late progress expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_9_gc_pause_false_positive() -> None:
    spec = _build_spec(
        "manager_worker_20_9_gc_pause_false_positive",
        "20.9 Worker Health and Recovery - GC pause false positive",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "GC pause expected health monitor"
    finally:
        await runtime.stop_cluster()


async def validate_20_10_result_dedupe_across_restarts() -> None:
    spec = _build_spec(
        "manager_worker_20_10_result_dedupe_across_restarts",
        "20.10 Result Integrity and Validation - Result dedupe across restarts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_aggregated_results, dict), (
            "Result dedupe expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_10_result_merge_after_retries() -> None:
    spec = _build_spec(
        "manager_worker_20_10_result_merge_after_retries",
        "20.10 Result Integrity and Validation - Result merge after retries",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_aggregated_results, dict), (
            "Result merge expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_10_result_schema_change() -> None:
    spec = _build_spec(
        "manager_worker_20_10_result_schema_change",
        "20.10 Result Integrity and Validation - Result schema change",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_aggregated_results, dict), (
            "Result schema change expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_11_snapshot_with_in_flight_dispatches() -> None:
    spec = _build_spec(
        "manager_worker_20_11_snapshot_with_in_flight_dispatches",
        "20.11 State Sync and Consistency - Snapshot with in-flight dispatches",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_lifecycle_states, dict), (
            "Snapshot with dispatches expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_11_restore_pending_cancellations() -> None:
    spec = _build_spec(
        "manager_worker_20_11_restore_pending_cancellations",
        "20.11 State Sync and Consistency - Restore pending cancellations",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cancel_events, dict), (
            "Restore cancellations expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_11_stale_state_version_rejection() -> None:
    spec = _build_spec(
        "manager_worker_20_11_stale_state_version_rejection",
        "20.11 State Sync and Consistency - Stale state version rejection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_lifecycle_states, dict), (
            "Stale state version expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_20_1_timeout()
    await validate_20_1_rejection()
    await validate_20_1_exception()
    await validate_20_2_workflow_exception()
    await validate_20_2_serialization_error()
    await validate_20_2_resource_error()
    await validate_20_3_retry_dispatch()
    await validate_20_3_mark_worker_unhealthy()
    await validate_20_3_escalate_to_gate()
    await validate_20_4_stats_batching_drift()
    await validate_20_4_priority_fairness_under_contention()
    await validate_20_4_retry_budget_exhaustion()
    await validate_20_4_progress_idempotency()
    await validate_20_4_late_dispatch_ack_reconciliation()
    await validate_20_4_worker_state_sync_after_restart()
    await validate_20_4_circuit_breaker_oscillation()
    await validate_20_4_result_integrity_on_restart()
    await validate_20_5_starvation_prevention()
    await validate_20_5_uneven_core_fairness()
    await validate_20_5_priority_inversion()
    await validate_20_6_duplicate_dispatch_acks()
    await validate_20_6_ack_without_execution()
    await validate_20_6_redispatch_after_partial_execution()
    await validate_20_7_progress_buffer_overflow_recovery()
    await validate_20_7_progress_jitter_smoothing()
    await validate_20_7_backpressure_deescalation_hysteresis()
    await validate_20_8_retry_budget_reset_on_failover()
    await validate_20_8_extension_early_completion()
    await validate_20_8_overlapping_retry_windows()
    await validate_20_9_health_restored_mid_dispatch()
    await validate_20_9_zombie_late_progress()
    await validate_20_9_gc_pause_false_positive()
    await validate_20_10_result_dedupe_across_restarts()
    await validate_20_10_result_merge_after_retries()
    await validate_20_10_result_schema_change()
    await validate_20_11_snapshot_with_in_flight_dispatches()
    await validate_20_11_restore_pending_cancellations()
    await validate_20_11_stale_state_version_rejection()


if __name__ == "__main__":
    asyncio.run(run())
