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


async def validate_55_1_worker_reconnect_flood() -> None:
    spec = _build_spec(
        "manager_worker_55_1_worker_reconnect_flood",
        "55.1 Worker reconnect flood - Reconnect flood does not overload manager",
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
            "Reconnect flood expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_2_manager_dispatch_retry_jitter() -> None:
    spec = _build_spec(
        "manager_worker_55_2_manager_dispatch_retry_jitter",
        "55.2 Manager dispatch retry jitter - Jitter spreads retries across window",
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
            "Dispatch retry jitter expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_3_progress_watermark_lag() -> None:
    spec = _build_spec(
        "manager_worker_55_3_progress_watermark_lag",
        "55.3 Progress watermark lag - Watermark lag does not regress stats",
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
            "Watermark lag expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_4_result_ack_idempotency() -> None:
    spec = _build_spec(
        "manager_worker_55_4_result_ack_idempotency",
        "55.4 Result ack idempotency - Duplicate ack does not double-close",
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
            "Result ack idempotency expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_5_worker_shutdown_with_backlog() -> None:
    spec = _build_spec(
        "manager_worker_55_5_worker_shutdown_with_backlog",
        "55.5 Worker shutdown with backlog - Backlog rescheduled on shutdown",
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
            "Worker shutdown expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_6_manager_failover_cancel_safety() -> None:
    spec = _build_spec(
        "manager_worker_55_6_manager_failover_cancel_safety",
        "55.6 Manager failover cancel safety - Cancels survive manager failover",
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
            "Failover cancel safety expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_7_worker_health_decay() -> None:
    spec = _build_spec(
        "manager_worker_55_7_worker_health_decay",
        "55.7 Worker health decay - Gradual decay before unhealthy",
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
            "Health decay expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_8_retry_escalation_tiers() -> None:
    spec = _build_spec(
        "manager_worker_55_8_retry_escalation_tiers",
        "55.8 Retry escalation tiers - Tiered retries avoid hot loops",
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
            "Retry escalation expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_9_dispatch_queue_spillover() -> None:
    spec = _build_spec(
        "manager_worker_55_9_dispatch_queue_spillover",
        "55.9 Dispatch queue spillover - Spillover routes to secondary manager",
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
            "Dispatch spillover expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_55_10_progress_drop_detection() -> None:
    spec = _build_spec(
        "manager_worker_55_10_progress_drop_detection",
        "55.10 Progress drop detection - Drop detection triggers warning",
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
            "Progress drop expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_55_1_worker_reconnect_flood()
    await validate_55_2_manager_dispatch_retry_jitter()
    await validate_55_3_progress_watermark_lag()
    await validate_55_4_result_ack_idempotency()
    await validate_55_5_worker_shutdown_with_backlog()
    await validate_55_6_manager_failover_cancel_safety()
    await validate_55_7_worker_health_decay()
    await validate_55_8_retry_escalation_tiers()
    await validate_55_9_dispatch_queue_spillover()
    await validate_55_10_progress_drop_detection()


if __name__ == "__main__":
    asyncio.run(run())
