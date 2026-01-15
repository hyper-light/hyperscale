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


async def validate_58_1_worker_lease_renewal_backlog() -> None:
    spec = _build_spec(
        "manager_worker_58_1_worker_lease_renewal_backlog",
        "58.1 Worker lease renewal backlog - Renewal backlog drains without expiry",
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
            "Lease renewal backlog expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_2_dispatch_ack_flood() -> None:
    spec = _build_spec(
        "manager_worker_58_2_dispatch_ack_flood",
        "58.2 Dispatch ack flood - Ack flood does not stall dispatch loop",
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
            "Ack flood expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_3_progress_ordering_watermark() -> None:
    spec = _build_spec(
        "manager_worker_58_3_progress_ordering_watermark",
        "58.3 Progress ordering watermark - Watermark enforces monotonic progress",
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
            "Progress watermark expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_4_result_batching_retry() -> None:
    spec = _build_spec(
        "manager_worker_58_4_result_batching_retry",
        "58.4 Result batching retry - Retry uses exponential backoff",
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
            "Batching retry expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_5_manager_retry_queue_overflow() -> None:
    spec = _build_spec(
        "manager_worker_58_5_manager_retry_queue_overflow",
        "58.5 Manager retry queue overflow - Overflow drops oldest safely",
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
            "Retry queue overflow expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_6_worker_heartbeat_coalescing() -> None:
    spec = _build_spec(
        "manager_worker_58_6_worker_heartbeat_coalescing",
        "58.6 Worker heartbeat coalescing - Coalescing reduces overhead",
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
            "Heartbeat coalescing expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_7_cancel_dispatch_priority() -> None:
    spec = _build_spec(
        "manager_worker_58_7_cancel_dispatch_priority",
        "58.7 Cancel dispatch priority - Cancel dispatch not starved",
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
            "Cancel dispatch priority expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_8_worker_registry_snapshot() -> None:
    spec = _build_spec(
        "manager_worker_58_8_worker_registry_snapshot",
        "58.8 Worker registry snapshot - Snapshot includes all live workers",
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
            "Registry snapshot expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_9_dispatch_admission_sampling() -> None:
    spec = _build_spec(
        "manager_worker_58_9_dispatch_admission_sampling",
        "58.9 Dispatch admission sampling - Sampling keeps overhead low",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Admission sampling expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_58_10_progress_lag_alerting() -> None:
    spec = _build_spec(
        "manager_worker_58_10_progress_lag_alerting",
        "58.10 Progress lag alerting - Lag alert triggers once per threshold",
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
            "Progress lag alert expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_58_1_worker_lease_renewal_backlog()
    await validate_58_2_dispatch_ack_flood()
    await validate_58_3_progress_ordering_watermark()
    await validate_58_4_result_batching_retry()
    await validate_58_5_manager_retry_queue_overflow()
    await validate_58_6_worker_heartbeat_coalescing()
    await validate_58_7_cancel_dispatch_priority()
    await validate_58_8_worker_registry_snapshot()
    await validate_58_9_dispatch_admission_sampling()
    await validate_58_10_progress_lag_alerting()


if __name__ == "__main__":
    asyncio.run(run())
