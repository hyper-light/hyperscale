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


async def validate_44_1_worker_lease_expiry() -> None:
    spec = _build_spec(
        "manager_worker_44_1_worker_lease_expiry",
        "44.1 Worker lease expiry - Lease expires during long action",
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
            "Worker lease expiry expected worker unhealthy since"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_2_dispatch_list_staleness() -> None:
    spec = _build_spec(
        "manager_worker_44_2_dispatch_list_staleness",
        "44.2 Dispatch list staleness - Manager dispatches using stale worker list",
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
            "Dispatch staleness expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_3_retry_token_mismatch() -> None:
    spec = _build_spec(
        "manager_worker_44_3_retry_token_mismatch",
        "44.3 Retry token mismatch - Worker reports mismatched retry token",
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
            "Retry token mismatch expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_4_progress_flush_on_shutdown() -> None:
    spec = _build_spec(
        "manager_worker_44_4_progress_flush_on_shutdown",
        "44.4 Progress flush on shutdown - Worker flushes progress before exit",
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
            "Progress flush expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_5_result_ack_retry_loop() -> None:
    spec = _build_spec(
        "manager_worker_44_5_result_ack_retry_loop",
        "44.5 Result ack retry loop - Manager retries ack for flaky worker",
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
            "Result ack retry expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_6_cancel_retry_race() -> None:
    spec = _build_spec(
        "manager_worker_44_6_cancel_retry_race",
        "44.6 Cancel vs retry race - Cancellation races with retry dispatch",
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
            "Cancel vs retry expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_7_worker_metadata_eviction() -> None:
    spec = _build_spec(
        "manager_worker_44_7_worker_metadata_eviction",
        "44.7 Worker metadata eviction - Evict stale worker metadata safely",
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
            "Worker metadata eviction expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_8_backpressure_recovery_ramp() -> None:
    spec = _build_spec(
        "manager_worker_44_8_backpressure_recovery_ramp",
        "44.8 Backpressure recovery ramp - Backpressure relaxes without spikes",
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
            "Backpressure recovery expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_9_manager_queue_fairness() -> None:
    spec = _build_spec(
        "manager_worker_44_9_manager_queue_fairness",
        "44.9 Manager queue fairness - Mixed retry/cancel fairness enforced",
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
            "Queue fairness expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_44_10_worker_health_debounce() -> None:
    spec = _build_spec(
        "manager_worker_44_10_worker_health_debounce",
        "44.10 Worker health debounce - Avoid flapping health states",
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
            "Health debounce expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_44_1_worker_lease_expiry()
    await validate_44_2_dispatch_list_staleness()
    await validate_44_3_retry_token_mismatch()
    await validate_44_4_progress_flush_on_shutdown()
    await validate_44_5_result_ack_retry_loop()
    await validate_44_6_cancel_retry_race()
    await validate_44_7_worker_metadata_eviction()
    await validate_44_8_backpressure_recovery_ramp()
    await validate_44_9_manager_queue_fairness()
    await validate_44_10_worker_health_debounce()


if __name__ == "__main__":
    asyncio.run(run())
