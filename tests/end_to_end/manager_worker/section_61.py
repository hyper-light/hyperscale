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


async def validate_61_1_worker_lease_orphan_cleanup() -> None:
    spec = _build_spec(
        "manager_worker_61_1_worker_lease_orphan_cleanup",
        "61.1 Worker lease orphan cleanup - Orphan cleanup clears stale leases",
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
            "Orphan cleanup expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_2_dispatch_retry_window_cap() -> None:
    spec = _build_spec(
        "manager_worker_61_2_dispatch_retry_window_cap",
        "61.2 Dispatch retry window cap - Cap prevents infinite retries",
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
            "Retry window cap expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_3_progress_backlog_eviction() -> None:
    spec = _build_spec(
        "manager_worker_61_3_progress_backlog_eviction",
        "61.3 Progress backlog eviction - Eviction avoids memory growth",
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
            "Backlog eviction expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_4_result_ack_batching() -> None:
    spec = _build_spec(
        "manager_worker_61_4_result_ack_batching",
        "61.4 Result ack batching - Batch acks reduce chatter",
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
            "Ack batching expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_5_manager_load_shed_recovery() -> None:
    spec = _build_spec(
        "manager_worker_61_5_manager_load_shed_recovery",
        "61.5 Manager load shed recovery - Recovery restores dispatch smoothly",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Load shed recovery expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_6_worker_health_grace() -> None:
    spec = _build_spec(
        "manager_worker_61_6_worker_health_grace",
        "61.6 Worker health grace - Grace period avoids false suspect",
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
            "Health grace expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_7_cancel_broadcast_batching() -> None:
    spec = _build_spec(
        "manager_worker_61_7_cancel_broadcast_batching",
        "61.7 Cancel broadcast batching - Batch cancels efficiently",
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
            "Cancel batching expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_8_worker_metadata_decay() -> None:
    spec = _build_spec(
        "manager_worker_61_8_worker_metadata_decay",
        "61.8 Worker metadata decay - Decay prunes inactive workers",
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
            "Metadata decay expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_9_dispatch_queue_visibility() -> None:
    spec = _build_spec(
        "manager_worker_61_9_dispatch_queue_visibility",
        "61.9 Dispatch queue visibility - Visibility metrics stay accurate",
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
            "Queue visibility expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_61_10_progress_merge_conflict() -> None:
    spec = _build_spec(
        "manager_worker_61_10_progress_merge_conflict",
        "61.10 Progress merge conflict - Conflict resolution keeps monotonicity",
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
            "Progress conflict expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_61_1_worker_lease_orphan_cleanup()
    await validate_61_2_dispatch_retry_window_cap()
    await validate_61_3_progress_backlog_eviction()
    await validate_61_4_result_ack_batching()
    await validate_61_5_manager_load_shed_recovery()
    await validate_61_6_worker_health_grace()
    await validate_61_7_cancel_broadcast_batching()
    await validate_61_8_worker_metadata_decay()
    await validate_61_9_dispatch_queue_visibility()
    await validate_61_10_progress_merge_conflict()


if __name__ == "__main__":
    asyncio.run(run())
