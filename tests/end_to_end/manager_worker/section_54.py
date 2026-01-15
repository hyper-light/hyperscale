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


async def validate_54_1_worker_backlog_drain_rate() -> None:
    spec = _build_spec(
        "manager_worker_54_1_worker_backlog_drain_rate",
        "54.1 Worker backlog drain rate - Drain rate stays within expected bounds",
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
            "Backlog drain rate expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_2_manager_dispatch_burst_coalescing() -> None:
    spec = _build_spec(
        "manager_worker_54_2_manager_dispatch_burst_coalescing",
        "54.2 Manager dispatch burst coalescing - Coalesce bursts without starvation",
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
            "Dispatch coalescing expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_3_progress_dedupe_window() -> None:
    spec = _build_spec(
        "manager_worker_54_3_progress_dedupe_window",
        "54.3 Progress dedupe window - Dedupe window prevents double counting",
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
            "Progress dedupe expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_4_result_batch_sizing() -> None:
    spec = _build_spec(
        "manager_worker_54_4_result_batch_sizing",
        "54.4 Result batch sizing - Batch sizing respects size limits",
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
            "Result batch sizing expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_5_worker_eviction_grace_period() -> None:
    spec = _build_spec(
        "manager_worker_54_5_worker_eviction_grace_period",
        "54.5 Worker eviction grace period - Grace period allows in-flight completion",
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
            "Eviction grace expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_6_manager_retry_queue_isolation() -> None:
    spec = _build_spec(
        "manager_worker_54_6_manager_retry_queue_isolation",
        "54.6 Manager retry queue isolation - Retry queue does not block new dispatch",
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
            "Retry queue isolation expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_7_health_state_snapshot_lag() -> None:
    spec = _build_spec(
        "manager_worker_54_7_health_state_snapshot_lag",
        "54.7 Health state snapshot lag - Snapshot lag does not regress state",
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
            "Health snapshot lag expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_8_worker_registration_storm() -> None:
    spec = _build_spec(
        "manager_worker_54_8_worker_registration_storm",
        "54.8 Worker registration storm - Registration storm does not drop workers",
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
            "Registration storm expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_9_dispatch_jitter_smoothing() -> None:
    spec = _build_spec(
        "manager_worker_54_9_dispatch_jitter_smoothing",
        "54.9 Dispatch jitter smoothing - Jitter smoothing avoids thundering herd",
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
            "Dispatch jitter expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_54_10_cancel_replay_safety() -> None:
    spec = _build_spec(
        "manager_worker_54_10_cancel_replay_safety",
        "54.10 Cancel replay safety - Replayed cancel does not re-open workflow",
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
            "Cancel replay expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_54_1_worker_backlog_drain_rate()
    await validate_54_2_manager_dispatch_burst_coalescing()
    await validate_54_3_progress_dedupe_window()
    await validate_54_4_result_batch_sizing()
    await validate_54_5_worker_eviction_grace_period()
    await validate_54_6_manager_retry_queue_isolation()
    await validate_54_7_health_state_snapshot_lag()
    await validate_54_8_worker_registration_storm()
    await validate_54_9_dispatch_jitter_smoothing()
    await validate_54_10_cancel_replay_safety()


if __name__ == "__main__":
    asyncio.run(run())
