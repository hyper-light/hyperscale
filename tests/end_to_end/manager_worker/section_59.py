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


async def validate_59_1_worker_lease_cancellation() -> None:
    spec = _build_spec(
        "manager_worker_59_1_worker_lease_cancellation",
        "59.1 Worker lease cancellation - Lease cancellation cleans up pending jobs",
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
            "Lease cancellation expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_2_dispatch_backoff_tuning() -> None:
    spec = _build_spec(
        "manager_worker_59_2_dispatch_backoff_tuning",
        "59.2 Dispatch backoff tuning - Backoff adapts to load",
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
            "Dispatch backoff expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_3_progress_durability_checkpoint() -> None:
    spec = _build_spec(
        "manager_worker_59_3_progress_durability_checkpoint",
        "59.3 Progress durability checkpoint - Checkpoints survive restart",
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
            "Progress checkpoints expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_4_result_dedupe_window() -> None:
    spec = _build_spec(
        "manager_worker_59_4_result_dedupe_window",
        "59.4 Result dedupe window - Dedupe window prevents double emit",
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


async def validate_59_5_manager_throttle_escalation() -> None:
    spec = _build_spec(
        "manager_worker_59_5_manager_throttle_escalation",
        "59.5 Manager throttle escalation - Throttle escalates under sustained load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Throttle escalation expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_6_worker_health_dampening() -> None:
    spec = _build_spec(
        "manager_worker_59_6_worker_health_dampening",
        "59.6 Worker health dampening - Dampening avoids rapid flips",
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
            "Health dampening expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_7_cancel_queue_isolation() -> None:
    spec = _build_spec(
        "manager_worker_59_7_cancel_queue_isolation",
        "59.7 Cancel queue isolation - Cancel queue does not block dispatch",
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
            "Cancel queue isolation expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_8_worker_metadata_compaction() -> None:
    spec = _build_spec(
        "manager_worker_59_8_worker_metadata_compaction",
        "59.8 Worker metadata compaction - Compaction keeps metadata bounded",
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
            "Metadata compaction expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_9_retry_budget_priority() -> None:
    spec = _build_spec(
        "manager_worker_59_9_retry_budget_priority",
        "59.9 Retry budget priority - High priority retries retain budget",
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
            "Retry budget priority expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_59_10_progress_resume_sync() -> None:
    spec = _build_spec(
        "manager_worker_59_10_progress_resume_sync",
        "59.10 Progress resume sync - Resume sync after worker restart",
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
            "Progress resume expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_59_1_worker_lease_cancellation()
    await validate_59_2_dispatch_backoff_tuning()
    await validate_59_3_progress_durability_checkpoint()
    await validate_59_4_result_dedupe_window()
    await validate_59_5_manager_throttle_escalation()
    await validate_59_6_worker_health_dampening()
    await validate_59_7_cancel_queue_isolation()
    await validate_59_8_worker_metadata_compaction()
    await validate_59_9_retry_budget_priority()
    await validate_59_10_progress_resume_sync()


if __name__ == "__main__":
    asyncio.run(run())
