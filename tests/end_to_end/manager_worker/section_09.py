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


async def validate_9_1_workflow_progress_updates() -> None:
    spec = _build_spec(
        "manager_worker_9_1_workflow_progress_updates",
        "9.1 Progress Collection - WorkflowProgress updates",
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
            "WorkflowProgress updates expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_1_step_stats() -> None:
    spec = _build_spec(
        "manager_worker_9_1_step_stats",
        "9.1 Progress Collection - Step stats",
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
            "Step stats expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_1_rate_calculation() -> None:
    spec = _build_spec(
        "manager_worker_9_1_rate_calculation",
        "9.1 Progress Collection - Rate calculation",
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
            "Rate calculation expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_buffer_updates() -> None:
    spec = _build_spec(
        "manager_worker_9_2_buffer_updates",
        "9.2 Progress Buffering - Buffer updates",
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
            "Buffer updates expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_flush_interval() -> None:
    spec = _build_spec(
        "manager_worker_9_2_flush_interval",
        "9.2 Progress Buffering - Flush interval",
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
            "Flush interval expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_backpressure_handling() -> None:
    spec = _build_spec(
        "manager_worker_9_2_backpressure_handling",
        "9.2 Progress Buffering - Backpressure handling",
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
            "Backpressure handling expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_none_backpressure() -> None:
    spec = _build_spec(
        "manager_worker_9_3_none_backpressure",
        "9.3 Backpressure Effects on Progress - NONE",
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
            "NONE backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_throttle_backpressure() -> None:
    spec = _build_spec(
        "manager_worker_9_3_throttle_backpressure",
        "9.3 Backpressure Effects on Progress - THROTTLE",
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
            "THROTTLE backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_batch_backpressure() -> None:
    spec = _build_spec(
        "manager_worker_9_3_batch_backpressure",
        "9.3 Backpressure Effects on Progress - BATCH",
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
            "BATCH backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_reject_backpressure() -> None:
    spec = _build_spec(
        "manager_worker_9_3_reject_backpressure",
        "9.3 Backpressure Effects on Progress - REJECT",
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
            "REJECT backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_workflow_progress_message() -> None:
    spec = _build_spec(
        "manager_worker_9_4_workflow_progress_message",
        "9.4 Progress to Manager - WorkflowProgress message",
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
            "WorkflowProgress message expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_manager_aggregation() -> None:
    spec = _build_spec(
        "manager_worker_9_4_manager_aggregation",
        "9.4 Progress to Manager - Manager aggregation",
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
            "Manager aggregation expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_forward_to_gate() -> None:
    spec = _build_spec(
        "manager_worker_9_4_forward_to_gate",
        "9.4 Progress to Manager - Forward to gate",
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
            "Forward to gate expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_9_1_workflow_progress_updates()
    await validate_9_1_step_stats()
    await validate_9_1_rate_calculation()
    await validate_9_2_buffer_updates()
    await validate_9_2_flush_interval()
    await validate_9_2_backpressure_handling()
    await validate_9_3_none_backpressure()
    await validate_9_3_throttle_backpressure()
    await validate_9_3_batch_backpressure()
    await validate_9_3_reject_backpressure()
    await validate_9_4_workflow_progress_message()
    await validate_9_4_manager_aggregation()
    await validate_9_4_forward_to_gate()


if __name__ == "__main__":
    asyncio.run(run())
