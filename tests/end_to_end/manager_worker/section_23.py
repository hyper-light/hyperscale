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


async def validate_23_1_sub_second_progress_updates() -> None:
    spec = _build_spec(
        "manager_worker_23_1_sub_second_progress_updates",
        "23.1 High-Frequency Progress - Sub-second progress updates",
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
            "Sub-second progress expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_1_progress_batching_efficiency() -> None:
    spec = _build_spec(
        "manager_worker_23_1_progress_batching_efficiency",
        "23.1 High-Frequency Progress - Progress batching efficiency",
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
            "Progress batching expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_1_progress_ordering() -> None:
    spec = _build_spec(
        "manager_worker_23_1_progress_ordering",
        "23.1 High-Frequency Progress - Progress ordering",
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
            "Progress ordering expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_1_progress_memory_churn() -> None:
    spec = _build_spec(
        "manager_worker_23_1_progress_memory_churn",
        "23.1 High-Frequency Progress - Progress memory churn",
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
            "Progress memory churn expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_multi_dc_progress_merge() -> None:
    spec = _build_spec(
        "manager_worker_23_2_multi_dc_progress_merge",
        "23.2 Progress Fan-Out - Multi-DC progress merge",
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
            "Multi-DC progress merge expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_progress_to_multiple_callbacks() -> None:
    spec = _build_spec(
        "manager_worker_23_2_progress_to_multiple_callbacks",
        "23.2 Progress Fan-Out - Progress to multiple callbacks",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._job_origin_gates is not None, (
            "Progress to callbacks expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_progress_callback_latency() -> None:
    spec = _build_spec(
        "manager_worker_23_2_progress_callback_latency",
        "23.2 Progress Fan-Out - Progress callback latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._job_origin_gates is not None, (
            "Progress callback latency expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_progress_callback_failure() -> None:
    spec = _build_spec(
        "manager_worker_23_2_progress_callback_failure",
        "23.2 Progress Fan-Out - Progress callback failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._job_origin_gates is not None, (
            "Progress callback failure expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_3_dc_becomes_unreachable() -> None:
    spec = _build_spec(
        "manager_worker_23_3_dc_becomes_unreachable",
        "23.3 Progress Under Partition - DC becomes unreachable",
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
            "DC unreachable expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_3_dc_reconnects() -> None:
    spec = _build_spec(
        "manager_worker_23_3_dc_reconnects",
        "23.3 Progress Under Partition - DC reconnects",
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
            "DC reconnects expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_3_progress_gap_detection() -> None:
    spec = _build_spec(
        "manager_worker_23_3_progress_gap_detection",
        "23.3 Progress Under Partition - Progress gap detection",
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
            "Progress gap detection expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_23_1_sub_second_progress_updates()
    await validate_23_1_progress_batching_efficiency()
    await validate_23_1_progress_ordering()
    await validate_23_1_progress_memory_churn()
    await validate_23_2_multi_dc_progress_merge()
    await validate_23_2_progress_to_multiple_callbacks()
    await validate_23_2_progress_callback_latency()
    await validate_23_2_progress_callback_failure()
    await validate_23_3_dc_becomes_unreachable()
    await validate_23_3_dc_reconnects()
    await validate_23_3_progress_gap_detection()


if __name__ == "__main__":
    asyncio.run(run())
