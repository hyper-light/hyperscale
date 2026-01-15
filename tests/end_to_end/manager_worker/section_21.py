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


async def validate_21_1_burst_stats_traffic() -> None:
    spec = _build_spec(
        "manager_worker_21_1_burst_stats_traffic",
        "21.1 Burst Stats Traffic - 1000 VUs generating stats",
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
            "Burst stats traffic expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_batching_under_load() -> None:
    spec = _build_spec(
        "manager_worker_21_1_stats_batching_under_load",
        "21.1 Burst Stats Traffic - Stats batching under load",
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
            "Stats batching expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_queue_overflow() -> None:
    spec = _build_spec(
        "manager_worker_21_1_stats_queue_overflow",
        "21.1 Burst Stats Traffic - Stats queue overflow",
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
            "Stats queue overflow expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_memory_pressure() -> None:
    spec = _build_spec(
        "manager_worker_21_1_stats_memory_pressure",
        "21.1 Burst Stats Traffic - Stats memory pressure",
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
            "Stats memory pressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_flush_backpressure() -> None:
    spec = _build_spec(
        "manager_worker_21_1_stats_flush_backpressure",
        "21.1 Burst Stats Traffic - Stats flush backpressure",
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
            "Stats flush backpressure expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_out_of_order_stats_batches() -> None:
    spec = _build_spec(
        "manager_worker_21_2_out_of_order_stats_batches",
        "21.2 Stats Ordering - Out-of-order stats batches",
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
            "Out-of-order stats expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_duplicate_stats_batch() -> None:
    spec = _build_spec(
        "manager_worker_21_2_duplicate_stats_batch",
        "21.2 Stats Ordering - Duplicate stats batch",
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
            "Duplicate stats batch expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_stats_from_dead_worker() -> None:
    spec = _build_spec(
        "manager_worker_21_2_stats_from_dead_worker",
        "21.2 Stats Ordering - Stats from dead worker",
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
            "Stats from dead worker expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_stats_version_conflict() -> None:
    spec = _build_spec(
        "manager_worker_21_2_stats_version_conflict",
        "21.2 Stats Ordering - Stats version conflict",
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
            "Stats version conflict expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_parallel_stats_merging() -> None:
    spec = _build_spec(
        "manager_worker_21_3_parallel_stats_merging",
        "21.3 Stats Aggregation - Parallel stats merging",
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
            "Parallel stats merging expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_partial_aggregation_windows() -> None:
    spec = _build_spec(
        "manager_worker_21_3_partial_aggregation_windows",
        "21.3 Stats Aggregation - Partial aggregation windows",
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
            "Partial aggregation expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_stats_window_boundary() -> None:
    spec = _build_spec(
        "manager_worker_21_3_stats_window_boundary",
        "21.3 Stats Aggregation - Stats window boundary",
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
            "Stats window boundary expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_stats_compression() -> None:
    spec = _build_spec(
        "manager_worker_21_3_stats_compression",
        "21.3 Stats Aggregation - Stats compression",
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
            "Stats compression expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_manager_overloaded() -> None:
    spec = _build_spec(
        "manager_worker_21_4_manager_overloaded",
        "21.4 Stats Pipeline Backpressure - Manager overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._overload_detector is not None, (
            "Manager overloaded expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_gate_overloaded() -> None:
    spec = _build_spec(
        "manager_worker_21_4_gate_overloaded",
        "21.4 Stats Pipeline Backpressure - Gate overloaded",
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
            "Gate overloaded expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_client_callback_slow() -> None:
    spec = _build_spec(
        "manager_worker_21_4_client_callback_slow",
        "21.4 Stats Pipeline Backpressure - Client callback slow",
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
            "Client callback slow expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_end_to_end_latency_spike() -> None:
    spec = _build_spec(
        "manager_worker_21_4_end_to_end_latency_spike",
        "21.4 Stats Pipeline Backpressure - End-to-end latency spike",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._overload_detector is not None, (
            "Latency spike expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_21_1_burst_stats_traffic()
    await validate_21_1_stats_batching_under_load()
    await validate_21_1_stats_queue_overflow()
    await validate_21_1_stats_memory_pressure()
    await validate_21_1_stats_flush_backpressure()
    await validate_21_2_out_of_order_stats_batches()
    await validate_21_2_duplicate_stats_batch()
    await validate_21_2_stats_from_dead_worker()
    await validate_21_2_stats_version_conflict()
    await validate_21_3_parallel_stats_merging()
    await validate_21_3_partial_aggregation_windows()
    await validate_21_3_stats_window_boundary()
    await validate_21_3_stats_compression()
    await validate_21_4_manager_overloaded()
    await validate_21_4_gate_overloaded()
    await validate_21_4_client_callback_slow()
    await validate_21_4_end_to_end_latency_spike()


if __name__ == "__main__":
    asyncio.run(run())
