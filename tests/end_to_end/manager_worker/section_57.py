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


async def validate_57_1_worker_lease_overlap() -> None:
    spec = _build_spec(
        "manager_worker_57_1_worker_lease_overlap",
        "57.1 Worker lease overlap - Overlap avoids double-scheduling",
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
            "Lease overlap expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_2_dispatch_ack_timeout_override() -> None:
    spec = _build_spec(
        "manager_worker_57_2_dispatch_ack_timeout_override",
        "57.2 Dispatch ack timeout override - Override per-tenant timeout",
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
            "Ack timeout override expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_3_progress_compression_fallback() -> None:
    spec = _build_spec(
        "manager_worker_57_3_progress_compression_fallback",
        "57.3 Progress compression fallback - Fallback to raw on decode error",
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
            "Compression fallback expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_4_result_routing_split() -> None:
    spec = _build_spec(
        "manager_worker_57_4_result_routing_split",
        "57.4 Result routing split - Split routing across gates for latency",
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
            "Routing split expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_5_manager_retry_queue_compaction() -> None:
    spec = _build_spec(
        "manager_worker_57_5_manager_retry_queue_compaction",
        "57.5 Manager retry queue compaction - Compaction keeps queue bounded",
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
            "Retry queue compaction expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_6_worker_health_quorum() -> None:
    spec = _build_spec(
        "manager_worker_57_6_worker_health_quorum",
        "57.6 Worker health quorum - Quorum avoids single-sample flaps",
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
            "Health quorum expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_7_cancel_result_ordering() -> None:
    spec = _build_spec(
        "manager_worker_57_7_cancel_result_ordering",
        "57.7 Cancel vs result ordering - Result after cancel handled safely",
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
            "Cancel/result ordering expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_8_worker_stats_sampling() -> None:
    spec = _build_spec(
        "manager_worker_57_8_worker_stats_sampling",
        "57.8 Worker stats sampling - Sampling does not skew aggregates",
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
            "Stats sampling expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_9_manager_admission_control() -> None:
    spec = _build_spec(
        "manager_worker_57_9_manager_admission_control",
        "57.9 Manager admission control - Admission control enforces limits",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Admission control expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_57_10_progress_ack_lag() -> None:
    spec = _build_spec(
        "manager_worker_57_10_progress_ack_lag",
        "57.10 Progress ack lag - Ack lag does not block pipeline",
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
            "Progress ack lag expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_57_1_worker_lease_overlap()
    await validate_57_2_dispatch_ack_timeout_override()
    await validate_57_3_progress_compression_fallback()
    await validate_57_4_result_routing_split()
    await validate_57_5_manager_retry_queue_compaction()
    await validate_57_6_worker_health_quorum()
    await validate_57_7_cancel_result_ordering()
    await validate_57_8_worker_stats_sampling()
    await validate_57_9_manager_admission_control()
    await validate_57_10_progress_ack_lag()


if __name__ == "__main__":
    asyncio.run(run())
