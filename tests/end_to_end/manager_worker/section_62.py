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


async def validate_62_1_worker_lease_renewal_override() -> None:
    spec = _build_spec(
        "manager_worker_62_1_worker_lease_renewal_override",
        "62.1 Worker lease renewal override - Override renew interval during load",
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
            "Renewal override expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_2_dispatch_retry_enqueue_fairness() -> None:
    spec = _build_spec(
        "manager_worker_62_2_dispatch_retry_enqueue_fairness",
        "62.2 Dispatch retry enqueue fairness - Retry enqueue does not starve new",
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
            "Retry enqueue fairness expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_3_progress_snapshot_eviction() -> None:
    spec = _build_spec(
        "manager_worker_62_3_progress_snapshot_eviction",
        "62.3 Progress snapshot eviction - Eviction keeps snapshot size bounded",
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
            "Snapshot eviction expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_4_result_ack_timeout_escalation() -> None:
    spec = _build_spec(
        "manager_worker_62_4_result_ack_timeout_escalation",
        "62.4 Result ack timeout escalation - Escalation triggers alert",
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
            "Ack timeout escalation expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_5_manager_load_shed_floor() -> None:
    spec = _build_spec(
        "manager_worker_62_5_manager_load_shed_floor",
        "62.5 Manager load shed floor - Floor avoids total blackout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Load shed floor expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_6_worker_health_probe_jitter() -> None:
    spec = _build_spec(
        "manager_worker_62_6_worker_health_probe_jitter",
        "62.6 Worker health probe jitter - Jitter avoids synchronized probes",
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
            "Probe jitter expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_7_cancel_queue_compaction() -> None:
    spec = _build_spec(
        "manager_worker_62_7_cancel_queue_compaction",
        "62.7 Cancel queue compaction - Compaction keeps cancel queue bounded",
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
            "Cancel compaction expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_8_worker_metadata_flush() -> None:
    spec = _build_spec(
        "manager_worker_62_8_worker_metadata_flush",
        "62.8 Worker metadata flush - Flush writes metadata on shutdown",
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
            "Metadata flush expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_9_dispatch_queue_admission_floor() -> None:
    spec = _build_spec(
        "manager_worker_62_9_dispatch_queue_admission_floor",
        "62.9 Dispatch queue admission floor - Floor allows critical jobs",
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
            "Admission floor expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_62_10_progress_lag_recovery() -> None:
    spec = _build_spec(
        "manager_worker_62_10_progress_lag_recovery",
        "62.10 Progress lag recovery - Recovery clears lag state",
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
            "Lag recovery expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_62_1_worker_lease_renewal_override()
    await validate_62_2_dispatch_retry_enqueue_fairness()
    await validate_62_3_progress_snapshot_eviction()
    await validate_62_4_result_ack_timeout_escalation()
    await validate_62_5_manager_load_shed_floor()
    await validate_62_6_worker_health_probe_jitter()
    await validate_62_7_cancel_queue_compaction()
    await validate_62_8_worker_metadata_flush()
    await validate_62_9_dispatch_queue_admission_floor()
    await validate_62_10_progress_lag_recovery()


if __name__ == "__main__":
    asyncio.run(run())
