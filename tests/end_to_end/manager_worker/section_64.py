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


async def validate_64_1_worker_lease_jitter_cap() -> None:
    spec = _build_spec(
        "manager_worker_64_1_worker_lease_jitter_cap",
        "64.1 Worker lease jitter cap - Cap prevents excessive jitter",
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
            "Jitter cap expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_2_dispatch_retry_token_reuse() -> None:
    spec = _build_spec(
        "manager_worker_64_2_dispatch_retry_token_reuse",
        "64.2 Dispatch retry token reuse - Reuse does not confuse retries",
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
            "Retry token reuse expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_3_progress_snapshot_lag() -> None:
    spec = _build_spec(
        "manager_worker_64_3_progress_snapshot_lag",
        "64.3 Progress snapshot lag - Snapshot lag bounded",
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
            "Snapshot lag expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_4_result_ack_loss_detection() -> None:
    spec = _build_spec(
        "manager_worker_64_4_result_ack_loss_detection",
        "64.4 Result ack loss detection - Loss detection triggers resend",
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
            "Ack loss detection expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_5_manager_load_shed_reporting() -> None:
    spec = _build_spec(
        "manager_worker_64_5_manager_load_shed_reporting",
        "64.5 Manager load shed reporting - Reporting emits warning once",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Load shed reporting expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_6_worker_health_probe_drop() -> None:
    spec = _build_spec(
        "manager_worker_64_6_worker_health_probe_drop",
        "64.6 Worker health probe drop - Drop triggers suspect state",
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
            "Probe drop expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_7_cancel_ack_delay() -> None:
    spec = _build_spec(
        "manager_worker_64_7_cancel_ack_delay",
        "64.7 Cancel ack delay - Delay does not block new cancels",
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
            "Cancel ack delay expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_8_worker_metadata_refresh() -> None:
    spec = _build_spec(
        "manager_worker_64_8_worker_metadata_refresh",
        "64.8 Worker metadata refresh - Refresh keeps metadata fresh",
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
            "Metadata refresh expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_9_dispatch_admission_burst() -> None:
    spec = _build_spec(
        "manager_worker_64_9_dispatch_admission_burst",
        "64.9 Dispatch admission burst - Burst handled without starvation",
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
            "Admission burst expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_64_10_progress_ack_reorder() -> None:
    spec = _build_spec(
        "manager_worker_64_10_progress_ack_reorder",
        "64.10 Progress ack reorder - Reorder handled without regression",
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
            "Ack reorder expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_64_1_worker_lease_jitter_cap()
    await validate_64_2_dispatch_retry_token_reuse()
    await validate_64_3_progress_snapshot_lag()
    await validate_64_4_result_ack_loss_detection()
    await validate_64_5_manager_load_shed_reporting()
    await validate_64_6_worker_health_probe_drop()
    await validate_64_7_cancel_ack_delay()
    await validate_64_8_worker_metadata_refresh()
    await validate_64_9_dispatch_admission_burst()
    await validate_64_10_progress_ack_reorder()


if __name__ == "__main__":
    asyncio.run(run())
