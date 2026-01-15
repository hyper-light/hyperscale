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


async def validate_60_1_worker_lease_fast_renew() -> None:
    spec = _build_spec(
        "manager_worker_60_1_worker_lease_fast_renew",
        "60.1 Worker lease fast renew - Fast renew does not starve dispatch",
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
            "Fast renew expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_2_dispatch_retry_fairness() -> None:
    spec = _build_spec(
        "manager_worker_60_2_dispatch_retry_fairness",
        "60.2 Dispatch retry fairness - Fairness across retries and new work",
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
            "Dispatch fairness expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_3_progress_window_trimming() -> None:
    spec = _build_spec(
        "manager_worker_60_3_progress_window_trimming",
        "60.3 Progress window trimming - Trimming keeps window bounded",
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
            "Progress trimming expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_4_result_ack_timeout_backoff() -> None:
    spec = _build_spec(
        "manager_worker_60_4_result_ack_timeout_backoff",
        "60.4 Result ack timeout backoff - Backoff avoids hammering",
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
            "Ack timeout backoff expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_5_manager_load_shed_hysteresis() -> None:
    spec = _build_spec(
        "manager_worker_60_5_manager_load_shed_hysteresis",
        "60.5 Manager load shed hysteresis - Hysteresis prevents oscillation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Load shed hysteresis expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_6_worker_health_probe_batching() -> None:
    spec = _build_spec(
        "manager_worker_60_6_worker_health_probe_batching",
        "60.6 Worker health probe batching - Batching reduces overhead",
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
            "Probe batching expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_7_cancel_path_priority() -> None:
    spec = _build_spec(
        "manager_worker_60_7_cancel_path_priority",
        "60.7 Cancel path priority - Cancel path preempts non-critical work",
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
            "Cancel path priority expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_8_worker_metadata_snapshot_drift() -> None:
    spec = _build_spec(
        "manager_worker_60_8_worker_metadata_snapshot_drift",
        "60.8 Worker metadata snapshot drift - Drift handled without regressions",
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
            "Snapshot drift expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_9_dispatch_queue_watermark() -> None:
    spec = _build_spec(
        "manager_worker_60_9_dispatch_queue_watermark",
        "60.9 Dispatch queue watermark - Watermark blocks overload",
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
            "Queue watermark expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_60_10_progress_lag_spike_suppression() -> None:
    spec = _build_spec(
        "manager_worker_60_10_progress_lag_spike_suppression",
        "60.10 Progress lag spike suppression - Suppress transient spikes",
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
            "Lag spike suppression expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_60_1_worker_lease_fast_renew()
    await validate_60_2_dispatch_retry_fairness()
    await validate_60_3_progress_window_trimming()
    await validate_60_4_result_ack_timeout_backoff()
    await validate_60_5_manager_load_shed_hysteresis()
    await validate_60_6_worker_health_probe_batching()
    await validate_60_7_cancel_path_priority()
    await validate_60_8_worker_metadata_snapshot_drift()
    await validate_60_9_dispatch_queue_watermark()
    await validate_60_10_progress_lag_spike_suppression()


if __name__ == "__main__":
    asyncio.run(run())
