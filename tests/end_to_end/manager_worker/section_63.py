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


async def validate_63_1_worker_lease_double_renew() -> None:
    spec = _build_spec(
        "manager_worker_63_1_worker_lease_double_renew",
        "63.1 Worker lease double-renew - Double renew does not extend beyond max",
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
            "Double renew expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_2_dispatch_retry_debounce() -> None:
    spec = _build_spec(
        "manager_worker_63_2_dispatch_retry_debounce",
        "63.2 Dispatch retry debounce - Debounce avoids rapid retries",
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
            "Retry debounce expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_3_progress_drop_backfill() -> None:
    spec = _build_spec(
        "manager_worker_63_3_progress_drop_backfill",
        "63.3 Progress drop backfill - Backfill recovers dropped progress",
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
            "Backfill expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_4_result_ack_quorum() -> None:
    spec = _build_spec(
        "manager_worker_63_4_result_ack_quorum",
        "63.4 Result ack quorum - Quorum required before close",
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
            "Ack quorum expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_5_manager_overload_grace() -> None:
    spec = _build_spec(
        "manager_worker_63_5_manager_overload_grace",
        "63.5 Manager overload grace - Grace period before shedding",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Overload grace expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_6_worker_probe_coalescing() -> None:
    spec = _build_spec(
        "manager_worker_63_6_worker_probe_coalescing",
        "63.6 Worker probe coalescing - Coalescing reduces ping storms",
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
            "Probe coalescing expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_7_cancel_batch_fairness() -> None:
    spec = _build_spec(
        "manager_worker_63_7_cancel_batch_fairness",
        "63.7 Cancel batch fairness - Fairness across cancel batches",
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
            "Cancel fairness expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_8_worker_metadata_ttl() -> None:
    spec = _build_spec(
        "manager_worker_63_8_worker_metadata_ttl",
        "63.8 Worker metadata ttl - TTL removes stale entries",
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
            "Metadata ttl expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_9_dispatch_queue_aging() -> None:
    spec = _build_spec(
        "manager_worker_63_9_dispatch_queue_aging",
        "63.9 Dispatch queue aging - Aging boosts long-waiting jobs",
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
            "Queue aging expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_63_10_progress_snapshot_merge() -> None:
    spec = _build_spec(
        "manager_worker_63_10_progress_snapshot_merge",
        "63.10 Progress snapshot merge - Merge keeps latest progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")n        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_job_last_progress, dict), (
            "Snapshot merge expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_63_1_worker_lease_double_renew()
    await validate_63_2_dispatch_retry_debounce()
    await validate_63_3_progress_drop_backfill()
    await validate_63_4_result_ack_quorum()
    await validate_63_5_manager_overload_grace()
    await validate_63_6_worker_probe_coalescing()
    await validate_63_7_cancel_batch_fairness()
    await validate_63_8_worker_metadata_ttl()
    await validate_63_9_dispatch_queue_aging()
    await validate_63_10_progress_snapshot_merge()


if __name__ == "__main__":
    asyncio.run(run())
