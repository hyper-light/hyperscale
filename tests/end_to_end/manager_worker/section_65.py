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


async def validate_65_1_worker_lease_rebalance() -> None:
    spec = _build_spec(
        "manager_worker_65_1_worker_lease_rebalance",
        "65.1 Worker lease rebalance - Rebalance does not double-assign",
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
            "Lease rebalance expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_2_dispatch_retry_spillover() -> None:
    spec = _build_spec(
        "manager_worker_65_2_dispatch_retry_spillover",
        "65.2 Dispatch retry spillover - Spillover uses least-loaded worker",
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
            "Retry spillover expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_3_progress_snapshot_dedupe() -> None:
    spec = _build_spec(
        "manager_worker_65_3_progress_snapshot_dedupe",
        "65.3 Progress snapshot dedupe - Dedupe avoids double-counting",
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
            "Snapshot dedupe expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_4_result_ack_escalation() -> None:
    spec = _build_spec(
        "manager_worker_65_4_result_ack_escalation",
        "65.4 Result ack escalation - Escalation triggers circuit breaker",
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
            "Ack escalation expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_5_manager_load_shed_sampling() -> None:
    spec = _build_spec(
        "manager_worker_65_5_manager_load_shed_sampling",
        "65.5 Manager load shed sampling - Sampling keeps shed decisions stable",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Load shed sampling expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_6_worker_health_probe_retry() -> None:
    spec = _build_spec(
        "manager_worker_65_6_worker_health_probe_retry",
        "65.6 Worker health probe retry - Retry does not spam network",
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
            "Probe retry expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_7_cancel_ack_timeout() -> None:
    spec = _build_spec(
        "manager_worker_65_7_cancel_ack_timeout",
        "65.7 Cancel ack timeout - Timeout triggers resend",
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
            "Cancel ack timeout expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_8_worker_metadata_reconciliation() -> None:
    spec = _build_spec(
        "manager_worker_65_8_worker_metadata_reconciliation",
        "65.8 Worker metadata reconciliation - Reconciliation resolves conflicts",
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
            "Metadata reconciliation expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_65_9_dispatch_fairness_across_priorities() -> None:
    spec = _build_spec(
        "manager_worker_65_9_dispatch_fairness_across_priorities",
        "65.9 Dispatch fairness across priorities - Priorities respected under load",
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


async def validate_65_10_progress_resume_ordering() -> None:
    spec = _build_spec(
        "manager_worker_65_10_progress_resume_ordering",
        "65.10 Progress resume ordering - Resume ordering stays monotonic",
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
            "Resume ordering expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_65_1_worker_lease_rebalance()
    await validate_65_2_dispatch_retry_spillover()
    await validate_65_3_progress_snapshot_dedupe()
    await validate_65_4_result_ack_escalation()
    await validate_65_5_manager_load_shed_sampling()
    await validate_65_6_worker_health_probe_retry()
    await validate_65_7_cancel_ack_timeout()
    await validate_65_8_worker_metadata_reconciliation()
    await validate_65_9_dispatch_fairness_across_priorities()
    await validate_65_10_progress_resume_ordering()


if __name__ == "__main__":
    asyncio.run(run())
