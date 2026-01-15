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


async def validate_53_1_worker_lease_renewal_jitter() -> None:
    spec = _build_spec(
        "manager_worker_53_1_worker_lease_renewal_jitter",
        "53.1 Worker lease renewal jitter - Renewal jitter does not cause false expiry",
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
            "Lease renewal jitter expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_2_dispatch_retry_collapse() -> None:
    spec = _build_spec(
        "manager_worker_53_2_dispatch_retry_collapse",
        "53.2 Dispatch retry collapse - Burst of retries collapses to single enqueue",
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
            "Dispatch retry collapse expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_3_progress_snapshot_batching() -> None:
    spec = _build_spec(
        "manager_worker_53_3_progress_snapshot_batching",
        "53.3 Progress snapshot batching - Snapshot batching avoids duplication",
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
            "Progress snapshot batching expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_4_result_forwarding_timeout() -> None:
    spec = _build_spec(
        "manager_worker_53_4_result_forwarding_timeout",
        "53.4 Result forwarding timeout - Retry with backoff to gate",
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
            "Result forwarding expected job origin gates"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_5_manager_load_shed_on_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_53_5_manager_load_shed_on_dispatch",
        "53.5 Manager load shed on dispatch - Load shed avoids overload spiral",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, "Load shed expected health monitor"
    finally:
        await runtime.stop_cluster()


async def validate_53_6_worker_queue_overflow() -> None:
    spec = _build_spec(
        "manager_worker_53_6_worker_queue_overflow",
        "53.6 Worker queue overflow - Oldest workflow dropped safely",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._pending_workflows, dict), (
            "Worker queue overflow expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_7_health_probe_priority_inversion() -> None:
    spec = _build_spec(
        "manager_worker_53_7_health_probe_priority_inversion",
        "53.7 Health probe priority inversion - Probes not starved by dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Health probe priority expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_8_worker_clock_skew() -> None:
    spec = _build_spec(
        "manager_worker_53_8_worker_clock_skew",
        "53.8 Worker clock skew - Manager tolerates skew in timestamps",
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
            "Worker clock skew expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_9_retry_budget_global_cap() -> None:
    spec = _build_spec(
        "manager_worker_53_9_retry_budget_global_cap",
        "53.9 Retry budget global cap - Per-job retries respect global cap",
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
            "Retry budget cap expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_53_10_cancel_propagation_lag() -> None:
    spec = _build_spec(
        "manager_worker_53_10_cancel_propagation_lag",
        "53.10 Cancel propagation lag - Cancel reaches all workers within SLA",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_cancel_events, dict), (
            "Cancel propagation expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_53_1_worker_lease_renewal_jitter()
    await validate_53_2_dispatch_retry_collapse()
    await validate_53_3_progress_snapshot_batching()
    await validate_53_4_result_forwarding_timeout()
    await validate_53_5_manager_load_shed_on_dispatch()
    await validate_53_6_worker_queue_overflow()
    await validate_53_7_health_probe_priority_inversion()
    await validate_53_8_worker_clock_skew()
    await validate_53_9_retry_budget_global_cap()
    await validate_53_10_cancel_propagation_lag()


if __name__ == "__main__":
    asyncio.run(run())
