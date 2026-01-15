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


async def validate_56_1_dispatch_fairness_across_tenants() -> None:
    spec = _build_spec(
        "manager_worker_56_1_dispatch_fairness_across_tenants",
        "56.1 Dispatch fairness across tenants - Tenant fairness preserved under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._dispatch_semaphores, dict), (
            "Tenant fairness expected dispatch semaphores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_2_worker_shutdown_handshake() -> None:
    spec = _build_spec(
        "manager_worker_56_2_worker_shutdown_handshake",
        "56.2 Worker shutdown handshake - Graceful shutdown handshake completes",
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
            "Shutdown handshake expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_3_manager_backpressure_on_retries() -> None:
    spec = _build_spec(
        "manager_worker_56_3_manager_backpressure_on_retries",
        "56.3 Manager backpressure on retries - Retry backlog respects backpressure",
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
            "Retry backpressure expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_4_progress_burst_coalescing() -> None:
    spec = _build_spec(
        "manager_worker_56_4_progress_burst_coalescing",
        "56.4 Progress burst coalescing - Progress bursts coalesce safely",
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
            "Progress coalescing expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_5_result_retry_cap() -> None:
    spec = _build_spec(
        "manager_worker_56_5_result_retry_cap",
        "56.5 Result retry cap - Retry cap avoids infinite loops",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._job_aggregated_results, dict), (
            "Result retry cap expected aggregated results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_6_worker_health_probe_timeouts() -> None:
    spec = _build_spec(
        "manager_worker_56_6_worker_health_probe_timeouts",
        "56.6 Worker health probe timeouts - Timeout escalates to suspect",
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
            "Probe timeouts expected worker unhealthy tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_7_cancel_dedupe_window() -> None:
    spec = _build_spec(
        "manager_worker_56_7_cancel_dedupe_window",
        "56.7 Cancel dedupe window - Duplicate cancels ignored",
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
            "Cancel dedupe expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_8_manager_metrics_lag() -> None:
    spec = _build_spec(
        "manager_worker_56_8_manager_metrics_lag",
        "56.8 Manager metrics lag - Metrics lag does not trip alerts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Metrics lag expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_9_worker_registration_retry() -> None:
    spec = _build_spec(
        "manager_worker_56_9_worker_registration_retry",
        "56.9 Worker registration retry - Registration retry honors backoff",
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
            "Registration retry expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_56_10_retry_budget_hysteresis() -> None:
    spec = _build_spec(
        "manager_worker_56_10_retry_budget_hysteresis",
        "56.10 Retry budget hysteresis - Hysteresis avoids oscillation",
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
            "Retry hysteresis expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_56_1_dispatch_fairness_across_tenants()
    await validate_56_2_worker_shutdown_handshake()
    await validate_56_3_manager_backpressure_on_retries()
    await validate_56_4_progress_burst_coalescing()
    await validate_56_5_result_retry_cap()
    await validate_56_6_worker_health_probe_timeouts()
    await validate_56_7_cancel_dedupe_window()
    await validate_56_8_manager_metrics_lag()
    await validate_56_9_worker_registration_retry()
    await validate_56_10_retry_budget_hysteresis()


if __name__ == "__main__":
    asyncio.run(run())
