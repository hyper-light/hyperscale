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


async def validate_18_1_worker_job_received() -> None:
    spec = _build_spec(
        "manager_worker_18_1_worker_job_received",
        "18.1 Workflow Events - WorkerJobReceived",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_tokens, dict), (
            "WorkerJobReceived expected workflow tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_1_worker_job_started() -> None:
    spec = _build_spec(
        "manager_worker_18_1_worker_job_started",
        "18.1 Workflow Events - WorkerJobStarted",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_start_times, dict), (
            "WorkerJobStarted expected workflow start times"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_1_worker_job_completed() -> None:
    spec = _build_spec(
        "manager_worker_18_1_worker_job_completed",
        "18.1 Workflow Events - WorkerJobCompleted",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cores_completed, dict), (
            "WorkerJobCompleted expected workflow cores completed"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_1_worker_job_failed() -> None:
    spec = _build_spec(
        "manager_worker_18_1_worker_job_failed",
        "18.1 Workflow Events - WorkerJobFailed",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_cores_completed, dict), (
            "WorkerJobFailed expected workflow cores completed"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_timing_fields() -> None:
    spec = _build_spec(
        "manager_worker_18_2_timing_fields",
        "18.2 Event Fields - Timing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_start_times, dict), (
            "Timing fields expected workflow start times"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_identifier_fields() -> None:
    spec = _build_spec(
        "manager_worker_18_2_identifier_fields",
        "18.2 Event Fields - Identifiers",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_tokens, dict), (
            "Identifiers expected workflow tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_metrics_fields() -> None:
    spec = _build_spec(
        "manager_worker_18_2_metrics_fields",
        "18.2 Event Fields - Metrics",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._workflow_latency_digest is not None, (
            "Metrics fields expected latency digest"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_error_fields() -> None:
    spec = _build_spec(
        "manager_worker_18_2_error_fields",
        "18.2 Event Fields - Errors",
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
            "Error fields expected workflow retries"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_18_1_worker_job_received()
    await validate_18_1_worker_job_started()
    await validate_18_1_worker_job_completed()
    await validate_18_1_worker_job_failed()
    await validate_18_2_timing_fields()
    await validate_18_2_identifier_fields()
    await validate_18_2_metrics_fields()
    await validate_18_2_error_fields()


if __name__ == "__main__":
    asyncio.run(run())
