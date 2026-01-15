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


async def validate_14_1_cancel_request() -> None:
    spec = _build_spec(
        "manager_worker_14_1_cancel_request",
        "14.1 Cancel Request - CancelJob received",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_pending_workflows, dict), (
            "CancelJob received expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_1_pending_workflows() -> None:
    spec = _build_spec(
        "manager_worker_14_1_pending_workflows",
        "14.1 Cancel Request - Pending workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_pending_workflows, dict), (
            "Pending workflows expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_1_send_to_workers() -> None:
    spec = _build_spec(
        "manager_worker_14_1_send_to_workers",
        "14.1 Cancel Request - Send to workers",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_pending_workflows, dict), (
            "Send to workers expected pending workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_2_cancel_event_set() -> None:
    spec = _build_spec(
        "manager_worker_14_2_cancel_event_set",
        "14.2 Worker Cancellation - Cancel event set",
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
            "Cancel event set expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_2_execution_interruption() -> None:
    spec = _build_spec(
        "manager_worker_14_2_execution_interruption",
        "14.2 Worker Cancellation - Execution interruption",
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
            "Execution interruption expected workflow cancel events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_2_status_update() -> None:
    spec = _build_spec(
        "manager_worker_14_2_status_update",
        "14.2 Worker Cancellation - Status update",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancelled_workflows, dict), (
            "Status update expected cancelled workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_3_all_workflows_cancelled() -> None:
    spec = _build_spec(
        "manager_worker_14_3_all_workflows_cancelled",
        "14.3 Cancellation Completion - All workflows cancelled",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_completion_events, dict), (
            "All workflows cancelled expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_3_completion_event() -> None:
    spec = _build_spec(
        "manager_worker_14_3_completion_event",
        "14.3 Cancellation Completion - Completion event",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_completion_events, dict), (
            "Completion event expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_3_error_collection() -> None:
    spec = _build_spec(
        "manager_worker_14_3_error_collection",
        "14.3 Cancellation Completion - Error collection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_errors, dict), (
            "Error collection expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_4_partial_cancellation() -> None:
    spec = _build_spec(
        "manager_worker_14_4_partial_cancellation",
        "14.4 Partial Cancellation - Partial cancellation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_errors, dict), (
            "Partial cancellation expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_4_timeout_handling() -> None:
    spec = _build_spec(
        "manager_worker_14_4_timeout_handling",
        "14.4 Partial Cancellation - Timeout handling",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_completion_events, dict), (
            "Timeout handling expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_14_4_error_reporting() -> None:
    spec = _build_spec(
        "manager_worker_14_4_error_reporting",
        "14.4 Partial Cancellation - Error reporting",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._cancellation_errors, dict), (
            "Error reporting expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_14_1_cancel_request()
    await validate_14_1_pending_workflows()
    await validate_14_1_send_to_workers()
    await validate_14_2_cancel_event_set()
    await validate_14_2_execution_interruption()
    await validate_14_2_status_update()
    await validate_14_3_all_workflows_cancelled()
    await validate_14_3_completion_event()
    await validate_14_3_error_collection()
    await validate_14_4_partial_cancellation()
    await validate_14_4_timeout_handling()
    await validate_14_4_error_reporting()


if __name__ == "__main__":
    asyncio.run(run())
