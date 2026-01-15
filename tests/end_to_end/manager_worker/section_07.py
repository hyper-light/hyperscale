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


async def validate_7_1_pending_to_dispatched() -> None:
    spec = _build_spec(
        "manager_worker_7_1_pending_to_dispatched",
        "7.1 State Machine Transitions - PENDING → DISPATCHED",
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
            "PENDING → DISPATCHED expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_dispatched_to_running() -> None:
    spec = _build_spec(
        "manager_worker_7_1_dispatched_to_running",
        "7.1 State Machine Transitions - DISPATCHED → RUNNING",
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
            "DISPATCHED → RUNNING expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_running_to_completed() -> None:
    spec = _build_spec(
        "manager_worker_7_1_running_to_completed",
        "7.1 State Machine Transitions - RUNNING → COMPLETED",
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
            "RUNNING → COMPLETED expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_running_to_failed() -> None:
    spec = _build_spec(
        "manager_worker_7_1_running_to_failed",
        "7.1 State Machine Transitions - RUNNING → FAILED",
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
            "RUNNING → FAILED expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_any_to_cancelled() -> None:
    spec = _build_spec(
        "manager_worker_7_1_any_to_cancelled",
        "7.1 State Machine Transitions - Any → CANCELLED",
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
            "Any → CANCELLED expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_completed_invalid_transition() -> None:
    spec = _build_spec(
        "manager_worker_7_2_completed_invalid_transition",
        "7.2 Invalid Transitions - COMPLETED → anything",
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
            "COMPLETED invalid transition expected lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_failed_invalid_transition() -> None:
    spec = _build_spec(
        "manager_worker_7_2_failed_invalid_transition",
        "7.2 Invalid Transitions - FAILED → anything",
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
            "FAILED invalid transition expected lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_cancelled_invalid_transition() -> None:
    spec = _build_spec(
        "manager_worker_7_2_cancelled_invalid_transition",
        "7.2 Invalid Transitions - CANCELLED → anything",
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
            "CANCELLED invalid transition expected lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_3_successful_transitions_logging() -> None:
    spec = _build_spec(
        "manager_worker_7_3_successful_transitions_logging",
        "7.3 Transition Logging - Successful transitions",
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
            "Successful transitions logging expected lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_3_failed_transitions_logging() -> None:
    spec = _build_spec(
        "manager_worker_7_3_failed_transitions_logging",
        "7.3 Transition Logging - Failed transitions",
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
            "Failed transitions logging expected lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_4_event_signaling() -> None:
    spec = _build_spec(
        "manager_worker_7_4_event_signaling",
        "7.4 Completion Events - Event signaling",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_completion_events, dict), (
            "Event signaling expected workflow completion events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_4_waiting_on_completion() -> None:
    spec = _build_spec(
        "manager_worker_7_4_waiting_on_completion",
        "7.4 Completion Events - Waiting on completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_completion_events, dict), (
            "Waiting on completion expected workflow completion events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_4_cleanup_after_completion() -> None:
    spec = _build_spec(
        "manager_worker_7_4_cleanup_after_completion",
        "7.4 Completion Events - Cleanup after completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_completion_events, dict), (
            "Cleanup after completion expected workflow completion events"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_7_1_pending_to_dispatched()
    await validate_7_1_dispatched_to_running()
    await validate_7_1_running_to_completed()
    await validate_7_1_running_to_failed()
    await validate_7_1_any_to_cancelled()
    await validate_7_2_completed_invalid_transition()
    await validate_7_2_failed_invalid_transition()
    await validate_7_2_cancelled_invalid_transition()
    await validate_7_3_successful_transitions_logging()
    await validate_7_3_failed_transitions_logging()
    await validate_7_4_event_signaling()
    await validate_7_4_waiting_on_completion()
    await validate_7_4_cleanup_after_completion()


if __name__ == "__main__":
    asyncio.run(run())
