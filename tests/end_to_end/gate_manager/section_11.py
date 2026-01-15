import asyncio
import re

from hyperscale.distributed.nodes.gate import GateServer

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
                "dc_count": 2,
                "managers_per_dc": 2,
                "workers_per_dc": 1,
                "cores_per_worker": 1,
                "base_gate_tcp": 8000,
            },
            "actions": [
                {"type": "start_cluster"},
                {"type": "await_gate_leader", "params": {"timeout": 30}},
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-A", "timeout": 30},
                },
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-B", "timeout": 30},
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


def _get_gate(runtime: ScenarioRuntime) -> GateServer:
    cluster = runtime.require_cluster()
    gate = cluster.get_gate_leader() or cluster.gates[0]
    return gate


def _require_runtime(outcome: ScenarioOutcome) -> ScenarioRuntime:
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def validate_11_1_progress_timeout() -> None:
    spec = _build_spec(
        "gate_manager_11_1_progress_timeout",
        "11.1 Timeout Detection - Progress timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Progress timeout expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_1_dc_local_timeout() -> None:
    spec = _build_spec(
        "gate_manager_11_1_dc_local_timeout",
        "11.1 Timeout Detection - DC-local timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "DC-local timeout expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_1_all_dc_stuck_detection() -> None:
    spec = _build_spec(
        "gate_manager_11_1_all_dc_stuck_detection",
        "11.1 Timeout Detection - All-DC stuck detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "All-DC stuck detection expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_1_global_timeout() -> None:
    spec = _build_spec(
        "gate_manager_11_1_global_timeout",
        "11.1 Timeout Detection - Global timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Global timeout expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_timeout_triggers_cancellation() -> None:
    spec = _build_spec(
        "gate_manager_11_2_timeout_triggers_cancellation",
        "11.2 Timeout Handling - Timeout triggers cancellation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_completion_events, dict), (
            "Timeout triggers cancellation expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_timeout_with_partial_completion() -> None:
    spec = _build_spec(
        "gate_manager_11_2_timeout_with_partial_completion",
        "11.2 Timeout Handling - Timeout with partial completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Timeout with partial completion expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_leader_transfer_on_timeout() -> None:
    spec = _build_spec(
        "gate_manager_11_2_leader_transfer_on_timeout",
        "11.2 Timeout Handling - Leader transfer on timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_leadership_tracker
        assert tracker is not None, (
            "Leader transfer on timeout expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_3_start_tracker() -> None:
    spec = _build_spec(
        "gate_manager_11_3_start_tracker",
        "11.3 Timeout Tracker Lifecycle - Start tracker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_timeout_tracker
        assert callable(getattr(tracker, "start", None)), (
            "Start tracker expected start method"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_3_stop_tracker() -> None:
    spec = _build_spec(
        "gate_manager_11_3_stop_tracker",
        "11.3 Timeout Tracker Lifecycle - Stop tracker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_timeout_tracker
        assert callable(getattr(tracker, "stop", None)), (
            "Stop tracker expected stop method"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_3_job_registration() -> None:
    spec = _build_spec(
        "gate_manager_11_3_job_registration",
        "11.3 Timeout Tracker Lifecycle - Job registration",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_timeout_tracker
        assert callable(getattr(tracker, "register_job", None)), (
            "Job registration expected register_job"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_3_job_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_11_3_job_cleanup",
        "11.3 Timeout Tracker Lifecycle - Job cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_timeout_tracker
        assert callable(getattr(tracker, "remove_job", None)), (
            "Job cleanup expected remove_job"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_11_1_progress_timeout()
    await validate_11_1_dc_local_timeout()
    await validate_11_1_all_dc_stuck_detection()
    await validate_11_1_global_timeout()
    await validate_11_2_timeout_triggers_cancellation()
    await validate_11_2_timeout_with_partial_completion()
    await validate_11_2_leader_transfer_on_timeout()
    await validate_11_3_start_tracker()
    await validate_11_3_stop_tracker()
    await validate_11_3_job_registration()
    await validate_11_3_job_cleanup()


if __name__ == "__main__":
    asyncio.run(run())
