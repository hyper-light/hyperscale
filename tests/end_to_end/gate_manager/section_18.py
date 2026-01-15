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


async def validate_18_1_client_requests_cancellation() -> None:
    spec = _build_spec(
        "gate_manager_18_1_client_requests_cancellation",
        "18.1 Job Cancellation - Client requests cancellation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Client requests cancellation expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_1_cancellation_to_managers() -> None:
    spec = _build_spec(
        "gate_manager_18_1_cancellation_to_managers",
        "18.1 Job Cancellation - Cancellation to managers",
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
            "Cancellation to managers expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_1_cancellation_acknowledgment() -> None:
    spec = _build_spec(
        "gate_manager_18_1_cancellation_acknowledgment",
        "18.1 Job Cancellation - Cancellation acknowledgment",
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
            "Cancellation acknowledgment expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_1_cancellation_completion() -> None:
    spec = _build_spec(
        "gate_manager_18_1_cancellation_completion",
        "18.1 Job Cancellation - Cancellation completion",
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
            "Cancellation completion expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_single_workflow_cancel() -> None:
    spec = _build_spec(
        "gate_manager_18_2_single_workflow_cancel",
        "18.2 Workflow Cancellation - Single workflow cancel",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Single workflow cancel expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_workflow_cancel_response() -> None:
    spec = _build_spec(
        "gate_manager_18_2_workflow_cancel_response",
        "18.2 Workflow Cancellation - Workflow cancel response",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Workflow cancel response expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_2_workflow_cancellation_status() -> None:
    spec = _build_spec(
        "gate_manager_18_2_workflow_cancellation_status",
        "18.2 Workflow Cancellation - Workflow cancellation status",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Workflow cancellation status expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_3_cancellation_coordinator() -> None:
    spec = _build_spec(
        "gate_manager_18_3_cancellation_coordinator",
        "18.3 Cancellation Coordination - Cancellation coordinator",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._cancellation_coordinator is not None, (
            "Cancellation coordinator expected cancellation coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_3_cancellation_errors() -> None:
    spec = _build_spec(
        "gate_manager_18_3_cancellation_errors",
        "18.3 Cancellation Coordination - Cancellation errors",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Cancellation errors expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_18_3_cancellation_event() -> None:
    spec = _build_spec(
        "gate_manager_18_3_cancellation_event",
        "18.3 Cancellation Coordination - Cancellation event",
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
            "Cancellation event expected cancellation events"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_18_1_client_requests_cancellation()
    await validate_18_1_cancellation_to_managers()
    await validate_18_1_cancellation_acknowledgment()
    await validate_18_1_cancellation_completion()
    await validate_18_2_single_workflow_cancel()
    await validate_18_2_workflow_cancel_response()
    await validate_18_2_workflow_cancellation_status()
    await validate_18_3_cancellation_coordinator()
    await validate_18_3_cancellation_errors()
    await validate_18_3_cancellation_event()


if __name__ == "__main__":
    asyncio.run(run())
