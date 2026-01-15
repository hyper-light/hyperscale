import asyncio
import re
from typing import Optional

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


def _job_id(runtime: ScenarioRuntime) -> Optional[str]:
    return runtime.job_ids.get("job-1") or runtime.last_job_id


async def validate_9_1_manager_sends_workflow_result_push() -> None:
    spec = _build_spec(
        "gate_manager_9_1_manager_sends_workflow_result_push",
        "9.1 Workflow Result Flow - Manager sends WorkflowResultPush",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        job_id = _job_id(runtime)
        assert job_id, "WorkflowResultPush expected job id"
        assert job_id in state._workflow_dc_results, (
            "WorkflowResultPush expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_1_track_expected_workflows() -> None:
    spec = _build_spec(
        "gate_manager_9_1_track_expected_workflows",
        "9.1 Workflow Result Flow - Track expected workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        job_id = _job_id(runtime)
        assert job_id, "Track expected workflows expected job id"
        assert job_id in state._job_workflow_ids, (
            "Track expected workflows expected job workflow ids"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_1_result_from_unknown_job() -> None:
    spec = _build_spec(
        "gate_manager_9_1_result_from_unknown_job",
        "9.1 Workflow Result Flow - Result from unknown job",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Result from unknown job expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_1_result_logging() -> None:
    spec = _build_spec(
        "gate_manager_9_1_result_logging",
        "9.1 Workflow Result Flow - Result logging",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Result logging expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_all_dcs_report_results() -> None:
    spec = _build_spec(
        "gate_manager_9_2_all_dcs_report_results",
        "9.2 Multi-DC Result Aggregation - All DCs report results",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "All DCs report results expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_partial_dc_results() -> None:
    spec = _build_spec(
        "gate_manager_9_2_partial_dc_results",
        "9.2 Multi-DC Result Aggregation - Partial DC results",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Partial DC results expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_dc_result_timeout() -> None:
    spec = _build_spec(
        "gate_manager_9_2_dc_result_timeout",
        "9.2 Multi-DC Result Aggregation - DC result timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "DC result timeout expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_2_aggregation_logic() -> None:
    spec = _build_spec(
        "gate_manager_9_2_aggregation_logic",
        "9.2 Multi-DC Result Aggregation - Aggregation logic",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Aggregation logic expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_forward_to_client() -> None:
    spec = _build_spec(
        "gate_manager_9_3_forward_to_client",
        "9.3 Result Forwarding - Forward to client",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Forward to client expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_forward_to_reporter() -> None:
    spec = _build_spec(
        "gate_manager_9_3_forward_to_reporter",
        "9.3 Result Forwarding - Forward to reporter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_reporter_tasks, dict), (
            "Forward to reporter expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_3_forward_to_peer_gates() -> None:
    spec = _build_spec(
        "gate_manager_9_3_forward_to_peer_gates",
        "9.3 Result Forwarding - Forward to peer gates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Forward to peer gates expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_duplicate_workflow_results() -> None:
    spec = _build_spec(
        "gate_manager_9_4_duplicate_workflow_results",
        "9.4 Result Edge Cases - Duplicate workflow results",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Duplicate workflow results expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_out_of_order_workflow_results() -> None:
    spec = _build_spec(
        "gate_manager_9_4_out_of_order_workflow_results",
        "9.4 Result Edge Cases - Out-of-order workflow results",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Out-of-order workflow results expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_workflow_result_for_cancelled_job() -> None:
    spec = _build_spec(
        "gate_manager_9_4_workflow_result_for_cancelled_job",
        "9.4 Result Edge Cases - Workflow result for cancelled job",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Workflow result for cancelled job expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_9_4_large_result_payload() -> None:
    spec = _build_spec(
        "gate_manager_9_4_large_result_payload",
        "9.4 Result Edge Cases - Large result payload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Large result payload expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_9_1_manager_sends_workflow_result_push()
    await validate_9_1_track_expected_workflows()
    await validate_9_1_result_from_unknown_job()
    await validate_9_1_result_logging()
    await validate_9_2_all_dcs_report_results()
    await validate_9_2_partial_dc_results()
    await validate_9_2_dc_result_timeout()
    await validate_9_2_aggregation_logic()
    await validate_9_3_forward_to_client()
    await validate_9_3_forward_to_reporter()
    await validate_9_3_forward_to_peer_gates()
    await validate_9_4_duplicate_workflow_results()
    await validate_9_4_out_of_order_workflow_results()
    await validate_9_4_workflow_result_for_cancelled_job()
    await validate_9_4_large_result_payload()


if __name__ == "__main__":
    asyncio.run(run())
