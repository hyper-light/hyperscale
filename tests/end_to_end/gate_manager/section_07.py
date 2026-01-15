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


async def validate_7_1_manager_sends_job_progress() -> None:
    spec = _build_spec(
        "gate_manager_7_1_manager_sends_job_progress",
        "7.1 Progress Updates - Manager sends JobProgress",
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
        assert job_id, "Manager sends JobProgress expected job id"
        assert job_id in state._job_progress_sequences, (
            "Manager sends JobProgress expected job progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_manager_sends_job_progress_report() -> None:
    spec = _build_spec(
        "gate_manager_7_1_manager_sends_job_progress_report",
        "7.1 Progress Updates - Manager sends JobProgressReport",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        timeout_tracker = gate._job_timeout_tracker
        assert callable(getattr(timeout_tracker, "record_progress", None)), (
            "JobProgressReport expected record_progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_progress_from_multiple_dcs() -> None:
    spec = _build_spec(
        "gate_manager_7_1_progress_from_multiple_dcs",
        "7.1 Progress Updates - Progress from multiple DCs",
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
        assert job_id, "Progress from multiple DCs expected job id"
        assert job_id in state._job_progress_seen, (
            "Progress from multiple DCs expected job progress seen"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_progress_with_workflow_details() -> None:
    spec = _build_spec(
        "gate_manager_7_1_progress_with_workflow_details",
        "7.1 Progress Updates - Progress with workflow details",
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
        assert job_id, "Progress with workflow details expected job id"
        assert job_id in state._job_progress_sequences, (
            "Progress with workflow details expected job progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_1_progress_callback_forwarding() -> None:
    spec = _build_spec(
        "gate_manager_7_1_progress_callback_forwarding",
        "7.1 Progress Updates - Progress callback forwarding",
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
        assert job_id, "Progress callback forwarding expected job id"
        assert job_id in state._progress_callbacks, (
            "Progress callback forwarding expected progress callbacks entry"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_out_of_order_progress() -> None:
    spec = _build_spec(
        "gate_manager_7_2_out_of_order_progress",
        "7.2 Progress Edge Cases - Out-of-order progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_sequences, dict), (
            "Out-of-order progress expected job progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_duplicate_progress() -> None:
    spec = _build_spec(
        "gate_manager_7_2_duplicate_progress",
        "7.2 Progress Edge Cases - Duplicate progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_seen, dict), (
            "Duplicate progress expected job progress seen tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_progress_for_unknown_job() -> None:
    spec = _build_spec(
        "gate_manager_7_2_progress_for_unknown_job",
        "7.2 Progress Edge Cases - Progress for unknown job",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_sequences, dict), (
            "Progress for unknown job expected job progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_progress_after_job_complete() -> None:
    spec = _build_spec(
        "gate_manager_7_2_progress_after_job_complete",
        "7.2 Progress Edge Cases - Progress after job complete",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_seen, dict), (
            "Progress after job complete expected job progress seen"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_2_manager_dies_mid_progress_stream() -> None:
    spec = _build_spec(
        "gate_manager_7_2_manager_dies_mid_progress_stream",
        "7.2 Progress Edge Cases - Manager dies mid-progress-stream",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_seen, dict), (
            "Manager dies mid-progress-stream expected progress tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_3_aggregate_progress_across_dcs() -> None:
    spec = _build_spec(
        "gate_manager_7_3_aggregate_progress_across_dcs",
        "7.3 Progress Aggregation - Aggregate progress across DCs",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_sequences, dict), (
            "Aggregate progress across DCs expected job progress sequences"
        )
        assert isinstance(state._job_progress_seen, dict), (
            "Aggregate progress across DCs expected job progress seen"
        )
    finally:
        await runtime.stop_cluster()


async def validate_7_3_progress_percentage_calculation() -> None:
    spec = _build_spec(
        "gate_manager_7_3_progress_percentage_calculation",
        "7.3 Progress Aggregation - Progress percentage calculation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_progress_sequences, dict), (
            "Progress percentage calculation expected job progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_7_1_manager_sends_job_progress()
    await validate_7_1_manager_sends_job_progress_report()
    await validate_7_1_progress_from_multiple_dcs()
    await validate_7_1_progress_with_workflow_details()
    await validate_7_1_progress_callback_forwarding()
    await validate_7_2_out_of_order_progress()
    await validate_7_2_duplicate_progress()
    await validate_7_2_progress_for_unknown_job()
    await validate_7_2_progress_after_job_complete()
    await validate_7_2_manager_dies_mid_progress_stream()
    await validate_7_3_aggregate_progress_across_dcs()
    await validate_7_3_progress_percentage_calculation()


if __name__ == "__main__":
    asyncio.run(run())
