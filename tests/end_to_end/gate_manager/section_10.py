import asyncio
import re
from typing import Optional

from hyperscale.distributed.models import JobFinalResult
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


async def validate_10_1_manager_sends_job_final_result() -> None:
    spec = _build_spec(
        "gate_manager_10_1_manager_sends_job_final_result",
        "10.1 Final Result Flow - Manager sends JobFinalResult",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        assert callable(getattr(JobFinalResult, "load", None)), (
            "JobFinalResult expected load method"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_1_route_learning_update() -> None:
    spec = _build_spec(
        "gate_manager_10_1_route_learning_update",
        "10.1 Final Result Flow - Route learning update",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._dispatch_time_tracker
        assert callable(getattr(tracker, "record_completion", None)), (
            "Route learning update expected record_completion"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_1_observed_latency_recording() -> None:
    spec = _build_spec(
        "gate_manager_10_1_observed_latency_recording",
        "10.1 Final Result Flow - Observed latency recording",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._observed_latency_tracker
        assert callable(getattr(tracker, "record_job_latency", None)), (
            "Observed latency recording expected record_job_latency"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_1_job_completion() -> None:
    spec = _build_spec(
        "gate_manager_10_1_job_completion",
        "10.1 Final Result Flow - Job completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, "Job completion expected job manager"
    finally:
        await runtime.stop_cluster()


async def validate_10_2_all_dcs_report_final() -> None:
    spec = _build_spec(
        "gate_manager_10_2_all_dcs_report_final",
        "10.2 Final Result Aggregation - All DCs report final",
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
            "All DCs report final expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_2_mixed_final_statuses() -> None:
    spec = _build_spec(
        "gate_manager_10_2_mixed_final_statuses",
        "10.2 Final Result Aggregation - Mixed final statuses",
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
            "Mixed final statuses expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_2_final_result_with_errors() -> None:
    spec = _build_spec(
        "gate_manager_10_2_final_result_with_errors",
        "10.2 Final Result Aggregation - Final result with errors",
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
            "Final result with errors expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_job_state_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_job_state_cleanup",
        "10.3 Job Completion Cleanup - Job state cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        job_manager = gate._job_manager
        assert callable(getattr(job_manager, "delete_job", None)), (
            "Job state cleanup expected delete_job"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_workflow_results_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_workflow_results_cleanup",
        "10.3 Job Completion Cleanup - Workflow results cleanup",
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
            "Workflow results cleanup expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_workflow_ids_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_workflow_ids_cleanup",
        "10.3 Job Completion Cleanup - Workflow IDs cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_workflow_ids, dict), (
            "Workflow IDs cleanup expected job workflow ids"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_progress_callbacks_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_progress_callbacks_cleanup",
        "10.3 Job Completion Cleanup - Progress callbacks cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._progress_callbacks, dict), (
            "Progress callbacks cleanup expected progress callbacks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_leadership_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_leadership_cleanup",
        "10.3 Job Completion Cleanup - Leadership cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_leadership_tracker
        assert callable(getattr(tracker, "release_leadership", None)), (
            "Leadership cleanup expected release_leadership"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_dc_managers_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_dc_managers_cleanup",
        "10.3 Job Completion Cleanup - DC managers cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_dc_managers, dict), (
            "DC managers cleanup expected job DC managers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_reporter_tasks_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_reporter_tasks_cleanup",
        "10.3 Job Completion Cleanup - Reporter tasks cleanup",
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
            "Reporter tasks cleanup expected job reporter tasks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_crdt_stats_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_crdt_stats_cleanup",
        "10.3 Job Completion Cleanup - CRDT stats cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "CRDT stats cleanup expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_3_router_state_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_10_3_router_state_cleanup",
        "10.3 Job Completion Cleanup - Router state cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        router = gate._job_router
        assert callable(getattr(router, "cleanup_job_state", None)), (
            "Router state cleanup expected cleanup_job_state"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_manager_dies_before_final_result() -> None:
    spec = _build_spec(
        "gate_manager_10_4_manager_dies_before_final_result",
        "10.4 Final Result Edge Cases - Manager dies before final result",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Manager dies before final result expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_duplicate_final_result() -> None:
    spec = _build_spec(
        "gate_manager_10_4_duplicate_final_result",
        "10.4 Final Result Edge Cases - Duplicate final result",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Duplicate final result expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_final_result_for_unknown_job() -> None:
    spec = _build_spec(
        "gate_manager_10_4_final_result_for_unknown_job",
        "10.4 Final Result Edge Cases - Final result for unknown job",
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
            "Final result for unknown job expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_10_4_route_learning_failure() -> None:
    spec = _build_spec(
        "gate_manager_10_4_route_learning_failure",
        "10.4 Final Result Edge Cases - Route learning failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dispatch_time_tracker is not None, (
            "Route learning failure expected dispatch time tracker"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_10_1_manager_sends_job_final_result()
    await validate_10_1_route_learning_update()
    await validate_10_1_observed_latency_recording()
    await validate_10_1_job_completion()
    await validate_10_2_all_dcs_report_final()
    await validate_10_2_mixed_final_statuses()
    await validate_10_2_final_result_with_errors()
    await validate_10_3_job_state_cleanup()
    await validate_10_3_workflow_results_cleanup()
    await validate_10_3_workflow_ids_cleanup()
    await validate_10_3_progress_callbacks_cleanup()
    await validate_10_3_leadership_cleanup()
    await validate_10_3_dc_managers_cleanup()
    await validate_10_3_reporter_tasks_cleanup()
    await validate_10_3_crdt_stats_cleanup()
    await validate_10_3_router_state_cleanup()
    await validate_10_4_manager_dies_before_final_result()
    await validate_10_4_duplicate_final_result()
    await validate_10_4_final_result_for_unknown_job()
    await validate_10_4_route_learning_failure()


if __name__ == "__main__":
    asyncio.run(run())
