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


async def validate_34_1_sub_millisecond_actions() -> None:
    spec = _build_spec(
        "gate_manager_34_1_sub_millisecond_actions",
        "34.1 Action Timing Stats - Sub-millisecond actions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Sub-millisecond actions expected windowed stats"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_1_very_long_actions() -> None:
    spec = _build_spec(
        "gate_manager_34_1_very_long_actions",
        "34.1 Action Timing Stats - Very long actions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Very long actions expected windowed stats"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_1_action_timeout_stats() -> None:
    spec = _build_spec(
        "gate_manager_34_1_action_timeout_stats",
        "34.1 Action Timing Stats - Action timeout stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Action timeout stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_1_action_retry_stats() -> None:
    spec = _build_spec(
        "gate_manager_34_1_action_retry_stats",
        "34.1 Action Timing Stats - Action retry stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Action retry stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_2_vu_ramp_up_stats() -> None:
    spec = _build_spec(
        "gate_manager_34_2_vu_ramp_up_stats",
        "34.2 VU Lifecycle Stats - VU ramp-up stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, "VU ramp-up expected job stats CRDT"
    finally:
        await runtime.stop_cluster()


async def validate_34_2_vu_ramp_down_stats() -> None:
    spec = _build_spec(
        "gate_manager_34_2_vu_ramp_down_stats",
        "34.2 VU Lifecycle Stats - VU ramp-down stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, "VU ramp-down expected job stats CRDT"
    finally:
        await runtime.stop_cluster()


async def validate_34_2_vu_iteration_stats() -> None:
    spec = _build_spec(
        "gate_manager_34_2_vu_iteration_stats",
        "34.2 VU Lifecycle Stats - VU iteration stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "VU iteration stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_2_vu_error_rate() -> None:
    spec = _build_spec(
        "gate_manager_34_2_vu_error_rate",
        "34.2 VU Lifecycle Stats - VU error rate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, "VU error rate expected job stats CRDT"
    finally:
        await runtime.stop_cluster()


async def validate_34_3_workflow_duration_histogram() -> None:
    spec = _build_spec(
        "gate_manager_34_3_workflow_duration_histogram",
        "34.3 Workflow-Level Stats - Workflow duration histogram",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Workflow duration histogram expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_3_workflow_throughput() -> None:
    spec = _build_spec(
        "gate_manager_34_3_workflow_throughput",
        "34.3 Workflow-Level Stats - Workflow throughput",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Workflow throughput expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_3_workflow_failure_rate() -> None:
    spec = _build_spec(
        "gate_manager_34_3_workflow_failure_rate",
        "34.3 Workflow-Level Stats - Workflow failure rate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Workflow failure rate expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_3_workflow_retry_rate() -> None:
    spec = _build_spec(
        "gate_manager_34_3_workflow_retry_rate",
        "34.3 Workflow-Level Stats - Workflow retry rate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Workflow retry rate expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_4_floating_point_precision() -> None:
    spec = _build_spec(
        "gate_manager_34_4_floating_point_precision",
        "34.4 Stats Accuracy - Floating point precision",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Floating point precision expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_4_counter_overflow() -> None:
    spec = _build_spec(
        "gate_manager_34_4_counter_overflow",
        "34.4 Stats Accuracy - Counter overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Counter overflow expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_4_rate_calculation_accuracy() -> None:
    spec = _build_spec(
        "gate_manager_34_4_rate_calculation_accuracy",
        "34.4 Stats Accuracy - Rate calculation accuracy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Rate calculation accuracy expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_34_4_percentile_accuracy() -> None:
    spec = _build_spec(
        "gate_manager_34_4_percentile_accuracy",
        "34.4 Stats Accuracy - Percentile accuracy",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Percentile accuracy expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_34_1_sub_millisecond_actions()
    await validate_34_1_very_long_actions()
    await validate_34_1_action_timeout_stats()
    await validate_34_1_action_retry_stats()
    await validate_34_2_vu_ramp_up_stats()
    await validate_34_2_vu_ramp_down_stats()
    await validate_34_2_vu_iteration_stats()
    await validate_34_2_vu_error_rate()
    await validate_34_3_workflow_duration_histogram()
    await validate_34_3_workflow_throughput()
    await validate_34_3_workflow_failure_rate()
    await validate_34_3_workflow_retry_rate()
    await validate_34_4_floating_point_precision()
    await validate_34_4_counter_overflow()
    await validate_34_4_rate_calculation_accuracy()
    await validate_34_4_percentile_accuracy()


if __name__ == "__main__":
    asyncio.run(run())
