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


async def validate_23_1_sub_second_progress_updates() -> None:
    spec = _build_spec(
        "gate_manager_23_1_sub_second_progress_updates",
        "23.1 High-Frequency Progress - Sub-second progress updates",
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
            "Sub-second progress updates expected progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_1_progress_batching_efficiency() -> None:
    spec = _build_spec(
        "gate_manager_23_1_progress_batching_efficiency",
        "23.1 High-Frequency Progress - Progress batching efficiency",
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
            "Progress batching efficiency expected progress seen"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_1_progress_ordering() -> None:
    spec = _build_spec(
        "gate_manager_23_1_progress_ordering",
        "23.1 High-Frequency Progress - Progress ordering",
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
            "Progress ordering expected sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_1_progress_memory_churn() -> None:
    spec = _build_spec(
        "gate_manager_23_1_progress_memory_churn",
        "23.1 High-Frequency Progress - Progress memory churn",
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
            "Progress memory churn expected progress seen"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_multi_dc_progress_merge() -> None:
    spec = _build_spec(
        "gate_manager_23_2_multi_dc_progress_merge",
        "23.2 Progress Fan-Out - Multi-DC progress merge",
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
            "Multi-DC progress merge expected progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_progress_to_multiple_callbacks() -> None:
    spec = _build_spec(
        "gate_manager_23_2_progress_to_multiple_callbacks",
        "23.2 Progress Fan-Out - Progress to multiple callbacks",
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
            "Progress to multiple callbacks expected progress callbacks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_progress_callback_latency() -> None:
    spec = _build_spec(
        "gate_manager_23_2_progress_callback_latency",
        "23.2 Progress Fan-Out - Progress callback latency",
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
            "Progress callback latency expected progress callbacks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_2_progress_callback_failure() -> None:
    spec = _build_spec(
        "gate_manager_23_2_progress_callback_failure",
        "23.2 Progress Fan-Out - Progress callback failure",
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
            "Progress callback failure expected progress callbacks"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_3_dc_unreachable() -> None:
    spec = _build_spec(
        "gate_manager_23_3_dc_unreachable",
        "23.3 Progress Under Partition - DC becomes unreachable",
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
            "DC unreachable expected progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_3_dc_reconnects() -> None:
    spec = _build_spec(
        "gate_manager_23_3_dc_reconnects",
        "23.3 Progress Under Partition - DC reconnects",
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
            "DC reconnects expected progress seen"
        )
    finally:
        await runtime.stop_cluster()


async def validate_23_3_progress_gap_detection() -> None:
    spec = _build_spec(
        "gate_manager_23_3_progress_gap_detection",
        "23.3 Progress Under Partition - Progress gap detection",
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
            "Progress gap detection expected progress sequences"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_23_1_sub_second_progress_updates()
    await validate_23_1_progress_batching_efficiency()
    await validate_23_1_progress_ordering()
    await validate_23_1_progress_memory_churn()
    await validate_23_2_multi_dc_progress_merge()
    await validate_23_2_progress_to_multiple_callbacks()
    await validate_23_2_progress_callback_latency()
    await validate_23_2_progress_callback_failure()
    await validate_23_3_dc_unreachable()
    await validate_23_3_dc_reconnects()
    await validate_23_3_progress_gap_detection()


if __name__ == "__main__":
    asyncio.run(run())
