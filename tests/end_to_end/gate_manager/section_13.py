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


async def validate_13_1_gate_assumes_leadership() -> None:
    spec = _build_spec(
        "gate_manager_13_1_gate_assumes_leadership",
        "13.1 Job Leadership Tracking - Gate assumes leadership",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_leadership_tracker
        assert callable(getattr(tracker, "assume_leadership", None)), (
            "Gate assumes leadership expected assume_leadership"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_1_leadership_broadcast() -> None:
    spec = _build_spec(
        "gate_manager_13_1_leadership_broadcast",
        "13.1 Job Leadership Tracking - Leadership broadcast",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_broadcast_job_leadership", None)), (
            "Leadership broadcast expected _broadcast_job_leadership"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_1_leadership_notification_received() -> None:
    spec = _build_spec(
        "gate_manager_13_1_leadership_notification_received",
        "13.1 Job Leadership Tracking - Leadership notification received",
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
            "Leadership notification expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_1_leadership_query() -> None:
    spec = _build_spec(
        "gate_manager_13_1_leadership_query",
        "13.1 Job Leadership Tracking - Leadership query",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_leadership_tracker
        assert callable(getattr(tracker, "is_leader", None)), (
            "Leadership query expected is_leader"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_2_gate_leader_dies() -> None:
    spec = _build_spec(
        "gate_manager_13_2_gate_leader_dies",
        "13.2 Leadership Transfers (Gate-to-Gate) - Gate leader dies",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_handle_job_leader_failure", None)), (
            "Gate leader dies expected _handle_job_leader_failure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_2_leadership_takeover() -> None:
    spec = _build_spec(
        "gate_manager_13_2_leadership_takeover",
        "13.2 Leadership Transfers (Gate-to-Gate) - Leadership takeover",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_leadership_tracker
        assert tracker is not None, "Leadership takeover expected leadership tracker"
    finally:
        await runtime.stop_cluster()


async def validate_13_2_transfer_acknowledgment() -> None:
    spec = _build_spec(
        "gate_manager_13_2_transfer_acknowledgment",
        "13.2 Leadership Transfers (Gate-to-Gate) - Transfer acknowledgment",
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
            "Transfer acknowledgment expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_3_manager_leader_transfer() -> None:
    spec = _build_spec(
        "gate_manager_13_3_manager_leader_transfer",
        "13.3 Leadership Transfers (Manager-Level) - Manager leader transfer",
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
            "Manager leader transfer expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_3_manager_leader_ack() -> None:
    spec = _build_spec(
        "gate_manager_13_3_manager_leader_ack",
        "13.3 Leadership Transfers (Manager-Level) - Manager leader ack",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        tracker = gate._job_leadership_tracker
        assert tracker is not None, "Manager leader ack expected leadership tracker"
    finally:
        await runtime.stop_cluster()


async def validate_13_3_manager_leader_notification() -> None:
    spec = _build_spec(
        "gate_manager_13_3_manager_leader_notification",
        "13.3 Leadership Transfers (Manager-Level) - Manager leader notification",
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
            "Manager leader notification expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_job_leader_gate_dies() -> None:
    spec = _build_spec(
        "gate_manager_13_4_job_leader_gate_dies",
        "13.4 Orphan Job Handling - Job leader gate dies",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._orphaned_jobs, dict), (
            "Job leader gate dies expected orphaned jobs"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_orphan_grace_period() -> None:
    spec = _build_spec(
        "gate_manager_13_4_orphan_grace_period",
        "13.4 Orphan Job Handling - Orphan grace period",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._orphaned_jobs, dict), (
            "Orphan grace period expected orphaned jobs"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_orphan_job_takeover() -> None:
    spec = _build_spec(
        "gate_manager_13_4_orphan_job_takeover",
        "13.4 Orphan Job Handling - Orphan job takeover",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._orphaned_jobs, dict), (
            "Orphan job takeover expected orphaned jobs"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_orphan_job_timeout() -> None:
    spec = _build_spec(
        "gate_manager_13_4_orphan_job_timeout",
        "13.4 Orphan Job Handling - Orphan job timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._orphaned_jobs, dict), (
            "Orphan job timeout expected orphaned jobs"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_13_1_gate_assumes_leadership()
    await validate_13_1_leadership_broadcast()
    await validate_13_1_leadership_notification_received()
    await validate_13_1_leadership_query()
    await validate_13_2_gate_leader_dies()
    await validate_13_2_leadership_takeover()
    await validate_13_2_transfer_acknowledgment()
    await validate_13_3_manager_leader_transfer()
    await validate_13_3_manager_leader_ack()
    await validate_13_3_manager_leader_notification()
    await validate_13_4_job_leader_gate_dies()
    await validate_13_4_orphan_grace_period()
    await validate_13_4_orphan_job_takeover()
    await validate_13_4_orphan_job_timeout()


if __name__ == "__main__":
    asyncio.run(run())
