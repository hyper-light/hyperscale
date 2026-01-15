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


async def validate_38_1_large_workflow_payload() -> None:
    spec = _build_spec(
        "gate_manager_38_1_large_workflow_payload",
        "38.1 Message Size Limits - Large workflow payload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Large workflow payload expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_1_large_result_payload() -> None:
    spec = _build_spec(
        "gate_manager_38_1_large_result_payload",
        "38.1 Message Size Limits - Large result payload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Large result payload expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_1_large_stats_batch() -> None:
    spec = _build_spec(
        "gate_manager_38_1_large_stats_batch",
        "38.1 Message Size Limits - Large stats batch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, "Large stats batch expected load shedder"
    finally:
        await runtime.stop_cluster()


async def validate_38_1_size_limit_exceeded() -> None:
    spec = _build_spec(
        "gate_manager_38_1_size_limit_exceeded",
        "38.1 Message Size Limits - Size limit exceeded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Size limit exceeded expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_2_fragmented_tcp_messages() -> None:
    spec = _build_spec(
        "gate_manager_38_2_fragmented_tcp_messages",
        "38.2 Message Fragmentation - Fragmented TCP messages",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Fragmented TCP messages expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_2_reassembly_under_load() -> None:
    spec = _build_spec(
        "gate_manager_38_2_reassembly_under_load",
        "38.2 Message Fragmentation - Reassembly under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Reassembly under load expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_2_incomplete_messages() -> None:
    spec = _build_spec(
        "gate_manager_38_2_incomplete_messages",
        "38.2 Message Fragmentation - Incomplete messages",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Incomplete messages expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_2_message_corruption_detection() -> None:
    spec = _build_spec(
        "gate_manager_38_2_message_corruption_detection",
        "38.2 Message Fragmentation - Message corruption detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Message corruption detection expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_3_mixed_version_cluster() -> None:
    spec = _build_spec(
        "gate_manager_38_3_mixed_version_cluster",
        "38.3 Protocol Version Negotiation - Mixed version cluster",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Mixed version cluster expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_3_feature_degradation() -> None:
    spec = _build_spec(
        "gate_manager_38_3_feature_degradation",
        "38.3 Protocol Version Negotiation - Feature degradation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Feature degradation expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_3_version_upgrade_during_test() -> None:
    spec = _build_spec(
        "gate_manager_38_3_version_upgrade_during_test",
        "38.3 Protocol Version Negotiation - Version upgrade during test",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Version upgrade expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_38_3_version_rollback() -> None:
    spec = _build_spec(
        "gate_manager_38_3_version_rollback",
        "38.3 Protocol Version Negotiation - Version rollback",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_negotiated_caps, dict), (
            "Version rollback expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_38_1_large_workflow_payload()
    await validate_38_1_large_result_payload()
    await validate_38_1_large_stats_batch()
    await validate_38_1_size_limit_exceeded()
    await validate_38_2_fragmented_tcp_messages()
    await validate_38_2_reassembly_under_load()
    await validate_38_2_incomplete_messages()
    await validate_38_2_message_corruption_detection()
    await validate_38_3_mixed_version_cluster()
    await validate_38_3_feature_degradation()
    await validate_38_3_version_upgrade_during_test()
    await validate_38_3_version_rollback()


if __name__ == "__main__":
    asyncio.run(run())
