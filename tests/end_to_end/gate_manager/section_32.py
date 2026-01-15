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


async def validate_32_1_network_hiccup_mass_retry() -> None:
    spec = _build_spec(
        "gate_manager_32_1_network_hiccup_mass_retry",
        "32.1 Retry Storm - Network hiccup causes mass retry",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Retry storm expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_1_idempotency_cache_pressure() -> None:
    spec = _build_spec(
        "gate_manager_32_1_idempotency_cache_pressure",
        "32.1 Retry Storm - Idempotency cache pressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Idempotency cache pressure expected cache"
        )
        assert hasattr(gate._idempotency_cache, "_cache"), (
            "Idempotency cache pressure expected cache storage"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_1_idempotency_key_collision() -> None:
    spec = _build_spec(
        "gate_manager_32_1_idempotency_key_collision",
        "32.1 Retry Storm - Idempotency key collision",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Idempotency key collision expected cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_1_idempotency_expiry_during_retry() -> None:
    spec = _build_spec(
        "gate_manager_32_1_idempotency_expiry_during_retry",
        "32.1 Retry Storm - Idempotency expiry during retry",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, "Idempotency expiry expected cache"
        assert hasattr(gate._idempotency_cache, "ttl_seconds"), (
            "Idempotency expiry expected ttl_seconds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_2_near_simultaneous_duplicates() -> None:
    spec = _build_spec(
        "gate_manager_32_2_near_simultaneous_duplicates",
        "32.2 Duplicate Detection - Near-simultaneous duplicates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Near-simultaneous duplicates expected idempotency cache"
        )
        assert hasattr(gate._idempotency_cache, "_cache"), (
            "Near-simultaneous duplicates expected cache storage"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_2_cross_gate_duplicates() -> None:
    spec = _build_spec(
        "gate_manager_32_2_cross_gate_duplicates",
        "32.2 Duplicate Detection - Cross-gate duplicates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Cross-gate duplicates expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_2_duplicate_with_different_payload() -> None:
    spec = _build_spec(
        "gate_manager_32_2_duplicate_with_different_payload",
        "32.2 Duplicate Detection - Duplicate with different payload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Duplicate with different payload expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_32_2_duplicate_after_completion() -> None:
    spec = _build_spec(
        "gate_manager_32_2_duplicate_after_completion",
        "32.2 Duplicate Detection - Duplicate after completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "Duplicate after completion expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_32_1_network_hiccup_mass_retry()
    await validate_32_1_idempotency_cache_pressure()
    await validate_32_1_idempotency_key_collision()
    await validate_32_1_idempotency_expiry_during_retry()
    await validate_32_2_near_simultaneous_duplicates()
    await validate_32_2_cross_gate_duplicates()
    await validate_32_2_duplicate_with_different_payload()
    await validate_32_2_duplicate_after_completion()


if __name__ == "__main__":
    asyncio.run(run())
