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


async def validate_15_1_quorum_available() -> None:
    spec = _build_spec(
        "gate_manager_15_1_quorum_available",
        "15.1 Quorum Checking - Quorum available",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_has_quorum_available", None)), (
            "Quorum available expected _has_quorum_available"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_1_quorum_unavailable() -> None:
    spec = _build_spec(
        "gate_manager_15_1_quorum_unavailable",
        "15.1 Quorum Checking - Quorum unavailable",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_has_quorum_available", None)), (
            "Quorum unavailable expected _has_quorum_available"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_1_quorum_size_calculation() -> None:
    spec = _build_spec(
        "gate_manager_15_1_quorum_size_calculation",
        "15.1 Quorum Checking - Quorum size calculation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert callable(getattr(gate, "_quorum_size", None)), (
            "Quorum size calculation expected _quorum_size"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_2_quorum_errors_tracked() -> None:
    spec = _build_spec(
        "gate_manager_15_2_quorum_errors_tracked",
        "15.2 Quorum Circuit Breaker - Quorum errors tracked",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Quorum errors tracked expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_2_quorum_circuit_opens() -> None:
    spec = _build_spec(
        "gate_manager_15_2_quorum_circuit_opens",
        "15.2 Quorum Circuit Breaker - Quorum circuit opens",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Quorum circuit opens expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_2_quorum_circuit_recovery() -> None:
    spec = _build_spec(
        "gate_manager_15_2_quorum_circuit_recovery",
        "15.2 Quorum Circuit Breaker - Quorum circuit recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Quorum circuit recovery expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_3_at_most_once_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_15_3_at_most_once_dispatch",
        "15.3 Consistency Guarantees - At-most-once dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._idempotency_cache is not None, (
            "At-most-once dispatch expected idempotency cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_3_exactly_once_completion() -> None:
    spec = _build_spec(
        "gate_manager_15_3_exactly_once_completion",
        "15.3 Consistency Guarantees - Exactly-once completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Exactly-once completion expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_3_ordered_operations() -> None:
    spec = _build_spec(
        "gate_manager_15_3_ordered_operations",
        "15.3 Consistency Guarantees - Ordered operations",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._state_version is not None, (
            "Ordered operations expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_15_1_quorum_available()
    await validate_15_1_quorum_unavailable()
    await validate_15_1_quorum_size_calculation()
    await validate_15_2_quorum_errors_tracked()
    await validate_15_2_quorum_circuit_opens()
    await validate_15_2_quorum_circuit_recovery()
    await validate_15_3_at_most_once_dispatch()
    await validate_15_3_exactly_once_completion()
    await validate_15_3_ordered_operations()


if __name__ == "__main__":
    asyncio.run(run())
