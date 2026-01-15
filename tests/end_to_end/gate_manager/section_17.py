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


async def validate_17_1_manager_advertises_capabilities() -> None:
    spec = _build_spec(
        "gate_manager_17_1_manager_advertises_capabilities",
        "17.1 Capability Negotiation - Manager advertises capabilities",
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
            "Manager advertises capabilities expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_17_1_negotiate_common_capabilities() -> None:
    spec = _build_spec(
        "gate_manager_17_1_negotiate_common_capabilities",
        "17.1 Capability Negotiation - Negotiate common capabilities",
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
            "Negotiate common capabilities expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_17_1_store_negotiated_caps() -> None:
    spec = _build_spec(
        "gate_manager_17_1_store_negotiated_caps",
        "17.1 Capability Negotiation - Store negotiated caps",
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
            "Store negotiated caps expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_17_2_same_version() -> None:
    spec = _build_spec(
        "gate_manager_17_2_same_version",
        "17.2 Version Compatibility - Same version",
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
            "Same version expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_17_2_older_manager() -> None:
    spec = _build_spec(
        "gate_manager_17_2_older_manager",
        "17.2 Version Compatibility - Older manager",
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
            "Older manager expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_17_2_newer_manager() -> None:
    spec = _build_spec(
        "gate_manager_17_2_newer_manager",
        "17.2 Version Compatibility - Newer manager",
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
            "Newer manager expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_17_2_feature_checking() -> None:
    spec = _build_spec(
        "gate_manager_17_2_feature_checking",
        "17.2 Version Compatibility - Feature checking",
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
            "Feature checking expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_17_1_manager_advertises_capabilities()
    await validate_17_1_negotiate_common_capabilities()
    await validate_17_1_store_negotiated_caps()
    await validate_17_2_same_version()
    await validate_17_2_older_manager()
    await validate_17_2_newer_manager()
    await validate_17_2_feature_checking()


if __name__ == "__main__":
    asyncio.run(run())
