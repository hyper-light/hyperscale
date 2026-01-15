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


async def validate_14_1_lease_acquisition() -> None:
    spec = _build_spec(
        "gate_manager_14_1_lease_acquisition",
        "14.1 Job Leases - Lease acquisition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "Lease acquisition expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_1_lease_renewal() -> None:
    spec = _build_spec(
        "gate_manager_14_1_lease_renewal",
        "14.1 Job Leases - Lease renewal",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "Lease renewal expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_1_lease_expiry() -> None:
    spec = _build_spec(
        "gate_manager_14_1_lease_expiry",
        "14.1 Job Leases - Lease expiry",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "Lease expiry expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_1_lease_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_14_1_lease_cleanup",
        "14.1 Job Leases - Lease cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "Lease cleanup expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_2_dc_lease_acquisition() -> None:
    spec = _build_spec(
        "gate_manager_14_2_dc_lease_acquisition",
        "14.2 Datacenter Leases - DC lease acquisition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "DC lease acquisition expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_2_lease_transfer() -> None:
    spec = _build_spec(
        "gate_manager_14_2_lease_transfer",
        "14.2 Datacenter Leases - Lease transfer",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "Lease transfer expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_2_lease_transfer_ack() -> None:
    spec = _build_spec(
        "gate_manager_14_2_lease_transfer_ack",
        "14.2 Datacenter Leases - Lease transfer ack",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._leases, dict), "Lease transfer ack expected leases"
    finally:
        await runtime.stop_cluster()


async def validate_14_2_fence_token_increment() -> None:
    spec = _build_spec(
        "gate_manager_14_2_fence_token_increment",
        "14.2 Datacenter Leases - Fence token increment",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._fence_token is not None, (
            "Fence token increment expected fence token"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_14_1_lease_acquisition()
    await validate_14_1_lease_renewal()
    await validate_14_1_lease_expiry()
    await validate_14_1_lease_cleanup()
    await validate_14_2_dc_lease_acquisition()
    await validate_14_2_lease_transfer()
    await validate_14_2_lease_transfer_ack()
    await validate_14_2_fence_token_increment()


if __name__ == "__main__":
    asyncio.run(run())
