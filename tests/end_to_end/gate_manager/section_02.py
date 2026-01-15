import asyncio
import re

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
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


def _get_gate(runtime):
    cluster = runtime.require_cluster()
    return cluster.get_gate_leader() or cluster.gates[0]


def _require_runtime(outcome):
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def validate_2_1_manager_registers_with_gate() -> None:
    spec = _build_spec(
        "gate_manager_2_1_manager_registers_with_gate",
        "2.1 Registration Flow - Manager registers with gate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert "DC-A" in state._datacenter_manager_status, (
            "Manager registration expected DC-A in _datacenter_manager_status"
        )
        assert state._datacenter_manager_status["DC-A"], (
            "Manager registration expected DC-A manager status entries"
        )
        assert state._manager_health, (
            "Manager registration expected _manager_health entries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_registration_with_capabilities() -> None:
    spec = _build_spec(
        "gate_manager_2_1_registration_with_capabilities",
        "2.1 Registration Flow - Registration with capabilities",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._manager_negotiated_caps, (
            "Registration with capabilities expected _manager_negotiated_caps"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_registration_from_unknown_dc() -> None:
    spec = _build_spec(
        "gate_manager_2_1_registration_from_unknown_dc",
        "2.1 Registration Flow - Registration from unknown DC",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert hasattr(state, "_dc_registration_states"), (
            "Unknown DC registration expected _dc_registration_states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_re_registration_after_restart() -> None:
    spec = _build_spec(
        "gate_manager_2_1_re_registration_after_restart",
        "2.1 Registration Flow - Re-registration after restart",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._manager_last_status, (
            "Re-registration expected _manager_last_status entries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_1_role_validation() -> None:
    spec = _build_spec(
        "gate_manager_2_1_role_validation",
        "2.1 Registration Flow - Role validation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_role_validator"), (
            "Role validation expected _role_validator on gate"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_gate_broadcasts_manager_discovery() -> None:
    spec = _build_spec(
        "gate_manager_2_2_gate_broadcasts_manager_discovery",
        "2.2 Discovery Propagation - Gate broadcasts manager discovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_gate_broadcaster"), (
            "Discovery broadcast expected _gate_broadcaster"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_gate_receives_manager_discovery() -> None:
    spec = _build_spec(
        "gate_manager_2_2_gate_receives_manager_discovery",
        "2.2 Discovery Propagation - Gate receives manager discovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert "DC-A" in state._datacenter_manager_status, (
            "Discovery receive expected DC-A manager status"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_discovery_of_known_manager() -> None:
    spec = _build_spec(
        "gate_manager_2_2_discovery_of_known_manager",
        "2.2 Discovery Propagation - Discovery of already-known manager",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._datacenter_manager_status.get("DC-A"), (
            "Discovery of known manager expected existing status entries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_2_discovery_failure_decay() -> None:
    spec = _build_spec(
        "gate_manager_2_2_discovery_failure_decay",
        "2.2 Discovery Propagation - Discovery failure decay",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_discovery_maintenance_task"), (
            "Discovery failure decay expected _discovery_maintenance_task"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_manager_heartbeat_received() -> None:
    spec = _build_spec(
        "gate_manager_2_3_manager_heartbeat_received",
        "2.3 Manager Heartbeats - Manager heartbeat received",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._manager_last_status, (
            "Heartbeat received expected _manager_last_status entries"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_heartbeat_with_state_changes() -> None:
    spec = _build_spec(
        "gate_manager_2_3_heartbeat_with_state_changes",
        "2.3 Manager Heartbeats - Heartbeat with state changes",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._datacenter_manager_status.get("DC-A"), (
            "Heartbeat state changes expected manager status update"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_stale_heartbeat_rejection() -> None:
    spec = _build_spec(
        "gate_manager_2_3_stale_heartbeat_rejection",
        "2.3 Manager Heartbeats - Stale heartbeat rejection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_versioned_clock"), (
            "Stale heartbeat expected _versioned_clock on gate"
        )
    finally:
        await runtime.stop_cluster()


async def validate_2_3_heartbeat_timeout() -> None:
    spec = _build_spec(
        "gate_manager_2_3_heartbeat_timeout",
        "2.3 Manager Heartbeats - Heartbeat timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._manager_health, (
            "Heartbeat timeout expected _manager_health entries"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_2_1_manager_registers_with_gate()
    await validate_2_1_registration_with_capabilities()
    await validate_2_1_registration_from_unknown_dc()
    await validate_2_1_re_registration_after_restart()
    await validate_2_1_role_validation()
    await validate_2_2_gate_broadcasts_manager_discovery()
    await validate_2_2_gate_receives_manager_discovery()
    await validate_2_2_discovery_of_known_manager()
    await validate_2_2_discovery_failure_decay()
    await validate_2_3_manager_heartbeat_received()
    await validate_2_3_heartbeat_with_state_changes()
    await validate_2_3_stale_heartbeat_rejection()
    await validate_2_3_heartbeat_timeout()


if __name__ == "__main__":
    asyncio.run(run())
