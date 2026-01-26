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


async def validate_37_1_node_restart_under_load() -> None:
    spec = _build_spec(
        "gate_manager_37_1_node_restart_under_load",
        "37.1 Zombie Detection Under Load - Node restart under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Node restart expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_1_incarnation_validation() -> None:
    spec = _build_spec(
        "gate_manager_37_1_incarnation_validation",
        "37.1 Zombie Detection Under Load - Incarnation validation",
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
            "Incarnation validation expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_1_stale_message_rejection() -> None:
    spec = _build_spec(
        "gate_manager_37_1_stale_message_rejection",
        "37.1 Zombie Detection Under Load - Stale message rejection",
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
            "Stale message rejection expected state version"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_1_death_record_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_37_1_death_record_cleanup",
        "37.1 Zombie Detection Under Load - Death record cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Death record cleanup expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_2_completed_job_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_37_2_completed_job_cleanup",
        "37.2 Stale State Cleanup - Completed job cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_manager is not None, (
            "Completed job cleanup expected job manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_2_orphaned_workflow_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_37_2_orphaned_workflow_cleanup",
        "37.2 Stale State Cleanup - Orphaned workflow cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Orphaned workflow cleanup expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_2_dead_peer_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_37_2_dead_peer_cleanup",
        "37.2 Stale State Cleanup - Dead peer cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Dead peer cleanup expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_2_result_cache_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_37_2_result_cache_cleanup",
        "37.2 Stale State Cleanup - Result cache cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Result cache cleanup expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_3_long_running_test() -> None:
    spec = _build_spec(
        "gate_manager_37_3_long_running_test",
        "37.3 State Accumulation - Long-running test",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Long-running test expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_3_state_growth_monitoring() -> None:
    spec = _build_spec(
        "gate_manager_37_3_state_growth_monitoring",
        "37.3 State Accumulation - State growth monitoring",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "State growth monitoring expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_3_memory_leak_detection() -> None:
    spec = _build_spec(
        "gate_manager_37_3_memory_leak_detection",
        "37.3 State Accumulation - Memory leak detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Memory leak detection expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_37_3_file_descriptor_monitoring() -> None:
    spec = _build_spec(
        "gate_manager_37_3_file_descriptor_monitoring",
        "37.3 State Accumulation - File descriptor monitoring",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "File descriptor monitoring expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_37_1_node_restart_under_load()
    await validate_37_1_incarnation_validation()
    await validate_37_1_stale_message_rejection()
    await validate_37_1_death_record_cleanup()
    await validate_37_2_completed_job_cleanup()
    await validate_37_2_orphaned_workflow_cleanup()
    await validate_37_2_dead_peer_cleanup()
    await validate_37_2_result_cache_cleanup()
    await validate_37_3_long_running_test()
    await validate_37_3_state_growth_monitoring()
    await validate_37_3_memory_leak_detection()
    await validate_37_3_file_descriptor_monitoring()


if __name__ == "__main__":
    asyncio.run(run())
