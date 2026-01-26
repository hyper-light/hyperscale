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


async def validate_30_1_manager_dies_under_load() -> None:
    spec = _build_spec(
        "gate_manager_30_1_manager_dies_under_load",
        "30.1 Component Failure Under Load - Manager dies with 1000 active workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_dc_managers, dict), (
            "Manager dies under load expected job DC managers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_1_gate_dies_under_load() -> None:
    spec = _build_spec(
        "gate_manager_30_1_gate_dies_under_load",
        "30.1 Component Failure Under Load - Gate dies with 500 jobs in progress",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Gate dies under load expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_1_worker_dies_under_load() -> None:
    spec = _build_spec(
        "gate_manager_30_1_worker_dies_under_load",
        "30.1 Component Failure Under Load - Worker dies with 100 VUs running",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Worker dies under load expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_1_network_partition_during_burst() -> None:
    spec = _build_spec(
        "gate_manager_30_1_network_partition_during_burst",
        "30.1 Component Failure Under Load - Network partition during burst",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Network partition expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_2_manager_failure_overload() -> None:
    spec = _build_spec(
        "gate_manager_30_2_manager_failure_overload",
        "30.2 Cascading Failures - One manager fails, others overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Manager failure overload expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_2_worker_death_spiral() -> None:
    spec = _build_spec(
        "gate_manager_30_2_worker_death_spiral",
        "30.2 Cascading Failures - Worker death spiral",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Worker death spiral expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_2_gate_quorum_loss_under_load() -> None:
    spec = _build_spec(
        "gate_manager_30_2_gate_quorum_loss_under_load",
        "30.2 Cascading Failures - Gate quorum loss under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Gate quorum loss expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_2_circuit_breaker_cascade() -> None:
    spec = _build_spec(
        "gate_manager_30_2_circuit_breaker_cascade",
        "30.2 Cascading Failures - Circuit breaker cascade",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Circuit breaker cascade expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_3_manager_recovers_under_load() -> None:
    spec = _build_spec(
        "gate_manager_30_3_manager_recovers_under_load",
        "30.3 Recovery Under Load - Manager recovers during high load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Manager recovery expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_3_worker_recovers_pending_results() -> None:
    spec = _build_spec(
        "gate_manager_30_3_worker_recovers_pending_results",
        "30.3 Recovery Under Load - Worker recovers with pending results",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._workflow_dc_results, dict), (
            "Worker recovers expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_3_gate_recovers_jobs_in_flight() -> None:
    spec = _build_spec(
        "gate_manager_30_3_gate_recovers_jobs_in_flight",
        "30.3 Recovery Under Load - Gate recovers with jobs in flight",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Gate recovery expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_30_3_network_heals_backlog() -> None:
    spec = _build_spec(
        "gate_manager_30_3_network_heals_backlog",
        "30.3 Recovery Under Load - Network heals with message backlog",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Network heals expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_30_1_manager_dies_under_load()
    await validate_30_1_gate_dies_under_load()
    await validate_30_1_worker_dies_under_load()
    await validate_30_1_network_partition_during_burst()
    await validate_30_2_manager_failure_overload()
    await validate_30_2_worker_death_spiral()
    await validate_30_2_gate_quorum_loss_under_load()
    await validate_30_2_circuit_breaker_cascade()
    await validate_30_3_manager_recovers_under_load()
    await validate_30_3_worker_recovers_pending_results()
    await validate_30_3_gate_recovers_jobs_in_flight()
    await validate_30_3_network_heals_backlog()


if __name__ == "__main__":
    asyncio.run(run())
