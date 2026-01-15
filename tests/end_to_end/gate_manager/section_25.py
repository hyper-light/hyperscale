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


async def validate_25_1_job_created_us_dispatched_asia() -> None:
    spec = _build_spec(
        "gate_manager_25_1_job_created_us_dispatched_asia",
        "25.1 Job State Consistency - Job created in US, dispatched to Asia",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_submissions, dict), (
            "Job created in US expected job submissions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_25_1_job_cancelled_europe() -> None:
    spec = _build_spec(
        "gate_manager_25_1_job_cancelled_europe",
        "25.1 Job State Consistency - Job cancelled in Europe, running in US",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._cancellation_errors, dict), (
            "Job cancelled in Europe expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_25_1_job_completes_asia_gate_us() -> None:
    spec = _build_spec(
        "gate_manager_25_1_job_completes_asia_gate_us",
        "25.1 Job State Consistency - Job completes in Asia, gate in US",
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
            "Job completes in Asia expected workflow results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_25_2_new_gate_joins_europe() -> None:
    spec = _build_spec(
        "gate_manager_25_2_new_gate_joins_europe",
        "25.2 Membership Consistency - New gate joins in Europe",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._dead_gate_timestamps, dict), (
            "New gate joins expected gate peer tracking"
        )
    finally:
        await runtime.stop_cluster()


async def validate_25_2_worker_joins_asia() -> None:
    spec = _build_spec(
        "gate_manager_25_2_worker_joins_asia",
        "25.2 Membership Consistency - Worker joins in Asia",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Worker joins expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_25_2_manager_dies_us() -> None:
    spec = _build_spec(
        "gate_manager_25_2_manager_dies_us",
        "25.2 Membership Consistency - Manager dies in US",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_health, dict), (
            "Manager dies expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_25_3_rate_limit_change() -> None:
    spec = _build_spec(
        "gate_manager_25_3_rate_limit_change",
        "25.3 Configuration Consistency - Rate limit change",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, "Rate limit change expected rate limiter"
    finally:
        await runtime.stop_cluster()


async def validate_25_3_dc_capacity_update() -> None:
    spec = _build_spec(
        "gate_manager_25_3_dc_capacity_update",
        "25.3 Configuration Consistency - DC capacity update",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._capacity_aggregator is not None, (
            "DC capacity update expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_25_3_feature_flag_change() -> None:
    spec = _build_spec(
        "gate_manager_25_3_feature_flag_change",
        "25.3 Configuration Consistency - Feature flag change",
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
            "Feature flag change expected negotiated caps"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_25_1_job_created_us_dispatched_asia()
    await validate_25_1_job_cancelled_europe()
    await validate_25_1_job_completes_asia_gate_us()
    await validate_25_2_new_gate_joins_europe()
    await validate_25_2_worker_joins_asia()
    await validate_25_2_manager_dies_us()
    await validate_25_3_rate_limit_change()
    await validate_25_3_dc_capacity_update()
    await validate_25_3_feature_flag_change()


if __name__ == "__main__":
    asyncio.run(run())
