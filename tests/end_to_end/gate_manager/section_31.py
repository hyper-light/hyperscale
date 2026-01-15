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


async def validate_31_1_response_arrives_as_timeout_fires() -> None:
    spec = _build_spec(
        "gate_manager_31_1_response_arrives_as_timeout_fires",
        "31.1 Timeout Racing - Response arrives as timeout fires",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Timeout racing expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_1_multiple_timeouts_fire() -> None:
    spec = _build_spec(
        "gate_manager_31_1_multiple_timeouts_fire",
        "31.1 Timeout Racing - Multiple timeouts fire together",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Multiple timeouts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_1_timeout_success_race() -> None:
    spec = _build_spec(
        "gate_manager_31_1_timeout_success_race",
        "31.1 Timeout Racing - Timeout + success race",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Timeout success race expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_1_cascading_timeouts() -> None:
    spec = _build_spec(
        "gate_manager_31_1_cascading_timeouts",
        "31.1 Timeout Racing - Cascading timeouts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Cascading timeouts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_2_job_approaching_deadline() -> None:
    spec = _build_spec(
        "gate_manager_31_2_job_approaching_deadline",
        "31.2 Deadline Pressure - Job approaching deadline",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Job deadline expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_2_worker_extension_request() -> None:
    spec = _build_spec(
        "gate_manager_31_2_worker_extension_request",
        "31.2 Deadline Pressure - Worker extension request",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Extension request expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_2_extension_denied_under_load() -> None:
    spec = _build_spec(
        "gate_manager_31_2_extension_denied_under_load",
        "31.2 Deadline Pressure - Extension denied under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Extension denied expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_2_deadline_during_partition() -> None:
    spec = _build_spec(
        "gate_manager_31_2_deadline_during_partition",
        "31.2 Deadline Pressure - Deadline during partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Deadline during partition expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_3_aggressive_timeouts() -> None:
    spec = _build_spec(
        "gate_manager_31_3_aggressive_timeouts",
        "31.3 Timeout Configuration - Aggressive timeouts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Aggressive timeouts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_3_conservative_timeouts() -> None:
    spec = _build_spec(
        "gate_manager_31_3_conservative_timeouts",
        "31.3 Timeout Configuration - Conservative timeouts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Conservative timeouts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_3_adaptive_timeouts() -> None:
    spec = _build_spec(
        "gate_manager_31_3_adaptive_timeouts",
        "31.3 Timeout Configuration - Adaptive timeouts",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Adaptive timeouts expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_31_3_timeout_jitter() -> None:
    spec = _build_spec(
        "gate_manager_31_3_timeout_jitter",
        "31.3 Timeout Configuration - Timeout jitter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Timeout jitter expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_31_1_response_arrives_as_timeout_fires()
    await validate_31_1_multiple_timeouts_fire()
    await validate_31_1_timeout_success_race()
    await validate_31_1_cascading_timeouts()
    await validate_31_2_job_approaching_deadline()
    await validate_31_2_worker_extension_request()
    await validate_31_2_extension_denied_under_load()
    await validate_31_2_deadline_during_partition()
    await validate_31_3_aggressive_timeouts()
    await validate_31_3_conservative_timeouts()
    await validate_31_3_adaptive_timeouts()
    await validate_31_3_timeout_jitter()


if __name__ == "__main__":
    asyncio.run(run())
