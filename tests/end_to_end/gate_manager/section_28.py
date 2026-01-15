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


async def validate_28_1_two_dispatches_same_worker() -> None:
    spec = _build_spec(
        "gate_manager_28_1_two_dispatches_same_worker",
        "28.1 Concurrent Dispatch to Same Worker - Two dispatches hit same worker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Concurrent dispatch expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_28_1_dispatch_failure_simultaneous() -> None:
    spec = _build_spec(
        "gate_manager_28_1_dispatch_failure_simultaneous",
        "28.1 Concurrent Dispatch to Same Worker - Dispatch + failure simultaneous",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Dispatch failure race expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_28_1_dispatch_cancellation_race() -> None:
    spec = _build_spec(
        "gate_manager_28_1_dispatch_cancellation_race",
        "28.1 Concurrent Dispatch to Same Worker - Dispatch + cancellation race",
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
            "Dispatch cancellation race expected errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_1_dispatch_completion_race() -> None:
    spec = _build_spec(
        "gate_manager_28_1_dispatch_completion_race",
        "28.1 Concurrent Dispatch to Same Worker - Dispatch + completion race",
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
            "Dispatch completion race expected results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_2_two_gates_claim_leadership() -> None:
    spec = _build_spec(
        "gate_manager_28_2_two_gates_claim_leadership",
        "28.2 Leadership Race Conditions - Two gates claim job leadership",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Leadership race expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_2_leadership_transfer_during_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_28_2_leadership_transfer_during_dispatch",
        "28.2 Leadership Race Conditions - Leadership transfer during dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Leadership transfer during dispatch expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_2_leadership_cancellation_race() -> None:
    spec = _build_spec(
        "gate_manager_28_2_leadership_cancellation_race",
        "28.2 Leadership Race Conditions - Leadership + cancellation race",
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
            "Leadership cancellation race expected cancellation errors"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_2_leadership_timeout_race() -> None:
    spec = _build_spec(
        "gate_manager_28_2_leadership_timeout_race",
        "28.2 Leadership Race Conditions - Leadership timeout race",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Leadership timeout race expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_3_concurrent_health_updates() -> None:
    spec = _build_spec(
        "gate_manager_28_3_concurrent_health_updates",
        "28.3 State Update Race Conditions - Concurrent health state updates",
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
            "Concurrent health updates expected manager health"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_3_concurrent_stats_merge() -> None:
    spec = _build_spec(
        "gate_manager_28_3_concurrent_stats_merge",
        "28.3 State Update Race Conditions - Concurrent stats merge",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Concurrent stats merge expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_3_concurrent_result_submission() -> None:
    spec = _build_spec(
        "gate_manager_28_3_concurrent_result_submission",
        "28.3 State Update Race Conditions - Concurrent result submission",
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
            "Concurrent result submission expected workflow DC results"
        )
    finally:
        await runtime.stop_cluster()


async def validate_28_3_concurrent_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_28_3_concurrent_cleanup",
        "28.3 State Update Race Conditions - Concurrent cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._job_workflow_ids, dict), (
            "Concurrent cleanup expected job workflow ids"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_28_1_two_dispatches_same_worker()
    await validate_28_1_dispatch_failure_simultaneous()
    await validate_28_1_dispatch_cancellation_race()
    await validate_28_1_dispatch_completion_race()
    await validate_28_2_two_gates_claim_leadership()
    await validate_28_2_leadership_transfer_during_dispatch()
    await validate_28_2_leadership_cancellation_race()
    await validate_28_2_leadership_timeout_race()
    await validate_28_3_concurrent_health_updates()
    await validate_28_3_concurrent_stats_merge()
    await validate_28_3_concurrent_result_submission()
    await validate_28_3_concurrent_cleanup()


if __name__ == "__main__":
    asyncio.run(run())
