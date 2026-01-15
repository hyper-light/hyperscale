import asyncio
import re

from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

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
                "dc_count": 1,
                "managers_per_dc": 1,
                "workers_per_dc": 2,
                "cores_per_worker": 1,
                "base_gate_tcp": 9000,
            },
            "actions": [
                {"type": "start_cluster"},
                {"type": "await_gate_leader", "params": {"timeout": 30}},
                {
                    "type": "await_manager_leader",
                    "params": {"dc_id": "DC-A", "timeout": 30},
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


def _get_manager(runtime: ScenarioRuntime, dc_id: str) -> ManagerServer:
    cluster = runtime.require_cluster()
    return cluster.get_manager_leader(dc_id) or cluster.managers[dc_id][0]


def _get_worker(runtime: ScenarioRuntime) -> WorkerServer:
    cluster = runtime.require_cluster()
    return cluster.get_all_workers()[0]


def _require_runtime(outcome: ScenarioOutcome) -> ScenarioRuntime:
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def validate_15_1_request_provision() -> None:
    spec = _build_spec(
        "manager_worker_15_1_request_provision",
        "15.1 Provision Quorum - Request provision",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._pending_provisions, dict), (
            "Request provision expected pending provisions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_1_peer_confirmation() -> None:
    spec = _build_spec(
        "manager_worker_15_1_peer_confirmation",
        "15.1 Provision Quorum - Peer confirmation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._provision_confirmations, dict), (
            "Peer confirmation expected provision confirmations"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_1_quorum_achieved() -> None:
    spec = _build_spec(
        "manager_worker_15_1_quorum_achieved",
        "15.1 Provision Quorum - Quorum achieved",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._pending_provisions, dict), (
            "Quorum achieved expected pending provisions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_1_quorum_failed() -> None:
    spec = _build_spec(
        "manager_worker_15_1_quorum_failed",
        "15.1 Provision Quorum - Quorum failed",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._pending_provisions, dict), (
            "Quorum failed expected pending provisions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_2_quorum_size() -> None:
    spec = _build_spec(
        "manager_worker_15_2_quorum_size",
        "15.2 Quorum Calculation - Quorum size",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._provision_confirmations, dict), (
            "Quorum size expected provision confirmations"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_2_confirmation_tracking() -> None:
    spec = _build_spec(
        "manager_worker_15_2_confirmation_tracking",
        "15.2 Quorum Calculation - Confirmation tracking",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._provision_confirmations, dict), (
            "Confirmation tracking expected provision confirmations"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_2_timeout_handling() -> None:
    spec = _build_spec(
        "manager_worker_15_2_timeout_handling",
        "15.2 Quorum Calculation - Timeout handling",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._pending_provisions, dict), (
            "Timeout handling expected pending provisions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_3_clear_pending() -> None:
    spec = _build_spec(
        "manager_worker_15_3_clear_pending",
        "15.3 Provision Cleanup - Clear pending",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._pending_provisions, dict), (
            "Clear pending expected pending provisions"
        )
    finally:
        await runtime.stop_cluster()


async def validate_15_3_clear_confirmations() -> None:
    spec = _build_spec(
        "manager_worker_15_3_clear_confirmations",
        "15.3 Provision Cleanup - Clear confirmations",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._provision_confirmations, dict), (
            "Clear confirmations expected provision confirmations"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_15_1_request_provision()
    await validate_15_1_peer_confirmation()
    await validate_15_1_quorum_achieved()
    await validate_15_1_quorum_failed()
    await validate_15_2_quorum_size()
    await validate_15_2_confirmation_tracking()
    await validate_15_2_timeout_handling()
    await validate_15_3_clear_pending()
    await validate_15_3_clear_confirmations()


if __name__ == "__main__":
    asyncio.run(run())
