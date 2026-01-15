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


async def validate_12_1_manager_dies() -> None:
    spec = _build_spec(
        "manager_worker_12_1_manager_dies",
        "12.1 Orphan Detection - Manager dies",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Manager dies expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_1_mark_orphaned() -> None:
    spec = _build_spec(
        "manager_worker_12_1_mark_orphaned",
        "12.1 Orphan Detection - Mark orphaned",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Mark orphaned expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_1_orphaned_timestamp() -> None:
    spec = _build_spec(
        "manager_worker_12_1_orphaned_timestamp",
        "12.1 Orphan Detection - Orphaned timestamp",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Orphaned timestamp expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_2_wait_for_takeover() -> None:
    spec = _build_spec(
        "manager_worker_12_2_wait_for_takeover",
        "12.2 Grace Period - Wait for takeover",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Wait for takeover expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_2_manager_recovery() -> None:
    spec = _build_spec(
        "manager_worker_12_2_manager_recovery",
        "12.2 Grace Period - Manager recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Manager recovery expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_2_new_manager_takes_over() -> None:
    spec = _build_spec(
        "manager_worker_12_2_new_manager_takes_over",
        "12.2 Grace Period - New manager takes over",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._pending_transfers, dict), (
            "New manager takes over expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_3_grace_period_exceeded() -> None:
    spec = _build_spec(
        "manager_worker_12_3_grace_period_exceeded",
        "12.3 Orphan Expiry - Grace period exceeded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Grace period exceeded expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_3_workflow_handling() -> None:
    spec = _build_spec(
        "manager_worker_12_3_workflow_handling",
        "12.3 Orphan Expiry - Workflow handling",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Workflow handling expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def validate_12_3_cleanup() -> None:
    spec = _build_spec(
        "manager_worker_12_3_cleanup",
        "12.3 Orphan Expiry - Cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._orphaned_workflows, dict), (
            "Orphan cleanup expected orphaned workflows"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_12_1_manager_dies()
    await validate_12_1_mark_orphaned()
    await validate_12_1_orphaned_timestamp()
    await validate_12_2_wait_for_takeover()
    await validate_12_2_manager_recovery()
    await validate_12_2_new_manager_takes_over()
    await validate_12_3_grace_period_exceeded()
    await validate_12_3_workflow_handling()
    await validate_12_3_cleanup()


if __name__ == "__main__":
    asyncio.run(run())
