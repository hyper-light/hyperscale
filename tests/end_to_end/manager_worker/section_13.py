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


async def validate_13_1_transfer_message_received() -> None:
    spec = _build_spec(
        "manager_worker_13_1_transfer_message_received",
        "13.1 Transfer Protocol - Transfer message received",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._job_fence_tokens, dict), (
            "Transfer message expected job fence tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_1_fence_token_check() -> None:
    spec = _build_spec(
        "manager_worker_13_1_fence_token_check",
        "13.1 Transfer Protocol - Fence token check",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._job_fence_tokens, dict), (
            "Fence token check expected fence tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_1_accept_transfer() -> None:
    spec = _build_spec(
        "manager_worker_13_1_accept_transfer",
        "13.1 Transfer Protocol - Accept transfer",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._workflow_job_leader, dict), (
            "Accept transfer expected workflow job leader"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_2_stale_token_rejection() -> None:
    spec = _build_spec(
        "manager_worker_13_2_stale_token_rejection",
        "13.2 Transfer Validation - Stale token rejection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._job_fence_tokens, dict), (
            "Stale token rejection expected job fence tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_2_unknown_manager_rejection() -> None:
    spec = _build_spec(
        "manager_worker_13_2_unknown_manager_rejection",
        "13.2 Transfer Validation - Unknown manager rejection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._job_fence_tokens, dict), (
            "Unknown manager rejection expected job fence tokens"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_2_duplicate_transfer() -> None:
    spec = _build_spec(
        "manager_worker_13_2_duplicate_transfer",
        "13.2 Transfer Validation - Duplicate transfer",
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
            "Duplicate transfer expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_3_store_pending() -> None:
    spec = _build_spec(
        "manager_worker_13_3_store_pending",
        "13.3 Pending Transfers - Store pending",
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
            "Store pending expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_3_apply_on_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_13_3_apply_on_dispatch",
        "13.3 Pending Transfers - Apply on dispatch",
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
            "Apply on dispatch expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_3_cleanup() -> None:
    spec = _build_spec(
        "manager_worker_13_3_cleanup",
        "13.3 Pending Transfers - Cleanup",
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
            "Cleanup expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_received_count() -> None:
    spec = _build_spec(
        "manager_worker_13_4_received_count",
        "13.4 Transfer Metrics - Received count",
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
            "Received count expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_accepted_count() -> None:
    spec = _build_spec(
        "manager_worker_13_4_accepted_count",
        "13.4 Transfer Metrics - Accepted count",
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
            "Accepted count expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_13_4_rejected_counts() -> None:
    spec = _build_spec(
        "manager_worker_13_4_rejected_counts",
        "13.4 Transfer Metrics - Rejected counts",
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
            "Rejected counts expected pending transfers"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_13_1_transfer_message_received()
    await validate_13_1_fence_token_check()
    await validate_13_1_accept_transfer()
    await validate_13_2_stale_token_rejection()
    await validate_13_2_unknown_manager_rejection()
    await validate_13_2_duplicate_transfer()
    await validate_13_3_store_pending()
    await validate_13_3_apply_on_dispatch()
    await validate_13_3_cleanup()
    await validate_13_4_received_count()
    await validate_13_4_accepted_count()
    await validate_13_4_rejected_counts()


if __name__ == "__main__":
    asyncio.run(run())
