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


async def validate_11_1_backpressure_signal() -> None:
    spec = _build_spec(
        "manager_worker_11_1_backpressure_signal",
        "11.1 Manager → Worker Backpressure - Backpressure signal",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._manager_backpressure, dict), (
            "Backpressure signal expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_1_worker_receives() -> None:
    spec = _build_spec(
        "manager_worker_11_1_worker_receives",
        "11.1 Manager → Worker Backpressure - Worker receives",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._manager_backpressure, dict), (
            "Worker receives expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_1_behavior_adjustment() -> None:
    spec = _build_spec(
        "manager_worker_11_1_behavior_adjustment",
        "11.1 Manager → Worker Backpressure - Behavior adjustment",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "Behavior adjustment expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_none() -> None:
    spec = _build_spec(
        "manager_worker_11_2_none",
        "11.2 Worker Backpressure Response - NONE",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "NONE backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_throttle() -> None:
    spec = _build_spec(
        "manager_worker_11_2_throttle",
        "11.2 Worker Backpressure Response - THROTTLE",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "THROTTLE backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_batch() -> None:
    spec = _build_spec(
        "manager_worker_11_2_batch",
        "11.2 Worker Backpressure Response - BATCH",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "BATCH backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_2_reject() -> None:
    spec = _build_spec(
        "manager_worker_11_2_reject",
        "11.2 Worker Backpressure Response - REJECT",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        worker = _get_worker(runtime)
        state = worker._worker_state
        assert isinstance(state._progress_buffer, dict), (
            "REJECT backpressure expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_3_latency_recording() -> None:
    spec = _build_spec(
        "manager_worker_11_3_latency_recording",
        "11.3 Latency Recording - Workflow latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._workflow_latency_digest is not None, (
            "Workflow latency expected latency digest"
        )
    finally:
        await runtime.stop_cluster()


async def validate_11_3_latency_digest() -> None:
    spec = _build_spec(
        "manager_worker_11_3_latency_digest",
        "11.3 Latency Recording - Latency digest",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._workflow_latency_digest is not None, (
            "Latency digest expected latency digest"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_11_1_backpressure_signal()
    await validate_11_1_worker_receives()
    await validate_11_1_behavior_adjustment()
    await validate_11_2_none()
    await validate_11_2_throttle()
    await validate_11_2_batch()
    await validate_11_2_reject()
    await validate_11_3_latency_recording()
    await validate_11_3_latency_digest()


if __name__ == "__main__":
    asyncio.run(run())
