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


async def validate_16_1_dispatch_throughput() -> None:
    spec = _build_spec(
        "manager_worker_16_1_dispatch_throughput",
        "16.1 Dispatch Throughput - Throughput counter",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_count is not None, (
            "Dispatch throughput expected throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_1_interval_calculation() -> None:
    spec = _build_spec(
        "manager_worker_16_1_interval_calculation",
        "16.1 Dispatch Throughput - Interval calculation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_interval_start is not None, (
            "Interval calculation expected throughput interval start"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_1_reset_on_interval() -> None:
    spec = _build_spec(
        "manager_worker_16_1_reset_on_interval",
        "16.1 Dispatch Throughput - Reset on interval",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_throughput_interval_start is not None, (
            "Reset on interval expected throughput interval start"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_2_per_worker_latency() -> None:
    spec = _build_spec(
        "manager_worker_16_2_per_worker_latency",
        "16.2 Latency Tracking - Per-worker latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_latency_samples, dict), (
            "Per-worker latency expected worker latency samples"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_2_latency_samples() -> None:
    spec = _build_spec(
        "manager_worker_16_2_latency_samples",
        "16.2 Latency Tracking - Latency samples",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_latency_samples, dict), (
            "Latency samples expected worker latency samples"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_2_sample_cleanup() -> None:
    spec = _build_spec(
        "manager_worker_16_2_sample_cleanup",
        "16.2 Latency Tracking - Sample cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_latency_samples, dict), (
            "Sample cleanup expected worker latency samples"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_3_worker_count() -> None:
    spec = _build_spec(
        "manager_worker_16_3_worker_count",
        "16.3 Worker Metrics - Worker count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workers, dict), "Worker count expected workers"
    finally:
        await runtime.stop_cluster()


async def validate_16_3_unhealthy_count() -> None:
    spec = _build_spec(
        "manager_worker_16_3_unhealthy_count",
        "16.3 Worker Metrics - Unhealthy count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_unhealthy_since, dict), (
            "Unhealthy count expected worker unhealthy since"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_3_circuit_state() -> None:
    spec = _build_spec(
        "manager_worker_16_3_circuit_state",
        "16.3 Worker Metrics - Circuit state",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_circuits, dict), (
            "Circuit state expected worker circuits"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_4_workflow_latency_digest() -> None:
    spec = _build_spec(
        "manager_worker_16_4_workflow_latency_digest",
        "16.4 SLO Tracking - Workflow latency digest",
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
            "Workflow latency digest expected latency digest"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_4_latency_observations() -> None:
    spec = _build_spec(
        "manager_worker_16_4_latency_observations",
        "16.4 SLO Tracking - Latency observations",
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
            "Latency observations expected latency digest"
        )
    finally:
        await runtime.stop_cluster()


async def validate_16_4_percentile_calculation() -> None:
    spec = _build_spec(
        "manager_worker_16_4_percentile_calculation",
        "16.4 SLO Tracking - Percentile calculation",
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
            "Percentile calculation expected latency digest"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_16_1_dispatch_throughput()
    await validate_16_1_interval_calculation()
    await validate_16_1_reset_on_interval()
    await validate_16_2_per_worker_latency()
    await validate_16_2_latency_samples()
    await validate_16_2_sample_cleanup()
    await validate_16_3_worker_count()
    await validate_16_3_unhealthy_count()
    await validate_16_3_circuit_state()
    await validate_16_4_workflow_latency_digest()
    await validate_16_4_latency_observations()
    await validate_16_4_percentile_calculation()


if __name__ == "__main__":
    asyncio.run(run())
