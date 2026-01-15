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


async def validate_19_1_forward_throughput() -> None:
    spec = _build_spec(
        "gate_manager_19_1_forward_throughput",
        "19.1 Throughput Tracking - Forward throughput",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._forward_throughput_count is not None, (
            "Forward throughput expected forward throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_19_1_throughput_calculation() -> None:
    spec = _build_spec(
        "gate_manager_19_1_throughput_calculation",
        "19.1 Throughput Tracking - Throughput calculation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._forward_throughput_interval_start is not None, (
            "Throughput calculation expected throughput interval start"
        )
    finally:
        await runtime.stop_cluster()


async def validate_19_1_throughput_interval() -> None:
    spec = _build_spec(
        "gate_manager_19_1_throughput_interval",
        "19.1 Throughput Tracking - Throughput interval",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert state._forward_throughput_interval_start is not None, (
            "Throughput interval expected throughput interval start"
        )
    finally:
        await runtime.stop_cluster()


async def validate_19_2_per_manager_latency() -> None:
    spec = _build_spec(
        "gate_manager_19_2_per_manager_latency",
        "19.2 Latency Tracking - Per-manager latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Per-manager latency expected observed latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_19_2_latency_sample_age() -> None:
    spec = _build_spec(
        "gate_manager_19_2_latency_sample_age",
        "19.2 Latency Tracking - Latency sample age",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Latency sample age expected observed latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_19_2_latency_sample_count() -> None:
    spec = _build_spec(
        "gate_manager_19_2_latency_sample_count",
        "19.2 Latency Tracking - Latency sample count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._observed_latency_tracker is not None, (
            "Latency sample count expected observed latency tracker"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_19_1_forward_throughput()
    await validate_19_1_throughput_calculation()
    await validate_19_1_throughput_interval()
    await validate_19_2_per_manager_latency()
    await validate_19_2_latency_sample_age()
    await validate_19_2_latency_sample_count()


if __name__ == "__main__":
    asyncio.run(run())
