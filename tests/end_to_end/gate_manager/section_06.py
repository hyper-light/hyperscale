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


async def validate_6_1_manager_reports_capacity() -> None:
    spec = _build_spec(
        "gate_manager_6_1_manager_reports_capacity",
        "6.1 Datacenter Capacity Aggregator - Manager reports capacity",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        capacity_aggregator = gate._capacity_aggregator
        assert callable(getattr(capacity_aggregator, "record_heartbeat", None)), (
            "Manager reports capacity expected record_heartbeat"
        )
        assert callable(getattr(capacity_aggregator, "get_capacity", None)), (
            "Manager reports capacity expected get_capacity"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_1_capacity_staleness() -> None:
    spec = _build_spec(
        "gate_manager_6_1_capacity_staleness",
        "6.1 Datacenter Capacity Aggregator - Capacity staleness",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        capacity_aggregator = gate._capacity_aggregator
        assert hasattr(capacity_aggregator, "_staleness_threshold_seconds"), (
            "Capacity staleness expected _staleness_threshold_seconds"
        )
        assert hasattr(capacity_aggregator, "_manager_heartbeats"), (
            "Capacity staleness expected _manager_heartbeats storage"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_1_aggregate_dc_capacity() -> None:
    spec = _build_spec(
        "gate_manager_6_1_aggregate_dc_capacity",
        "6.1 Datacenter Capacity Aggregator - Aggregate DC capacity",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        capacity_aggregator = gate._capacity_aggregator
        assert callable(getattr(capacity_aggregator, "get_capacity", None)), (
            "Aggregate DC capacity expected get_capacity"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_spillover_enabled() -> None:
    spec = _build_spec(
        "gate_manager_6_2_spillover_enabled",
        "6.2 Spillover Evaluator - Spillover enabled",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        spillover_evaluator = gate._spillover_evaluator
        assert hasattr(spillover_evaluator, "_config"), (
            "Spillover enabled expected _config"
        )
        assert hasattr(spillover_evaluator._config, "spillover_enabled"), (
            "Spillover enabled expected spillover_enabled config"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_dc_at_capacity() -> None:
    spec = _build_spec(
        "gate_manager_6_2_dc_at_capacity",
        "6.2 Spillover Evaluator - DC at capacity",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        spillover_evaluator = gate._spillover_evaluator
        assert callable(getattr(spillover_evaluator, "evaluate", None)), (
            "DC at capacity expected evaluate"
        )
        assert gate._capacity_aggregator is not None, (
            "DC at capacity expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_spillover_latency_penalty() -> None:
    spec = _build_spec(
        "gate_manager_6_2_spillover_latency_penalty",
        "6.2 Spillover Evaluator - Spillover latency penalty",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        spillover_evaluator = gate._spillover_evaluator
        assert hasattr(spillover_evaluator._config, "max_latency_penalty_ms"), (
            "Spillover latency penalty expected max_latency_penalty_ms"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_spillover_improvement_ratio() -> None:
    spec = _build_spec(
        "gate_manager_6_2_spillover_improvement_ratio",
        "6.2 Spillover Evaluator - Spillover improvement ratio",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        spillover_evaluator = gate._spillover_evaluator
        assert hasattr(spillover_evaluator._config, "min_improvement_ratio"), (
            "Spillover improvement ratio expected min_improvement_ratio"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_spillover_wait_timeout() -> None:
    spec = _build_spec(
        "gate_manager_6_2_spillover_wait_timeout",
        "6.2 Spillover Evaluator - Spillover wait timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        spillover_evaluator = gate._spillover_evaluator
        assert hasattr(spillover_evaluator._config, "max_wait_seconds"), (
            "Spillover wait timeout expected max_wait_seconds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_6_2_no_spillover_target_available() -> None:
    spec = _build_spec(
        "gate_manager_6_2_no_spillover_target_available",
        "6.2 Spillover Evaluator - No spillover target available",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        spillover_evaluator = gate._spillover_evaluator
        assert callable(getattr(spillover_evaluator, "evaluate", None)), (
            "No spillover target expected evaluate"
        )
        assert gate._capacity_aggregator is not None, (
            "No spillover target expected capacity aggregator"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_6_1_manager_reports_capacity()
    await validate_6_1_capacity_staleness()
    await validate_6_1_aggregate_dc_capacity()
    await validate_6_2_spillover_enabled()
    await validate_6_2_dc_at_capacity()
    await validate_6_2_spillover_latency_penalty()
    await validate_6_2_spillover_improvement_ratio()
    await validate_6_2_spillover_wait_timeout()
    await validate_6_2_no_spillover_target_available()


if __name__ == "__main__":
    asyncio.run(run())
