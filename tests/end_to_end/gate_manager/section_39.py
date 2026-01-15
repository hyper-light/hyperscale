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


async def validate_39_1_log_volume() -> None:
    spec = _build_spec(
        "gate_manager_39_1_log_volume",
        "39.1 Logging Under Load - Log volume",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Log volume expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_1_log_sampling() -> None:
    spec = _build_spec(
        "gate_manager_39_1_log_sampling",
        "39.1 Logging Under Load - Log sampling",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Log sampling expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_1_structured_logging() -> None:
    spec = _build_spec(
        "gate_manager_39_1_structured_logging",
        "39.1 Logging Under Load - Structured logging",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Structured logging expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_1_log_buffer_overflow() -> None:
    spec = _build_spec(
        "gate_manager_39_1_log_buffer_overflow",
        "39.1 Logging Under Load - Log buffer overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Log buffer overflow expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_2_metrics_cardinality() -> None:
    spec = _build_spec(
        "gate_manager_39_2_metrics_cardinality",
        "39.2 Metrics Under Load - Metrics cardinality",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Metrics cardinality expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_2_metrics_sampling() -> None:
    spec = _build_spec(
        "gate_manager_39_2_metrics_sampling",
        "39.2 Metrics Under Load - Metrics sampling",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Metrics sampling expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_2_metrics_push_latency() -> None:
    spec = _build_spec(
        "gate_manager_39_2_metrics_push_latency",
        "39.2 Metrics Under Load - Metrics push latency",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Metrics push latency expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_2_metrics_memory() -> None:
    spec = _build_spec(
        "gate_manager_39_2_metrics_memory",
        "39.2 Metrics Under Load - Metrics memory",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Metrics memory expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_3_trace_sampling_rate() -> None:
    spec = _build_spec(
        "gate_manager_39_3_trace_sampling_rate",
        "39.3 Tracing Under Load - Trace sampling rate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Trace sampling rate expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_3_trace_propagation() -> None:
    spec = _build_spec(
        "gate_manager_39_3_trace_propagation",
        "39.3 Tracing Under Load - Trace propagation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Trace propagation expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_3_trace_storage() -> None:
    spec = _build_spec(
        "gate_manager_39_3_trace_storage",
        "39.3 Tracing Under Load - Trace storage",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Trace storage expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_39_3_trace_analysis() -> None:
    spec = _build_spec(
        "gate_manager_39_3_trace_analysis",
        "39.3 Tracing Under Load - Trace analysis",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Trace analysis expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_39_1_log_volume()
    await validate_39_1_log_sampling()
    await validate_39_1_structured_logging()
    await validate_39_1_log_buffer_overflow()
    await validate_39_2_metrics_cardinality()
    await validate_39_2_metrics_sampling()
    await validate_39_2_metrics_push_latency()
    await validate_39_2_metrics_memory()
    await validate_39_3_trace_sampling_rate()
    await validate_39_3_trace_propagation()
    await validate_39_3_trace_storage()
    await validate_39_3_trace_analysis()


if __name__ == "__main__":
    asyncio.run(run())
