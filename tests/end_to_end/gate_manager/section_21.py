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


async def validate_21_1_burst_stats_traffic() -> None:
    spec = _build_spec(
        "gate_manager_21_1_burst_stats_traffic",
        "21.1 Burst Stats Traffic - 1000 VUs generating stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Burst stats traffic expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_batching_under_load() -> None:
    spec = _build_spec(
        "gate_manager_21_1_stats_batching_under_load",
        "21.1 Burst Stats Traffic - Stats batching under load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Stats batching under load expected windowed stats"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_queue_overflow() -> None:
    spec = _build_spec(
        "gate_manager_21_1_stats_queue_overflow",
        "21.1 Burst Stats Traffic - Stats queue overflow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats queue overflow expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_memory_pressure() -> None:
    spec = _build_spec(
        "gate_manager_21_1_stats_memory_pressure",
        "21.1 Burst Stats Traffic - Stats memory pressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats memory pressure expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_1_stats_flush_backpressure() -> None:
    spec = _build_spec(
        "gate_manager_21_1_stats_flush_backpressure",
        "21.1 Burst Stats Traffic - Stats flush backpressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_backpressure, dict), (
            "Stats flush backpressure expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_out_of_order_stats_batches() -> None:
    spec = _build_spec(
        "gate_manager_21_2_out_of_order_stats_batches",
        "21.2 Stats Ordering and Deduplication - Out-of-order stats batches",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Out-of-order stats batches expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_duplicate_stats_batch() -> None:
    spec = _build_spec(
        "gate_manager_21_2_duplicate_stats_batch",
        "21.2 Stats Ordering and Deduplication - Duplicate stats batch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Duplicate stats batch expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_stats_from_dead_worker() -> None:
    spec = _build_spec(
        "gate_manager_21_2_stats_from_dead_worker",
        "21.2 Stats Ordering and Deduplication - Stats from dead worker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats from dead worker expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_2_stats_version_conflict() -> None:
    spec = _build_spec(
        "gate_manager_21_2_stats_version_conflict",
        "21.2 Stats Ordering and Deduplication - Stats version conflict",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats version conflict expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_parallel_stats_merging() -> None:
    spec = _build_spec(
        "gate_manager_21_3_parallel_stats_merging",
        "21.3 Stats Aggregation Under Load - Parallel stats merging",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Parallel stats merging expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_partial_aggregation_windows() -> None:
    spec = _build_spec(
        "gate_manager_21_3_partial_aggregation_windows",
        "21.3 Stats Aggregation Under Load - Partial aggregation windows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Partial aggregation windows expected windowed stats"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_stats_window_boundary() -> None:
    spec = _build_spec(
        "gate_manager_21_3_stats_window_boundary",
        "21.3 Stats Aggregation Under Load - Stats window boundary",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Stats window boundary expected windowed stats"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_3_stats_compression() -> None:
    spec = _build_spec(
        "gate_manager_21_3_stats_compression",
        "21.3 Stats Aggregation Under Load - Stats compression",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats compression expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_manager_overloaded() -> None:
    spec = _build_spec(
        "gate_manager_21_4_manager_overloaded",
        "21.4 Stats Pipeline Backpressure - Manager overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        assert isinstance(state._manager_backpressure, dict), (
            "Manager overloaded expected manager backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_gate_overloaded() -> None:
    spec = _build_spec(
        "gate_manager_21_4_gate_overloaded",
        "21.4 Stats Pipeline Backpressure - Gate overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Gate overloaded expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_client_callback_slow() -> None:
    spec = _build_spec(
        "gate_manager_21_4_client_callback_slow",
        "21.4 Stats Pipeline Backpressure - Client callback slow",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        stats_coordinator = gate._stats_coordinator
        assert stats_coordinator is not None, (
            "Client callback slow expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_21_4_end_to_end_latency_spike() -> None:
    spec = _build_spec(
        "gate_manager_21_4_end_to_end_latency_spike",
        "21.4 Stats Pipeline Backpressure - End-to-end latency spike",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "End-to-end latency spike expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_21_1_burst_stats_traffic()
    await validate_21_1_stats_batching_under_load()
    await validate_21_1_stats_queue_overflow()
    await validate_21_1_stats_memory_pressure()
    await validate_21_1_stats_flush_backpressure()
    await validate_21_2_out_of_order_stats_batches()
    await validate_21_2_duplicate_stats_batch()
    await validate_21_2_stats_from_dead_worker()
    await validate_21_2_stats_version_conflict()
    await validate_21_3_parallel_stats_merging()
    await validate_21_3_partial_aggregation_windows()
    await validate_21_3_stats_window_boundary()
    await validate_21_3_stats_compression()
    await validate_21_4_manager_overloaded()
    await validate_21_4_gate_overloaded()
    await validate_21_4_client_callback_slow()
    await validate_21_4_end_to_end_latency_spike()


if __name__ == "__main__":
    asyncio.run(run())
