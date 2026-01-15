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


async def validate_8_1_manager_sends_windowed_stats_push() -> None:
    spec = _build_spec(
        "gate_manager_8_1_manager_sends_windowed_stats_push",
        "8.1 Windowed Stats Collection - Manager sends WindowedStatsPush",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "WindowedStatsPush expected windowed stats collector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_1_stats_within_window() -> None:
    spec = _build_spec(
        "gate_manager_8_1_stats_within_window",
        "8.1 Windowed Stats Collection - Stats within window",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Stats within window expected windowed stats collector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_1_stats_outside_drift_tolerance() -> None:
    spec = _build_spec(
        "gate_manager_8_1_stats_outside_drift_tolerance",
        "8.1 Windowed Stats Collection - Stats outside drift tolerance",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Stats outside drift tolerance expected windowed stats collector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_1_stats_window_age_limit() -> None:
    spec = _build_spec(
        "gate_manager_8_1_stats_window_age_limit",
        "8.1 Windowed Stats Collection - Stats window age limit",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._windowed_stats is not None, (
            "Stats window age limit expected windowed stats collector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_single_dc_stats() -> None:
    spec = _build_spec(
        "gate_manager_8_2_single_dc_stats",
        "8.2 Stats CRDT Merge - Single DC stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Single DC stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_multi_dc_stats_merge() -> None:
    spec = _build_spec(
        "gate_manager_8_2_multi_dc_stats_merge",
        "8.2 Stats CRDT Merge - Multi-DC stats merge",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Multi-DC stats merge expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_concurrent_stats_updates() -> None:
    spec = _build_spec(
        "gate_manager_8_2_concurrent_stats_updates",
        "8.2 Stats CRDT Merge - Concurrent stats updates",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Concurrent stats updates expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_2_stats_conflict_resolution() -> None:
    spec = _build_spec(
        "gate_manager_8_2_stats_conflict_resolution",
        "8.2 Stats CRDT Merge - Stats conflict resolution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats conflict resolution expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_3_batch_stats_loop() -> None:
    spec = _build_spec(
        "gate_manager_8_3_batch_stats_loop",
        "8.3 Stats Push to Client - Batch stats loop",
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
            "Batch stats loop expected stats coordinator"
        )
        assert callable(getattr(stats_coordinator, "batch_stats_update", None)), (
            "Batch stats loop expected batch_stats_update"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_3_windowed_stats_push_loop() -> None:
    spec = _build_spec(
        "gate_manager_8_3_windowed_stats_push_loop",
        "8.3 Stats Push to Client - Windowed stats push loop",
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
            "Windowed stats push loop expected stats coordinator"
        )
        assert callable(getattr(stats_coordinator, "push_windowed_stats", None)), (
            "Windowed stats push loop expected push_windowed_stats"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_3_stats_coordinator_aggregation() -> None:
    spec = _build_spec(
        "gate_manager_8_3_stats_coordinator_aggregation",
        "8.3 Stats Push to Client - Stats coordinator aggregation",
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
            "Stats coordinator aggregation expected stats coordinator"
        )
        assert callable(getattr(stats_coordinator, "batch_stats_update", None)), (
            "Stats coordinator aggregation expected batch_stats_update"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_3_client_callback_delivery() -> None:
    spec = _build_spec(
        "gate_manager_8_3_client_callback_delivery",
        "8.3 Stats Push to Client - Client callback delivery",
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
            "Client callback delivery expected stats coordinator"
        )
        assert callable(getattr(stats_coordinator, "send_immediate_update", None)), (
            "Client callback delivery expected send_immediate_update"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_manager_dies_with_pending_stats() -> None:
    spec = _build_spec(
        "gate_manager_8_4_manager_dies_with_pending_stats",
        "8.4 Stats Edge Cases - Manager dies with pending stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Manager dies with pending stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_stats_for_completed_job() -> None:
    spec = _build_spec(
        "gate_manager_8_4_stats_for_completed_job",
        "8.4 Stats Edge Cases - Stats for completed job",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats for completed job expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_stats_for_unknown_job() -> None:
    spec = _build_spec(
        "gate_manager_8_4_stats_for_unknown_job",
        "8.4 Stats Edge Cases - Stats for unknown job",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "Stats for unknown job expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def validate_8_4_high_volume_stats() -> None:
    spec = _build_spec(
        "gate_manager_8_4_high_volume_stats",
        "8.4 Stats Edge Cases - High-volume stats",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_stats_crdt is not None, (
            "High-volume stats expected job stats CRDT"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_8_1_manager_sends_windowed_stats_push()
    await validate_8_1_stats_within_window()
    await validate_8_1_stats_outside_drift_tolerance()
    await validate_8_1_stats_window_age_limit()
    await validate_8_2_single_dc_stats()
    await validate_8_2_multi_dc_stats_merge()
    await validate_8_2_concurrent_stats_updates()
    await validate_8_2_stats_conflict_resolution()
    await validate_8_3_batch_stats_loop()
    await validate_8_3_windowed_stats_push_loop()
    await validate_8_3_stats_coordinator_aggregation()
    await validate_8_3_client_callback_delivery()
    await validate_8_4_manager_dies_with_pending_stats()
    await validate_8_4_stats_for_completed_job()
    await validate_8_4_stats_for_unknown_job()
    await validate_8_4_high_volume_stats()


if __name__ == "__main__":
    asyncio.run(run())
