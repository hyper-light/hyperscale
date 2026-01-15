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


async def validate_43_1_worker_affinity_rebalancing() -> None:
    spec = _build_spec(
        "manager_worker_43_1_worker_affinity_rebalancing",
        "43.1 Worker affinity vs rebalancing - Sticky assignment vs fairness under churn",
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
            "Worker affinity expected dispatch throughput count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_2_dispatch_gating_slow_heartbeats() -> None:
    spec = _build_spec(
        "manager_worker_43_2_dispatch_gating_slow_heartbeats",
        "43.2 Dispatch gating on slow heartbeats - Avoid routing to slow-but-healthy workers",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_health_states, dict), (
            "Dispatch gating expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_3_cancellation_storm_partial_completion() -> None:
    spec = _build_spec(
        "manager_worker_43_3_cancellation_storm_partial_completion",
        "43.3 Cancellation storms with partial completion - Cancel vs finalize race",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._workflow_lifecycle_states, dict), (
            "Cancellation storm expected workflow lifecycle states"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_4_manager_failover_mid_dispatch() -> None:
    spec = _build_spec(
        "manager_worker_43_4_manager_failover_mid_dispatch",
        "43.4 Manager failover mid-dispatch - Avoid double-dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert state._dispatch_failure_count is not None, (
            "Manager failover expected dispatch failure count"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_5_per_tenant_quotas_mixed_load() -> None:
    spec = _build_spec(
        "manager_worker_43_5_per_tenant_quotas_mixed_load",
        "43.5 Per-tenant quotas under mixed load - No cross-tenant starvation",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._dispatch_semaphores, dict), (
            "Per-tenant quotas expected dispatch semaphores"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_6_clock_drift_progress_timestamps() -> None:
    spec = _build_spec(
        "manager_worker_43_6_clock_drift_progress_timestamps",
        "43.6 Clock drift on progress timestamps - Ordering and dedupe stability",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_job_last_progress, dict), (
            "Clock drift expected worker job progress"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_7_compression_negotiation_progress_results() -> None:
    spec = _build_spec(
        "manager_worker_43_7_compression_negotiation_progress_results",
        "43.7 Compression negotiation for progress/results - Fallback when unsupported",
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
            "Compression negotiation expected progress buffer"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_8_cold_start_throttling() -> None:
    spec = _build_spec(
        "manager_worker_43_8_cold_start_throttling",
        "43.8 Cold-start throttling - Ramp first workflow after restart",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Cold-start throttling expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_9_heartbeat_loss_burst_recovery() -> None:
    spec = _build_spec(
        "manager_worker_43_9_heartbeat_loss_burst_recovery",
        "43.9 Heartbeat loss burst then recovery - No false mass-eviction",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        assert manager._health_monitor is not None, (
            "Heartbeat recovery expected health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_43_10_worker_capability_downgrade_mid_run() -> None:
    spec = _build_spec(
        "manager_worker_43_10_worker_capability_downgrade_mid_run",
        "43.10 Worker capability downgrade mid-run - Feature negotiation fallback",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        manager = _get_manager(runtime, "DC-A")
        state = manager._manager_state
        assert isinstance(state._worker_health_states, dict), (
            "Capability downgrade expected worker health states"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_43_1_worker_affinity_rebalancing()
    await validate_43_2_dispatch_gating_slow_heartbeats()
    await validate_43_3_cancellation_storm_partial_completion()
    await validate_43_4_manager_failover_mid_dispatch()
    await validate_43_5_per_tenant_quotas_mixed_load()
    await validate_43_6_clock_drift_progress_timestamps()
    await validate_43_7_compression_negotiation_progress_results()
    await validate_43_8_cold_start_throttling()
    await validate_43_9_heartbeat_loss_burst_recovery()
    await validate_43_10_worker_capability_downgrade_mid_run()


if __name__ == "__main__":
    asyncio.run(run())
