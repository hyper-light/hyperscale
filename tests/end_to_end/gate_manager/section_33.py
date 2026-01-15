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


async def validate_33_1_gate_cluster_split() -> None:
    spec = _build_spec(
        "gate_manager_33_1_gate_cluster_split",
        "33.1 Gate Cluster Split - 3/5 gates partitioned",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._quorum_circuit is not None, (
            "Gate cluster split expected quorum circuit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_1_jobs_in_both_partitions() -> None:
    spec = _build_spec(
        "gate_manager_33_1_jobs_in_both_partitions",
        "33.1 Gate Cluster Split - Jobs in both partitions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Jobs in both partitions expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_1_partition_heals() -> None:
    spec = _build_spec(
        "gate_manager_33_1_partition_heals",
        "33.1 Gate Cluster Split - Partition heals",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Partition heals expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_1_fencing_token_resolution() -> None:
    spec = _build_spec(
        "gate_manager_33_1_fencing_token_resolution",
        "33.1 Gate Cluster Split - Fencing token resolution",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_leadership_tracker is not None, (
            "Fencing token resolution expected leadership tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_2_manager_cluster_split() -> None:
    spec = _build_spec(
        "gate_manager_33_2_manager_cluster_split",
        "33.2 Manager Cluster Split - Manager cluster splits",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_manager is not None, (
            "Manager cluster split expected DC health manager"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_2_worker_dispatch_wrong_partition() -> None:
    spec = _build_spec(
        "gate_manager_33_2_worker_dispatch_wrong_partition",
        "33.2 Manager Cluster Split - Worker dispatches to wrong partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, (
            "Wrong partition dispatch expected job router"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_2_partition_detection() -> None:
    spec = _build_spec(
        "gate_manager_33_2_partition_detection",
        "33.2 Manager Cluster Split - Partition detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Partition detection expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_2_partition_recovery() -> None:
    spec = _build_spec(
        "gate_manager_33_2_partition_recovery",
        "33.2 Manager Cluster Split - Partition recovery",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Partition recovery expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_3_entire_dc_isolated() -> None:
    spec = _build_spec(
        "gate_manager_33_3_entire_dc_isolated",
        "33.3 DC Isolation - Entire DC isolated",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "DC isolation expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_3_isolated_dc_continues_running() -> None:
    spec = _build_spec(
        "gate_manager_33_3_isolated_dc_continues_running",
        "33.3 DC Isolation - Isolated DC continues running",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Isolated DC running expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_33_3_isolation_detected() -> None:
    spec = _build_spec(
        "gate_manager_33_3_isolation_detected",
        "33.3 DC Isolation - Isolation detected",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "Isolation detected expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_33_3_isolation_ends() -> None:
    spec = _build_spec(
        "gate_manager_33_3_isolation_ends",
        "33.3 DC Isolation - Isolation ends",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Isolation ends expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_33_1_gate_cluster_split()
    await validate_33_1_jobs_in_both_partitions()
    await validate_33_1_partition_heals()
    await validate_33_1_fencing_token_resolution()
    await validate_33_2_manager_cluster_split()
    await validate_33_2_worker_dispatch_wrong_partition()
    await validate_33_2_partition_detection()
    await validate_33_2_partition_recovery()
    await validate_33_3_entire_dc_isolated()
    await validate_33_3_isolated_dc_continues_running()
    await validate_33_3_isolation_detected()
    await validate_33_3_isolation_ends()


if __name__ == "__main__":
    asyncio.run(run())
