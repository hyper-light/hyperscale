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


async def validate_36_1_ramp_up_pattern() -> None:
    spec = _build_spec(
        "gate_manager_36_1_ramp_up_pattern",
        "36.1 Realistic Load Profile - Ramp-up pattern",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Ramp-up pattern expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_1_steady_state() -> None:
    spec = _build_spec(
        "gate_manager_36_1_steady_state",
        "36.1 Realistic Load Profile - Steady state",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Steady state expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_1_spike_pattern() -> None:
    spec = _build_spec(
        "gate_manager_36_1_spike_pattern",
        "36.1 Realistic Load Profile - Spike pattern",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Spike pattern expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_1_ramp_down_pattern() -> None:
    spec = _build_spec(
        "gate_manager_36_1_ramp_down_pattern",
        "36.1 Realistic Load Profile - Ramp-down pattern",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._overload_detector is not None, (
            "Ramp-down pattern expected overload detector"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_2_load_from_us() -> None:
    spec = _build_spec(
        "gate_manager_36_2_load_from_us",
        "36.2 Multi-Region Load Test - Load from US",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Load from US expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_2_load_from_europe() -> None:
    spec = _build_spec(
        "gate_manager_36_2_load_from_europe",
        "36.2 Multi-Region Load Test - Load from Europe",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Load from Europe expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_2_load_from_asia() -> None:
    spec = _build_spec(
        "gate_manager_36_2_load_from_asia",
        "36.2 Multi-Region Load Test - Load from Asia",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Load from Asia expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_2_cross_region_load() -> None:
    spec = _build_spec(
        "gate_manager_36_2_cross_region_load",
        "36.2 Multi-Region Load Test - Cross-region load",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Cross-region load expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_3_http_workflows() -> None:
    spec = _build_spec(
        "gate_manager_36_3_http_workflows",
        "36.3 Mixed Workflow Types - HTTP workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "HTTP workflows expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_3_graphql_workflows() -> None:
    spec = _build_spec(
        "gate_manager_36_3_graphql_workflows",
        "36.3 Mixed Workflow Types - GraphQL workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "GraphQL workflows expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_3_playwright_workflows() -> None:
    spec = _build_spec(
        "gate_manager_36_3_playwright_workflows",
        "36.3 Mixed Workflow Types - Playwright workflows",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Playwright workflows expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_3_mixed_workload() -> None:
    spec = _build_spec(
        "gate_manager_36_3_mixed_workload",
        "36.3 Mixed Workflow Types - Mixed workload",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_router is not None, "Mixed workload expected job router"
    finally:
        await runtime.stop_cluster()


async def validate_36_4_kill_random_worker() -> None:
    spec = _build_spec(
        "gate_manager_36_4_kill_random_worker",
        "36.4 Failure Injection During Load - Kill random worker",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Kill random worker expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_4_kill_random_manager() -> None:
    spec = _build_spec(
        "gate_manager_36_4_kill_random_manager",
        "36.4 Failure Injection During Load - Kill random manager",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Kill random manager expected timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_4_network_partition() -> None:
    spec = _build_spec(
        "gate_manager_36_4_network_partition",
        "36.4 Failure Injection During Load - Network partition",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._state_sync_handler is not None, (
            "Network partition expected state sync handler"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_4_dc_failure() -> None:
    spec = _build_spec(
        "gate_manager_36_4_dc_failure",
        "36.4 Failure Injection During Load - DC failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._dc_health_monitor is not None, (
            "DC failure expected DC health monitor"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_5_memory_growth() -> None:
    spec = _build_spec(
        "gate_manager_36_5_memory_growth",
        "36.5 Resource Monitoring During Load - Memory growth",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Memory growth expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_5_cpu_utilization() -> None:
    spec = _build_spec(
        "gate_manager_36_5_cpu_utilization",
        "36.5 Resource Monitoring During Load - CPU utilization",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "CPU utilization expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_5_network_throughput() -> None:
    spec = _build_spec(
        "gate_manager_36_5_network_throughput",
        "36.5 Resource Monitoring During Load - Network throughput",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Network throughput expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_5_connection_count() -> None:
    spec = _build_spec(
        "gate_manager_36_5_connection_count",
        "36.5 Resource Monitoring During Load - Connection count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Connection count expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def validate_36_5_task_count() -> None:
    spec = _build_spec(
        "gate_manager_36_5_task_count",
        "36.5 Resource Monitoring During Load - Goroutine/task count",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._stats_coordinator is not None, (
            "Task count expected stats coordinator"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_36_1_ramp_up_pattern()
    await validate_36_1_steady_state()
    await validate_36_1_spike_pattern()
    await validate_36_1_ramp_down_pattern()
    await validate_36_2_load_from_us()
    await validate_36_2_load_from_europe()
    await validate_36_2_load_from_asia()
    await validate_36_2_cross_region_load()
    await validate_36_3_http_workflows()
    await validate_36_3_graphql_workflows()
    await validate_36_3_playwright_workflows()
    await validate_36_3_mixed_workload()
    await validate_36_4_kill_random_worker()
    await validate_36_4_kill_random_manager()
    await validate_36_4_network_partition()
    await validate_36_4_dc_failure()
    await validate_36_5_memory_growth()
    await validate_36_5_cpu_utilization()
    await validate_36_5_network_throughput()
    await validate_36_5_connection_count()
    await validate_36_5_task_count()


if __name__ == "__main__":
    asyncio.run(run())
