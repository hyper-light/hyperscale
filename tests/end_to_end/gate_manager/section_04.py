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


async def validate_4_1_delta_based_detection() -> None:
    spec = _build_spec(
        "gate_manager_4_1_delta_based_detection",
        "4.1 Hybrid Overload Detector - Delta-based detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        detector = gate._overload_detector
        assert callable(getattr(detector, "record_latency", None)), (
            "Delta detection expected record_latency"
        )
        assert callable(getattr(detector, "get_state", None)), (
            "Delta detection expected get_state"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_absolute_threshold_detection() -> None:
    spec = _build_spec(
        "gate_manager_4_1_absolute_threshold_detection",
        "4.1 Hybrid Overload Detector - Absolute threshold detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        detector = gate._overload_detector
        assert hasattr(detector, "_config"), (
            "Absolute threshold detection expected _config"
        )
        assert hasattr(detector._config, "absolute_bounds"), (
            "Absolute threshold detection expected absolute_bounds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_cpu_based_detection() -> None:
    spec = _build_spec(
        "gate_manager_4_1_cpu_based_detection",
        "4.1 Hybrid Overload Detector - CPU-based detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        detector = gate._overload_detector
        assert hasattr(detector._config, "cpu_thresholds"), (
            "CPU-based detection expected cpu_thresholds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_memory_based_detection() -> None:
    spec = _build_spec(
        "gate_manager_4_1_memory_based_detection",
        "4.1 Hybrid Overload Detector - Memory-based detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        detector = gate._overload_detector
        assert hasattr(detector._config, "memory_thresholds"), (
            "Memory-based detection expected memory_thresholds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_state_transitions() -> None:
    spec = _build_spec(
        "gate_manager_4_1_state_transitions",
        "4.1 Hybrid Overload Detector - State transitions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        detector = gate._overload_detector
        assert hasattr(detector, "_current_state"), (
            "State transitions expected _current_state"
        )
        assert callable(getattr(detector, "get_state", None)), (
            "State transitions expected get_state"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_1_recovery_detection() -> None:
    spec = _build_spec(
        "gate_manager_4_1_recovery_detection",
        "4.1 Hybrid Overload Detector - Recovery detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        detector = gate._overload_detector
        assert callable(getattr(detector, "get_diagnostics", None)), (
            "Recovery detection expected get_diagnostics"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_shed_request_when_overloaded() -> None:
    spec = _build_spec(
        "gate_manager_4_2_shed_request_when_overloaded",
        "4.2 Load Shedding - Shed request when overloaded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        shedder = gate._load_shedder
        assert callable(getattr(shedder, "should_shed", None)), (
            "Load shedding expected should_shed"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_shed_percentage_by_state() -> None:
    spec = _build_spec(
        "gate_manager_4_2_shed_percentage_by_state",
        "4.2 Load Shedding - Shed percentage by state",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        shedder = gate._load_shedder
        assert hasattr(shedder, "_config"), "Load shedding percentage expected _config"
        assert hasattr(shedder._config, "shed_thresholds"), (
            "Load shedding percentage expected shed_thresholds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_priority_based_shedding() -> None:
    spec = _build_spec(
        "gate_manager_4_2_priority_based_shedding",
        "4.2 Load Shedding - Priority-based shedding",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        shedder = gate._load_shedder
        assert callable(getattr(shedder, "classify_request", None)), (
            "Priority shedding expected classify_request"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_2_shed_response_to_client() -> None:
    spec = _build_spec(
        "gate_manager_4_2_shed_response_to_client",
        "4.2 Load Shedding - Shed response to client",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        shedder = gate._load_shedder
        assert hasattr(shedder, "_shed_requests"), (
            "Shed response expected _shed_requests counter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_3_per_client_rate_limiting() -> None:
    spec = _build_spec(
        "gate_manager_4_3_per_client_rate_limiting",
        "4.3 Rate Limiting - Per-client rate limiting",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        limiter = gate._rate_limiter
        assert callable(getattr(limiter, "check_rate_limit", None)), (
            "Rate limiting expected check_rate_limit"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_3_rate_limit_exceeded() -> None:
    spec = _build_spec(
        "gate_manager_4_3_rate_limit_exceeded",
        "4.3 Rate Limiting - Rate limit exceeded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        limiter = gate._rate_limiter
        assert callable(getattr(limiter, "check", None)), (
            "Rate limit exceeded expected check"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_3_rate_limit_cleanup() -> None:
    spec = _build_spec(
        "gate_manager_4_3_rate_limit_cleanup",
        "4.3 Rate Limiting - Rate limit cleanup",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        limiter = gate._rate_limiter
        assert callable(getattr(limiter, "cleanup_inactive_clients", None)), (
            "Rate limit cleanup expected cleanup_inactive_clients"
        )
    finally:
        await runtime.stop_cluster()


async def validate_4_3_rate_limit_with_backpressure() -> None:
    spec = _build_spec(
        "gate_manager_4_3_rate_limit_with_backpressure",
        "4.3 Rate Limiting - Rate limit with backpressure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        limiter = gate._rate_limiter
        assert hasattr(limiter, "_adaptive"), (
            "Rate limit backpressure expected adaptive limiter"
        )
        assert hasattr(limiter._adaptive, "overload_detector"), (
            "Rate limit backpressure expected overload_detector"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_4_1_delta_based_detection()
    await validate_4_1_absolute_threshold_detection()
    await validate_4_1_cpu_based_detection()
    await validate_4_1_memory_based_detection()
    await validate_4_1_state_transitions()
    await validate_4_1_recovery_detection()
    await validate_4_2_shed_request_when_overloaded()
    await validate_4_2_shed_percentage_by_state()
    await validate_4_2_priority_based_shedding()
    await validate_4_2_shed_response_to_client()
    await validate_4_3_per_client_rate_limiting()
    await validate_4_3_rate_limit_exceeded()
    await validate_4_3_rate_limit_cleanup()
    await validate_4_3_rate_limit_with_backpressure()


if __name__ == "__main__":
    asyncio.run(run())
