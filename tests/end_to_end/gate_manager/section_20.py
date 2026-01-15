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


async def validate_20_1_handler_exceptions() -> None:
    spec = _build_spec(
        "gate_manager_20_1_handler_exceptions",
        "20.1 Exception Handling - Handler exceptions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Handler exceptions expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_1_background_loop_exceptions() -> None:
    spec = _build_spec(
        "gate_manager_20_1_background_loop_exceptions",
        "20.1 Exception Handling - Background loop exceptions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, (
            "Background loop exceptions expected rate limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_1_coordinator_exceptions() -> None:
    spec = _build_spec(
        "gate_manager_20_1_coordinator_exceptions",
        "20.1 Exception Handling - Coordinator exceptions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Coordinator exceptions expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_2_tcp_send_failure() -> None:
    spec = _build_spec(
        "gate_manager_20_2_tcp_send_failure",
        "20.2 Connection Failures - TCP send failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, "TCP send failure expected load shedder"
    finally:
        await runtime.stop_cluster()


async def validate_20_2_udp_send_failure() -> None:
    spec = _build_spec(
        "gate_manager_20_2_udp_send_failure",
        "20.2 Connection Failures - UDP send failure",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, "UDP send failure expected rate limiter"
    finally:
        await runtime.stop_cluster()


async def validate_20_2_connection_timeout() -> None:
    spec = _build_spec(
        "gate_manager_20_2_connection_timeout",
        "20.2 Connection Failures - Connection timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._job_timeout_tracker is not None, (
            "Connection timeout expected job timeout tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_3_invalid_message_format() -> None:
    spec = _build_spec(
        "gate_manager_20_3_invalid_message_format",
        "20.3 Serialization Failures - Invalid message format",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, (
            "Invalid message format expected load shedder"
        )
    finally:
        await runtime.stop_cluster()


async def validate_20_3_partial_message() -> None:
    spec = _build_spec(
        "gate_manager_20_3_partial_message",
        "20.3 Serialization Failures - Partial message",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._rate_limiter is not None, "Partial message expected rate limiter"
    finally:
        await runtime.stop_cluster()


async def validate_20_3_large_message() -> None:
    spec = _build_spec(
        "gate_manager_20_3_large_message",
        "20.3 Serialization Failures - Large message",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert gate._load_shedder is not None, "Large message expected load shedder"
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_20_1_handler_exceptions()
    await validate_20_1_background_loop_exceptions()
    await validate_20_1_coordinator_exceptions()
    await validate_20_2_tcp_send_failure()
    await validate_20_2_udp_send_failure()
    await validate_20_2_connection_timeout()
    await validate_20_3_invalid_message_format()
    await validate_20_3_partial_message()
    await validate_20_3_large_message()


if __name__ == "__main__":
    asyncio.run(run())
