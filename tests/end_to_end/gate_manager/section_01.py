import asyncio
import re

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
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


def _get_gate(runtime):
    cluster = runtime.require_cluster()
    return cluster.get_gate_leader() or cluster.gates[0]


def _require_runtime(outcome):
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


def _job_id(runtime):
    return runtime.job_ids.get("job-1") or runtime.last_job_id


async def validate_1_1_single_dc_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_1_1_single_dc_dispatch",
        "1.1 Basic Dispatch - Single DC dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        job_id = _job_id(runtime)
        assert job_id, "Expected job id recorded for single DC dispatch"
        assert job_id in state._job_dc_managers, (
            "Single DC dispatch expected _job_dc_managers to include job"
        )
        assert "DC-A" in state._job_dc_managers[job_id], (
            "Single DC dispatch expected DC-A manager assignment"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_1_multi_dc_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_1_1_multi_dc_dispatch",
        "1.1 Basic Dispatch - Multi-DC dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        job_id = _job_id(runtime)
        assert job_id, "Expected job id recorded for multi-DC dispatch"
        assert job_id in state._job_dc_managers, (
            "Multi-DC dispatch expected _job_dc_managers to include job"
        )
        assigned_dcs = set(state._job_dc_managers[job_id].keys())
        assert {"DC-A", "DC-B"}.issubset(assigned_dcs), (
            "Multi-DC dispatch expected DC-A and DC-B assignments"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_1_dispatch_with_client_callback() -> None:
    spec = _build_spec(
        "gate_manager_1_1_dispatch_with_client_callback",
        "1.1 Basic Dispatch - Dispatch with client callback",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        state = gate._modular_state
        job_id = _job_id(runtime)
        assert job_id, "Expected job id recorded for dispatch callback"
        assert job_id in state._progress_callbacks, (
            "Dispatch callback expected _progress_callbacks entry"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_vivaldi_coordinate_routing() -> None:
    spec = _build_spec(
        "gate_manager_1_2_vivaldi_coordinate_routing",
        "1.2 Routing Decisions - Vivaldi coordinate-based routing",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_coordinate_tracker"), (
            "Vivaldi routing expected _coordinate_tracker on gate"
        )
        assert gate._coordinate_tracker is not None, (
            "Vivaldi routing expected _coordinate_tracker initialized"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_blended_latency_scoring() -> None:
    spec = _build_spec(
        "gate_manager_1_2_blended_latency_scoring",
        "1.2 Routing Decisions - Blended latency scoring",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_blended_scorer"), (
            "Blended latency scoring expected _blended_scorer on gate"
        )
        assert callable(getattr(gate._blended_scorer, "score", None)), (
            "Blended latency scoring expected _blended_scorer.score"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_route_learning_record_start() -> None:
    spec = _build_spec(
        "gate_manager_1_2_route_learning_record_start",
        "1.2 Routing Decisions - Route learning record_start",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_dispatch_time_tracker"), (
            "Route learning expected _dispatch_time_tracker on gate"
        )
        assert callable(getattr(gate._dispatch_time_tracker, "record_start", None)), (
            "Route learning expected record_start method"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_route_learning_completion() -> None:
    spec = _build_spec(
        "gate_manager_1_2_route_learning_completion",
        "1.2 Routing Decisions - Route learning completion",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_observed_latency_tracker"), (
            "Route learning completion expected _observed_latency_tracker on gate"
        )
        assert callable(
            getattr(gate._observed_latency_tracker, "record_job_latency", None)
        ), "Route learning completion expected record_job_latency method"
    finally:
        await runtime.stop_cluster()


async def validate_1_2_stale_route_data() -> None:
    spec = _build_spec(
        "gate_manager_1_2_stale_route_data",
        "1.2 Routing Decisions - Stale route data",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "Stale route data expected _job_router"
        assert callable(getattr(gate._job_router, "_filter_stale_latency", None)), (
            "Stale route data expected _filter_stale_latency"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_insufficient_samples() -> None:
    spec = _build_spec(
        "gate_manager_1_2_insufficient_samples",
        "1.2 Routing Decisions - Insufficient samples",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "Insufficient samples expected _job_router"
        assert hasattr(gate._job_router, "_min_samples_for_confidence"), (
            "Insufficient samples expected _min_samples_for_confidence"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_2_dc_candidate_building() -> None:
    spec = _build_spec(
        "gate_manager_1_2_dc_candidate_building",
        "1.2 Routing Decisions - DC candidate building",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "Candidate building expected _job_router"
        assert callable(
            getattr(gate._job_router, "_build_datacenter_candidates", None)
        ), "Candidate building expected _build_datacenter_candidates"
    finally:
        await runtime.stop_cluster()


async def validate_1_3_manager_dies_mid_dispatch() -> None:
    spec = _build_spec(
        "gate_manager_1_3_manager_dies_mid_dispatch",
        "1.3 Dispatch Failures - Manager dies mid-dispatch",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), (
            "Manager dies mid-dispatch expected _job_router"
        )
        assert hasattr(gate._modular_state, "_job_dc_managers"), (
            "Manager dies mid-dispatch expected _job_dc_managers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_all_managers_fail() -> None:
    spec = _build_spec(
        "gate_manager_1_3_all_managers_fail",
        "1.3 Dispatch Failures - All managers in DC fail",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "All managers fail expected _job_router"
        assert hasattr(gate._modular_state, "_job_dc_managers"), (
            "All managers fail expected _job_dc_managers"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_dispatch_timeout() -> None:
    spec = _build_spec(
        "gate_manager_1_3_dispatch_timeout",
        "1.3 Dispatch Failures - Dispatch timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_router"), "Dispatch timeout expected _job_router"
        assert hasattr(gate, "_job_timeout_tracker"), (
            "Dispatch timeout expected _job_timeout_tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_dispatch_rejected_rate_limited() -> None:
    spec = _build_spec(
        "gate_manager_1_3_dispatch_rejected_rate_limited",
        "1.3 Dispatch Failures - Dispatch rejected (rate limited)",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_rate_limiter"), (
            "Rate limited dispatch expected _rate_limiter"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_3_dispatch_rejected_backpressure() -> None:
    spec = _build_spec(
        "gate_manager_1_3_dispatch_rejected_backpressure",
        "1.3 Dispatch Failures - Dispatch rejected (backpressure)",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate._modular_state, "_manager_backpressure"), (
            "Backpressure rejection expected _manager_backpressure"
        )
        assert hasattr(gate._modular_state, "_dc_backpressure"), (
            "Backpressure rejection expected _dc_backpressure"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_4_job_forwarded_to_owner_gate() -> None:
    spec = _build_spec(
        "gate_manager_1_4_job_forwarded_to_owner_gate",
        "1.4 Job Forwarding - Job forwarded to owner gate",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_forwarding_tracker"), (
            "Job forwarding expected _job_forwarding_tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_4_forward_timeout() -> None:
    spec = _build_spec(
        "gate_manager_1_4_forward_timeout",
        "1.4 Job Forwarding - Forward timeout",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_forwarding_tracker"), (
            "Forward timeout expected _job_forwarding_tracker"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_4_max_forward_attempts_exceeded() -> None:
    spec = _build_spec(
        "gate_manager_1_4_max_forward_attempts_exceeded",
        "1.4 Job Forwarding - Max forward attempts exceeded",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_forwarding_tracker"), (
            "Max forward attempts expected _job_forwarding_tracker"
        )
        assert hasattr(gate._job_forwarding_tracker, "max_forward_attempts"), (
            "Max forward attempts expected max_forward_attempts"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_4_forward_loop_detection() -> None:
    spec = _build_spec(
        "gate_manager_1_4_forward_loop_detection",
        "1.4 Job Forwarding - Forward loop detection",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_job_forwarding_tracker"), (
            "Forward loop detection expected _job_forwarding_tracker"
        )
        assert hasattr(gate._job_forwarding_tracker, "detect_loop"), (
            "Forward loop detection expected detect_loop"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_5_duplicate_job_submission() -> None:
    spec = _build_spec(
        "gate_manager_1_5_duplicate_job_submission",
        "1.5 Idempotency - Duplicate job submission",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_idempotency_cache"), (
            "Duplicate submission expected _idempotency_cache"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_5_idempotency_key_expiry() -> None:
    spec = _build_spec(
        "gate_manager_1_5_idempotency_key_expiry",
        "1.5 Idempotency - Idempotency key expiry",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_idempotency_cache"), (
            "Idempotency expiry expected _idempotency_cache"
        )
        assert hasattr(gate._idempotency_cache, "ttl_seconds"), (
            "Idempotency expiry expected ttl_seconds"
        )
    finally:
        await runtime.stop_cluster()


async def validate_1_5_concurrent_duplicate_submissions() -> None:
    spec = _build_spec(
        "gate_manager_1_5_concurrent_duplicate_submissions",
        "1.5 Idempotency - Concurrent duplicate submissions",
    )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    outcome = await runner.run(spec, cleanup=False)
    runtime = _require_runtime(outcome)
    try:
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(outcome.error or "Scenario failed")
        gate = _get_gate(runtime)
        assert hasattr(gate, "_idempotency_cache"), (
            "Concurrent duplicates expected _idempotency_cache"
        )
        assert hasattr(gate._idempotency_cache, "_cache"), (
            "Concurrent duplicates expected cache storage"
        )
    finally:
        await runtime.stop_cluster()


async def run() -> None:
    await validate_1_1_single_dc_dispatch()
    await validate_1_1_multi_dc_dispatch()
    await validate_1_1_dispatch_with_client_callback()
    await validate_1_2_vivaldi_coordinate_routing()
    await validate_1_2_blended_latency_scoring()
    await validate_1_2_route_learning_record_start()
    await validate_1_2_route_learning_completion()
    await validate_1_2_stale_route_data()
    await validate_1_2_insufficient_samples()
    await validate_1_2_dc_candidate_building()
    await validate_1_3_manager_dies_mid_dispatch()
    await validate_1_3_all_managers_fail()
    await validate_1_3_dispatch_timeout()
    await validate_1_3_dispatch_rejected_rate_limited()
    await validate_1_3_dispatch_rejected_backpressure()
    await validate_1_4_job_forwarded_to_owner_gate()
    await validate_1_4_forward_timeout()
    await validate_1_4_max_forward_attempts_exceeded()
    await validate_1_4_forward_loop_detection()
    await validate_1_5_duplicate_job_submission()
    await validate_1_5_idempotency_key_expiry()
    await validate_1_5_concurrent_duplicate_submissions()


if __name__ == "__main__":
    asyncio.run(run())
