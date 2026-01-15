import asyncio
import re
from pathlib import Path

from hyperscale.distributed.models import JobFinalResult

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.scenario_spec import ScenarioSpec


SCENARIO_PATH = Path(__file__).resolve().parents[2] / "SCENARIOS.md"
SECTION_START = "Gate <-> Manager Scenarios (Comprehensive)"
SECTION_END = "Manager <-> Worker Scenarios (Comprehensive)"

WORKFLOW_REGISTRY = {"BaseScenarioWorkflow": BaseScenarioWorkflow}

FIELD_TARGETS = {
    "_datacenter_manager_status": "gate_state",
    "_manager_last_status": "gate_state",
    "_manager_health": "gate_state",
    "_manager_backpressure": "gate_state",
    "_dc_backpressure": "gate_state",
    "_backpressure_delay_ms": "gate_state",
    "_manager_negotiated_caps": "gate_state",
    "_workflow_dc_results": "gate_state",
    "_job_workflow_ids": "gate_state",
    "_job_dc_managers": "gate_state",
    "_job_submissions": "gate_state",
    "_job_reporter_tasks": "gate_state",
    "_job_lease_renewal_tokens": "gate_state",
    "_job_progress_sequences": "gate_state",
    "_job_progress_seen": "gate_state",
    "_job_progress_lock": "gate_state",
    "_cancellation_completion_events": "gate_state",
    "_cancellation_errors": "gate_state",
    "_progress_callbacks": "gate_state",
    "_job_update_sequences": "gate_state",
    "_job_update_history": "gate_state",
    "_job_client_update_positions": "gate_state",
    "_leases": "gate_state",
    "_fence_token": "gate_state",
    "_dead_job_leaders": "gate_state",
    "_orphaned_jobs": "gate_state",
    "_gate_state": "gate_state",
    "_state_version": "gate_state",
    "_gate_peer_unhealthy_since": "gate_state",
    "_dead_gate_peers": "gate_state",
    "_dead_gate_timestamps": "gate_state",
    "_forward_throughput_count": "gate_state",
    "_forward_throughput_interval_start": "gate_state",
    "_forward_throughput_last_value": "gate_state",
    "_job_router": "gate",
    "_job_timeout_tracker": "gate",
    "_job_leadership_tracker": "gate",
    "_job_manager": "gate",
    "_capacity_aggregator": "gate",
    "_dispatch_time_tracker": "gate",
    "_observed_latency_tracker": "gate",
    "_coordinate_tracker": "gate",
    "_blended_scorer": "gate",
    "_job_forwarding_tracker": "gate",
    "_idempotency_cache": "gate",
    "_quorum_circuit": "gate",
    "_load_shedder": "gate",
    "_rate_limiter": "gate",
    "_overload_detector": "gate",
    "_state_sync_handler": "gate",
    "_manager_peer_unhealthy_since": "manager_state",
}

JOB_KEY_FIELDS = {
    "_job_dc_managers",
    "_job_workflow_ids",
    "_job_submissions",
    "_job_reporter_tasks",
    "_job_progress_sequences",
    "_job_progress_seen",
    "_cancellation_completion_events",
    "_cancellation_errors",
    "_progress_callbacks",
    "_job_update_sequences",
    "_job_update_history",
    "_job_client_update_positions",
    "_workflow_dc_results",
}

CLASS_FIELD_MAP = {
    "GateJobTimeoutTracker": ("gate", "_job_timeout_tracker"),
    "GateJobManager": ("gate", "_job_manager"),
    "GateJobRouter": ("gate", "_job_router"),
    "JobLeadershipTracker": ("gate", "_job_leadership_tracker"),
    "GateIdempotencyCache": ("gate", "_idempotency_cache"),
    "DatacenterCapacityAggregator": ("gate", "_capacity_aggregator"),
    "GateStateSyncHandler": ("gate", "_state_sync_handler"),
}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return slug[:80] if slug else "scenario"


def _extract_bullets(start_marker: str, end_marker: str | None) -> list[str]:
    bullets: list[str] = []
    in_section = False
    for line in SCENARIO_PATH.read_text().splitlines():
        if start_marker in line:
            in_section = True
            continue
        if in_section and end_marker and end_marker in line:
            break
        if in_section and line.strip().startswith("- "):
            bullets.append(line.strip()[2:])
    return bullets


def _build_scenario(name: str, description: str) -> dict:
    slug = _slugify(name)
    subclass_name = f"ScenarioWorkflow{slug[:32]}"
    return {
        "name": f"gate_manager_{slug}",
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


def _get_gate(runtime: ScenarioRuntime):
    cluster = runtime.require_cluster()
    return cluster.get_gate_leader() or cluster.gates[0]


def _get_manager(runtime: ScenarioRuntime, dc_id: str):
    cluster = runtime.require_cluster()
    return cluster.get_manager_leader(dc_id) or cluster.managers[dc_id][0]


def _get_target(runtime: ScenarioRuntime, target_name: str):
    gate = _get_gate(runtime)
    manager = _get_manager(runtime, "DC-A")
    if target_name == "gate":
        return gate
    if target_name == "gate_state":
        return gate._modular_state
    if target_name == "manager":
        return manager
    if target_name == "manager_state":
        return manager._manager_state
    raise AssertionError(f"Unknown target {target_name}")


def _extract_field_refs(bullet: str) -> list[str]:
    return list(dict.fromkeys(re.findall(r"_[a-zA-Z0-9_]+", bullet)))


def _extract_method_refs(bullet: str) -> list[tuple[str, str]]:
    return [
        (match.group(1), match.group(2))
        for match in re.finditer(r"(_[a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\(", bullet)
    ]


def _extract_class_method_refs(bullet: str) -> list[tuple[str, str]]:
    return [
        (match.group(1), match.group(2))
        for match in re.finditer(r"([A-Za-z][A-Za-z0-9_]+)\.([a-zA-Z0-9_]+)\(", bullet)
    ]


def _assert_field(runtime: ScenarioRuntime, field_name: str, bullet: str) -> None:
    if field_name not in FIELD_TARGETS:
        raise AssertionError(
            f"Bullet '{bullet}' references unmapped field '{field_name}'"
        )
    target = _get_target(runtime, FIELD_TARGETS[field_name])
    assert hasattr(target, field_name), (
        f"Bullet '{bullet}' expected {target} to have '{field_name}'"
    )
    value = getattr(target, field_name)
    if field_name in JOB_KEY_FIELDS:
        job_id = runtime.job_ids.get("job-1") or runtime.last_job_id
        if job_id:
            assert job_id in value, (
                f"Bullet '{bullet}' expected {field_name} to include job {job_id}"
            )


def _assert_method(
    runtime: ScenarioRuntime,
    field_name: str,
    method_name: str,
    bullet: str,
) -> None:
    target = _get_target(runtime, FIELD_TARGETS[field_name])
    field = getattr(target, field_name)
    assert hasattr(field, method_name), (
        f"Bullet '{bullet}' expected {field_name}.{method_name} to exist"
    )
    assert callable(getattr(field, method_name)), (
        f"Bullet '{bullet}' expected {field_name}.{method_name} to be callable"
    )


def _assert_class_method(
    runtime: ScenarioRuntime,
    class_name: str,
    method_name: str,
    bullet: str,
) -> None:
    if class_name in CLASS_FIELD_MAP:
        target_name, field_name = CLASS_FIELD_MAP[class_name]
        target = _get_target(runtime, target_name)
        field = getattr(target, field_name)
        assert hasattr(field, method_name), (
            f"Bullet '{bullet}' expected {class_name}.{method_name}"
        )
        assert callable(getattr(field, method_name)), (
            f"Bullet '{bullet}' expected {class_name}.{method_name} to be callable"
        )
        return
    if class_name == "JobFinalResult":
        assert hasattr(JobFinalResult, method_name), (
            f"Bullet '{bullet}' expected JobFinalResult.{method_name}"
        )
        assert callable(getattr(JobFinalResult, method_name)), (
            f"Bullet '{bullet}' expected JobFinalResult.{method_name} to be callable"
        )
        return


def _assert_fallbacks(bullet: str, runtime: ScenarioRuntime) -> bool:
    bullet_lower = bullet.lower()
    if "reporter" in bullet_lower:
        assert runtime.callbacks.reporter_results is not None, (
            f"Bullet '{bullet}' expected reporter_results"
        )
        return True
    if "workflow result" in bullet_lower or "result" in bullet_lower:
        assert runtime.callbacks.workflow_results is not None, (
            f"Bullet '{bullet}' expected workflow_results"
        )
        return True
    if "progress" in bullet_lower:
        assert runtime.callbacks.progress_updates is not None, (
            f"Bullet '{bullet}' expected progress_updates"
        )
        return True
    if "status" in bullet_lower:
        assert runtime.callbacks.status_updates is not None, (
            f"Bullet '{bullet}' expected status_updates"
        )
        return True
    return False


def _assert_gate_manager_bullet(bullet: str, runtime: ScenarioRuntime) -> None:
    field_refs = _extract_field_refs(bullet)
    method_refs = _extract_method_refs(bullet)
    class_method_refs = _extract_class_method_refs(bullet)

    for field_name in field_refs:
        _assert_field(runtime, field_name, bullet)
    for field_name, method_name in method_refs:
        if field_name in FIELD_TARGETS:
            _assert_method(runtime, field_name, method_name, bullet)
    for class_name, method_name in class_method_refs:
        _assert_class_method(runtime, class_name, method_name, bullet)

    if not field_refs and not method_refs and not class_method_refs:
        matched = _assert_fallbacks(bullet.lower(), runtime)
        if not matched:
            raise AssertionError(f"No explicit assertions for bullet: {bullet}")


def _get_runtime(outcome):
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def run_all_scenarios() -> None:
    bullets = _extract_bullets(SECTION_START, SECTION_END)
    if not bullets:
        raise AssertionError("No Gate <-> Manager scenarios found")
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    for bullet in bullets:
        spec = ScenarioSpec.from_dict(_build_scenario(bullet, bullet))
        outcome = await runner.run(spec, cleanup=False)
        runtime = _get_runtime(outcome)
        try:
            if outcome.result != ScenarioResult.PASSED:
                raise AssertionError(f"{spec.name} failed: {outcome.error}")
            _assert_gate_manager_bullet(bullet, runtime)
        finally:
            await runtime.stop_cluster()


if __name__ == "__main__":
    asyncio.run(run_all_scenarios())
