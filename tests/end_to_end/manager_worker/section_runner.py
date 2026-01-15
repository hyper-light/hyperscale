import asyncio
import re
from pathlib import Path

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.scenario_spec import ScenarioSpec


SCENARIO_PATH = Path(__file__).resolve().parents[3] / "SCENARIOS.md"
SECTION_START = "Manager <-> Worker Scenarios (Comprehensive)"
SECTION_END = "High-Throughput Load Test Scenarios"

WORKFLOW_REGISTRY = {"BaseScenarioWorkflow": BaseScenarioWorkflow}

FIELD_TARGETS = {
    "_workers": "manager_state",
    "_worker_addr_to_id": "manager_state",
    "_worker_circuits": "manager_state",
    "_worker_unhealthy_since": "manager_state",
    "_worker_deadlines": "manager_state",
    "_worker_job_last_progress": "manager_state",
    "_worker_health_states": "manager_state",
    "_dispatch_semaphores": "manager_state",
    "_job_leaders": "manager_state",
    "_job_leader_addrs": "manager_state",
    "_job_fencing_tokens": "manager_state",
    "_job_contexts": "manager_state",
    "_job_callbacks": "manager_state",
    "_client_callbacks": "manager_state",
    "_job_origin_gates": "manager_state",
    "_progress_callbacks": "manager_state",
    "_cancellation_pending_workflows": "manager_state",
    "_cancellation_errors": "manager_state",
    "_cancellation_completion_events": "manager_state",
    "_cancelled_workflows": "manager_state",
    "_workflow_lifecycle_states": "manager_state",
    "_workflow_completion_events": "manager_state",
    "_job_submissions": "manager_state",
    "_job_reporter_tasks": "manager_state",
    "_workflow_retries": "manager_state",
    "_job_timeout_strategies": "manager_state",
    "_job_aggregated_results": "manager_state",
    "_cores_available_event": "manager_state",
    "_core_allocation_lock": "manager_state",
    "_eager_dispatch_lock": "manager_state",
    "_dispatch_throughput_count": "manager_state",
    "_dispatch_throughput_interval_start": "manager_state",
    "_dispatch_throughput_last_value": "manager_state",
    "_dispatch_failure_count": "manager_state",
    "_workflow_latency_digest": "manager_state",
    "_gate_latency_samples": "manager_state",
    "_peer_manager_latency_samples": "manager_state",
    "_worker_latency_samples": "manager_state",
    "_pending_provisions": "manager_state",
    "_provision_confirmations": "manager_state",
    "_versioned_clock": "manager_state",
    "_state_version": "manager_state",
    "_fence_token": "manager_state",
    "_manager_state": "manager_state",
    "_known_gates": "manager_state",
    "_healthy_gate_ids": "manager_state",
    "_known_manager_peers": "manager_state",
    "_active_manager_peer_ids": "manager_state",
    "_manager_peer_info": "manager_state",
    "_workflow_tokens": "worker_state",
    "_workflow_cancel_events": "worker_state",
    "_workflow_id_to_name": "worker_state",
    "_workflow_job_leader": "worker_state",
    "_workflow_fence_tokens": "worker_state",
    "_workflow_cores_completed": "worker_state",
    "_workflow_start_times": "worker_state",
    "_workflow_timeout_seconds": "worker_state",
    "_pending_workflows": "worker_state",
    "_orphaned_workflows": "worker_state",
    "_pending_transfers": "worker_state",
    "_job_fence_tokens": "worker_state",
    "_progress_buffer": "worker_state",
    "_extension_requested": "worker_state",
    "_extension_reason": "worker_state",
    "_extension_current_progress": "worker_state",
    "_extension_completed_items": "worker_state",
    "_extension_total_items": "worker_state",
    "_extension_estimated_completion": "worker_state",
    "_extension_active_workflow_count": "worker_state",
    "_registry": "manager",
    "_worker_pool": "manager",
    "_health_monitor": "manager",
    "_cancellation": "manager",
    "_dispatch": "manager",
    "_workflow_dispatcher": "manager",
    "_overload_detector": "manager",
    "_load_shedder": "manager",
    "_rate_limiter": "manager",
    "_core_allocator": "worker",
    "_core_assignments": "core_allocator",
    "_workflow_cores": "core_allocator",
    "_available_cores": "core_allocator",
    "_cores_available": "core_allocator",
}

KEYWORD_REQUIREMENTS = {
    "register": ["_workers", "_worker_addr_to_id", "_worker_circuits"],
    "registration": ["_workers", "_worker_addr_to_id", "_worker_circuits"],
    "unregister": ["_worker_deadlines", "_worker_unhealthy_since"],
    "disconnect": ["_worker_deadlines", "_worker_unhealthy_since"],
    "pool": ["_worker_pool"],
    "core": ["_core_allocator"],
    "allocation": ["_core_allocator"],
    "dispatch": ["_dispatch", "_dispatch_semaphores", "_workflow_dispatcher"],
    "priority": ["_job_submissions"],
    "health": ["_health_monitor", "_worker_unhealthy_since"],
    "circuit": ["_worker_circuits"],
    "workflow": ["_workflow_completion_events"],
    "progress": ["_worker_job_last_progress", "_progress_buffer"],
    "cancel": ["_cancellation_pending_workflows", "_workflow_cancel_events"],
    "cancellation": ["_cancellation_completion_events"],
    "reporter": ["_job_reporter_tasks"],
    "leadership": ["_job_leaders", "_job_fencing_tokens"],
    "leader": ["_job_leaders", "_job_fencing_tokens"],
    "timeout": ["_job_timeout_strategies"],
    "orphan": ["_orphaned_workflows"],
    "stats": ["_workflow_latency_digest"],
    "metrics": ["_workflow_latency_digest"],
    "latency": ["_workflow_latency_digest"],
    "throughput": ["_dispatch_throughput_count"],
}

JOB_KEY_FIELDS = {
    "_job_leaders",
    "_job_leader_addrs",
    "_job_fencing_tokens",
    "_job_contexts",
    "_job_callbacks",
    "_job_origin_gates",
    "_progress_callbacks",
    "_cancellation_pending_workflows",
    "_cancellation_errors",
    "_cancellation_completion_events",
    "_job_submissions",
    "_job_reporter_tasks",
    "_workflow_retries",
    "_job_timeout_strategies",
    "_job_aggregated_results",
}

CLASS_FIELD_MAP = {
    "ManagerRegistry": ("manager", "_registry"),
    "WorkerPool": ("manager", "_worker_pool"),
    "CoreAllocator": ("worker", "_core_allocator"),
    "ManagerDispatchCoordinator": ("manager", "_dispatch"),
    "ManagerHealthMonitor": ("manager", "_health_monitor"),
    "ManagerCancellationCoordinator": ("manager", "_cancellation"),
    "ManagerLoadShedder": ("manager", "_load_shedder"),
    "ServerRateLimiter": ("manager", "_rate_limiter"),
    "WorkflowDispatcher": ("manager", "_workflow_dispatcher"),
}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return slug[:80] if slug else "scenario"


def _extract_section_bullets(section_number: int) -> list[str]:
    bullets: list[str] = []
    in_section = False
    in_numbered_section = False
    for line in SCENARIO_PATH.read_text().splitlines():
        if SECTION_START in line:
            in_section = True
            continue
        if in_section and SECTION_END in line:
            break
        if not in_section:
            continue
        if re.match(r"^\d+\.\s", line):
            current_number = int(line.split(".", 1)[0])
            in_numbered_section = current_number == section_number
            continue
        if in_numbered_section and line.strip().startswith("- "):
            bullets.append(line.strip()[2:])
    return bullets


def _build_scenario(name: str, description: str) -> dict:
    slug = _slugify(name)
    subclass_name = f"ScenarioWorkflow{slug[:32]}"
    return {
        "name": f"manager_worker_{slug}",
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


def _get_manager(runtime: ScenarioRuntime, dc_id: str):
    cluster = runtime.require_cluster()
    return cluster.get_manager_leader(dc_id) or cluster.managers[dc_id][0]


def _get_worker(runtime: ScenarioRuntime):
    cluster = runtime.require_cluster()
    return cluster.get_all_workers()[0]


def _get_target(runtime: ScenarioRuntime, target_name: str):
    manager = _get_manager(runtime, "DC-A")
    worker = _get_worker(runtime)
    match target_name:
        case "manager":
            return manager
        case "manager_state":
            return manager._manager_state
        case "worker":
            return worker
        case "worker_state":
            return worker._worker_state
        case "core_allocator":
            return worker._core_allocator
        case _:
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


def _assert_keywords(bullet: str, runtime: ScenarioRuntime) -> bool:
    bullet_lower = bullet.lower()
    matched = False
    for keyword, fields in KEYWORD_REQUIREMENTS.items():
        if keyword in bullet_lower:
            matched = True
            for field_name in fields:
                _assert_field(runtime, field_name, bullet)
    return matched


def _assert_manager_worker_bullet(bullet: str, runtime: ScenarioRuntime) -> None:
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
        if not _assert_keywords(bullet, runtime):
            raise AssertionError(f"No explicit assertions for bullet: {bullet}")


def _get_runtime(outcome):
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


def _build_title(section_number: int, bullet: str) -> str:
    return f"Manager<->Worker {section_number}: {bullet}"


async def run_section(section_number: int) -> None:
    bullets = _extract_section_bullets(section_number)
    if not bullets:
        raise AssertionError(
            f"No bullets found for Manager<->Worker section {section_number}"
        )
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    for bullet in bullets:
        spec = ScenarioSpec.from_dict(
            _build_scenario(bullet, _build_title(section_number, bullet))
        )
        outcome = await runner.run(spec, cleanup=False)
        runtime = _get_runtime(outcome)
        try:
            if outcome.result != ScenarioResult.PASSED:
                raise AssertionError(f"{spec.name} failed: {outcome.error}")
            _assert_manager_worker_bullet(bullet, runtime)
        finally:
            await runtime.stop_cluster()
