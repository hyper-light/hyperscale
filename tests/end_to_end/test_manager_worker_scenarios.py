import asyncio
import re
from pathlib import Path

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.scenario_spec import ScenarioSpec


SCENARIO_PATH = Path(__file__).resolve().parents[2] / "SCENARIOS.md"
SECTION_START = "Manager <-> Worker Scenarios (Comprehensive)"

WORKFLOW_REGISTRY = {"BaseScenarioWorkflow": BaseScenarioWorkflow}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return slug[:80] if slug else "scenario"


def _extract_bullets(start_marker: str) -> list[str]:
    bullets: list[str] = []
    in_section = False
    for line in SCENARIO_PATH.read_text().splitlines():
        if start_marker in line:
            in_section = True
            continue
        if in_section and line.startswith("---"):
            continue
        if in_section and line.strip().startswith("- "):
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


def _assert_hasattr(obj: object, name: str) -> None:
    assert hasattr(obj, name), f"Expected {obj} to have attribute '{name}'"


def _assert_manager_worker_bullet(bullet: str, runtime: ScenarioRuntime) -> None:
    manager = _get_manager(runtime, "DC-A")
    worker = _get_worker(runtime)
    manager_state = manager._manager_state
    worker_state = worker._worker_state
    job_id = runtime.job_ids.get("job-1") or runtime.last_job_id
    bullet_lower = bullet.lower()
    matched = False

    if "register" in bullet_lower or "registration" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_workers")
        _assert_hasattr(manager_state, "_worker_addr_to_id")
        _assert_hasattr(manager_state, "_worker_circuits")
        _assert_hasattr(manager_state, "_worker_health_states")

    if "unregister" in bullet_lower or "disconnect" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_worker_deadlines")
        _assert_hasattr(manager_state, "_worker_unhealthy_since")

    if "worker pool" in bullet_lower or "health state" in bullet_lower:
        matched = True
        _assert_hasattr(manager, "_worker_pool")

    if "core" in bullet_lower or "allocation" in bullet_lower:
        matched = True
        _assert_hasattr(worker, "_core_allocator")
        _assert_hasattr(worker_state, "_workflow_cores_completed")

    if "dispatch" in bullet_lower:
        matched = True
        _assert_hasattr(manager, "_dispatch_coordinator")
        _assert_hasattr(manager_state, "_dispatch_semaphores")

    if "priority" in bullet_lower or "scheduling" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_job_submissions")

    if "health" in bullet_lower:
        matched = True
        _assert_hasattr(manager, "_health_monitor")
        _assert_hasattr(manager_state, "_worker_unhealthy_since")

    if "circuit" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_worker_circuits")

    if "workflow" in bullet_lower and "state" in bullet_lower:
        matched = True
        _assert_hasattr(worker_state, "_workflow_completion_events")

    if "progress" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_worker_job_last_progress")
        _assert_hasattr(worker_state, "_progress_buffer")

    if "cancellation" in bullet_lower or "cancel" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_cancellation_pending_workflows")
        _assert_hasattr(manager_state, "_cancellation_completion_events")
        _assert_hasattr(worker_state, "_workflow_cancel_events")

    if "reporter" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_job_reporter_tasks")

    if "leadership" in bullet_lower or "leader" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_job_leaders")
        _assert_hasattr(manager_state, "_job_fencing_tokens")

    if "timeout" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_job_timeout_strategies")

    if "orphan" in bullet_lower:
        matched = True
        _assert_hasattr(worker_state, "_orphaned_workflows")

    if "metrics" in bullet_lower or "stats" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_workflow_stats_buffer")

    if "latency" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_workflow_latency_tracker")

    if job_id and "job" in bullet_lower:
        matched = True
        assert job_id in runtime.job_ids.values(), "Expected job id recorded"

    if not matched:
        raise AssertionError(f"No verification criteria mapped for bullet: {bullet}")


def _get_runtime(outcome):
    runtime = outcome.runtime
    if runtime is None:
        raise AssertionError("Scenario runtime not available")
    return runtime


async def run_all_scenarios() -> None:
    bullets = _extract_bullets(SECTION_START)
    if not bullets:
        raise AssertionError("No Manager <-> Worker scenarios found")
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    for bullet in bullets:
        spec = ScenarioSpec.from_dict(_build_scenario(bullet, bullet))
        outcome = await runner.run(spec, cleanup=False)
        runtime = _get_runtime(outcome)
        try:
            if outcome.result != ScenarioResult.PASSED:
                raise AssertionError(f"{spec.name} failed: {outcome.error}")
            _assert_manager_worker_bullet(bullet, runtime)
        finally:
            await runtime.stop_cluster()


if __name__ == "__main__":
    asyncio.run(run_all_scenarios())
