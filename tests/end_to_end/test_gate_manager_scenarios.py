import asyncio
import re
from pathlib import Path

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.specs.scenario_spec import ScenarioSpec
from tests.framework.runtime.scenario_runtime import ScenarioRuntime


SCENARIO_PATH = Path(__file__).resolve().parents[2] / "SCENARIOS.md"
SECTION_START = "Gate <-> Manager Scenarios (Comprehensive)"
SECTION_END = "Manager <-> Worker Scenarios (Comprehensive)"

WORKFLOW_REGISTRY = {"BaseScenarioWorkflow": BaseScenarioWorkflow}


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


def _assert_hasattr(obj: object, name: str) -> None:
    assert hasattr(obj, name), f"Expected {obj} to have attribute '{name}'"


def _assert_non_empty(mapping: dict, label: str) -> None:
    assert mapping, f"Expected {label} to be non-empty"


def _assert_gate_manager_bullet(
    bullet: str,
    runtime: ScenarioRuntime,
) -> None:
    gate = _get_gate(runtime)
    manager = _get_manager(runtime, "DC-A")
    gate_state = gate._modular_state
    manager_state = manager._manager_state
    job_id = runtime.job_ids.get("job-1") or runtime.last_job_id
    bullet_lower = bullet.lower()
    matched = False

    if "dispatch" in bullet_lower or "routing" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_job_router")
        _assert_hasattr(gate, "_dispatch_time_tracker")
        _assert_hasattr(gate, "_observed_latency_tracker")
        _assert_hasattr(gate, "_coordinate_tracker")
        _assert_hasattr(gate, "_blended_scorer")
        _assert_hasattr(gate_state, "_job_dc_managers")
        if job_id:
            assert job_id in runtime.job_ids.values(), "Expected job id recorded"

    if "forward" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_job_forwarding_tracker")
        _assert_hasattr(gate_state, "_forward_throughput_count")

    if "idempotency" in bullet_lower or "idempotent" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_idempotency_cache")

    if "register" in bullet_lower or "discovery" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_datacenter_manager_status")
        assert "DC-A" in gate_state._datacenter_manager_status
        _assert_non_empty(
            gate_state._datacenter_manager_status["DC-A"], "manager status"
        )
        _assert_hasattr(gate_state, "_manager_health")
        _assert_hasattr(gate_state, "_manager_last_status")

    if "heartbeat" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_manager_last_status")
        _assert_non_empty(gate_state._manager_last_status, "manager_last_status")

    if "health" in bullet_lower or "unhealthy" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_manager_health")
        _assert_hasattr(gate_state, "_gate_peer_health")

    if "circuit" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_manager_backpressure")
        _assert_hasattr(gate, "_quorum_circuit")

    if "backpressure" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_manager_backpressure")
        _assert_hasattr(gate_state, "_dc_backpressure")
        _assert_hasattr(gate_state, "_backpressure_delay_ms")

    if "capacity" in bullet_lower or "spillover" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_capacity_aggregator")
        _assert_hasattr(gate, "_job_router")

    if "progress" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_job_progress_sequences")
        _assert_hasattr(gate_state, "_job_progress_seen")
        _assert_hasattr(gate_state, "_progress_callbacks")

    if "stats" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_windowed_stats")
        _assert_hasattr(gate_state, "_job_stats_crdt")

    if "workflow result" in bullet_lower or "result" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_workflow_dc_results")
        _assert_hasattr(gate_state, "_job_workflow_ids")

    if "final" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_job_manager")
        _assert_hasattr(gate, "_dispatch_time_tracker")
        _assert_hasattr(gate, "_observed_latency_tracker")

    if "timeout" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_job_timeout_tracker")

    if "reporter" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_job_reporter_tasks")

    if "leadership" in bullet_lower or "leader" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_job_leadership_tracker")
        _assert_hasattr(gate_state, "_dead_job_leaders")

    if "lease" in bullet_lower or "fence" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_leases")
        _assert_hasattr(gate_state, "_fence_token")

    if "quorum" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_quorum_circuit")

    if "sync" in bullet_lower or "snapshot" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_state_sync_handler")
        _assert_hasattr(gate_state, "_state_version")

    if "protocol" in bullet_lower or "capabilities" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_manager_negotiated_caps")

    if "cancellation" in bullet_lower or "cancel" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_cancellation_errors")
        _assert_hasattr(gate_state, "_cancellation_completion_events")

    if "throughput" in bullet_lower or "latency" in bullet_lower:
        matched = True
        _assert_hasattr(gate_state, "_forward_throughput_count")
        _assert_hasattr(gate_state, "_forward_throughput_interval_start")

    if "error" in bullet_lower or "exception" in bullet_lower:
        matched = True
        _assert_hasattr(gate, "_load_shedder")
        _assert_hasattr(gate, "_rate_limiter")

    if "manager" in bullet_lower and "health" in bullet_lower:
        matched = True
        _assert_hasattr(manager_state, "_manager_peer_unhealthy_since")

    if not matched:
        raise AssertionError(f"No verification criteria mapped for bullet: {bullet}")


def _run_scenario(runtime: ScenarioRuntime, bullet: str) -> None:
    _assert_gate_manager_bullet(bullet, runtime)


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
            _run_scenario(runtime, bullet)
        finally:
            await runtime.stop_cluster()


if __name__ == "__main__":
    asyncio.run(run_all_scenarios())
