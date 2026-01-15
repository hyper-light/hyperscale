import asyncio
import re
from pathlib import Path

from tests.end_to_end.workflows.base_scenario_workflow import BaseScenarioWorkflow
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.specs.scenario_spec import ScenarioSpec


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
            {"type": "stop_cluster"},
        ],
    }


async def run_all_scenarios() -> None:
    bullets = _extract_bullets(SECTION_START, SECTION_END)
    if not bullets:
        raise AssertionError("No Gate <-> Manager scenarios found")
    runner = ScenarioRunner(WORKFLOW_REGISTRY)
    for bullet in bullets:
        spec = ScenarioSpec.from_dict(_build_scenario(bullet, bullet))
        outcome = await runner.run(spec)
        if outcome.result != ScenarioResult.PASSED:
            raise AssertionError(f"{spec.name} failed: {outcome.error}")


if __name__ == "__main__":
    asyncio.run(run_all_scenarios())
