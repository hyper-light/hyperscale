import asyncio
from pathlib import Path

from tests.framework.results.scenario_outcome import ScenarioOutcome
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runner.scenario_runner import ScenarioRunner
from tests.framework.specs.scenario_spec import ScenarioSpec


async def run_from_json(path: str, workflow_registry: dict) -> ScenarioOutcome:
    loop = asyncio.get_event_loop()
    spec = loop.run_in_executor(None, ScenarioSpec.from_json, Path(path))
    runner = ScenarioRunner(workflow_registry)
    outcome = await runner.run(spec)
    if outcome.result != ScenarioResult.PASSED:
        raise AssertionError(outcome.error or "Scenario failed")
    return outcome
