import asyncio
import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    duration = float(action.params.get("seconds", 0))
    await asyncio.sleep(duration)
    return ActionOutcome(
        name="sleep",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=f"slept {duration}s",
    )
