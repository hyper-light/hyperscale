import asyncio
import time

from ..results.action_outcome import ActionOutcome
from ..runtime.scenario_runtime import ScenarioRuntime
from ..specs.action_spec import ActionSpec


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
