import time

from ..results.action_outcome import ActionOutcome
from ..runtime.scenario_runtime import ScenarioRuntime
from ..specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    await runtime.start_cluster()
    return ActionOutcome(
        name="start_cluster",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
    )
