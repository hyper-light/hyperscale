import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    await runtime.stop_cluster()
    return ActionOutcome(
        name="stop_cluster",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
    )
