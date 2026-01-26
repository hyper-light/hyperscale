import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec
from tests.framework.actions.stop_nodes import _select_nodes


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    role = action.params.get("role")
    if not role:
        raise ValueError("restart_nodes requires role")
    dc_id = action.params.get("dc_id")
    indices = action.params.get("indices")
    count = action.params.get("count")
    nodes = _select_nodes(runtime, role, dc_id)
    if indices is not None:
        nodes = [nodes[index] for index in indices]
    if count is not None:
        nodes = nodes[: int(count)]
    assert nodes, "No nodes selected for restart_nodes"
    for node in nodes:
        await node.stop(drain_timeout=0.5, broadcast_leave=False)
    for node in nodes:
        await node.start()
    return ActionOutcome(
        name="restart_nodes",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=f"restarted {len(nodes)} {role} nodes",
    )
