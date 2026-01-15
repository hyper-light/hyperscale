import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


def _select_nodes(runtime: ScenarioRuntime, role: str, dc_id: str | None):
    cluster = runtime.require_cluster()
    if role == "gate":
        return cluster.gates
    if role == "manager":
        if dc_id:
            return cluster.managers.get(dc_id, [])
        nodes = []
        for managers in cluster.managers.values():
            nodes.extend(managers)
        return nodes
    if role == "worker":
        if dc_id:
            return cluster.workers.get(dc_id, [])
        nodes = []
        for workers in cluster.workers.values():
            nodes.extend(workers)
        return nodes
    raise ValueError(f"Unknown role '{role}'")


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    role = action.params.get("role")
    if not role:
        raise ValueError("stop_nodes requires role")
    dc_id = action.params.get("dc_id")
    indices = action.params.get("indices")
    count = action.params.get("count")
    nodes = _select_nodes(runtime, role, dc_id)
    if indices is not None:
        nodes = [nodes[index] for index in indices]
    if count is not None:
        nodes = nodes[: int(count)]
    assert nodes, "No nodes selected for stop_nodes"
    for node in nodes:
        await node.stop(drain_timeout=0.5, broadcast_leave=False)
    return ActionOutcome(
        name="stop_nodes",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=f"stopped {len(nodes)} {role} nodes",
    )
