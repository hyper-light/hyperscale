import asyncio
import time

from ..results.action_outcome import ActionOutcome
from ..runtime.scenario_runtime import ScenarioRuntime
from ..specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    timeout = float(action.params.get("timeout", 20.0))
    cluster = runtime.require_cluster()
    deadline = time.monotonic() + timeout
    leader = cluster.get_gate_leader()
    while leader is None and time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        leader = cluster.get_gate_leader()
    assert leader is not None, "Gate leader not elected"
    return ActionOutcome(
        name="await_gate_leader",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=leader.node_id if hasattr(leader, "node_id") else None,
    )
