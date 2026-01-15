import asyncio
import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    timeout = float(action.params.get("timeout", 20.0))
    datacenter_id = action.params.get("dc_id")
    if not datacenter_id:
        raise ValueError("await_manager_leader requires dc_id")
    cluster = runtime.require_cluster()
    deadline = time.monotonic() + timeout
    leader = cluster.get_manager_leader(datacenter_id)
    while leader is None and time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        leader = cluster.get_manager_leader(datacenter_id)
    assert leader is not None, f"Manager leader not elected for {datacenter_id}"
    details = leader.node_id if hasattr(leader, "node_id") else None
    return ActionOutcome(
        name="await_manager_leader",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=details,
    )
