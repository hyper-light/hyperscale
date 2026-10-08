"""Test double for ``FaultMatrix``'s inspection surface."""

from tests.simulation.harness.server_handle import ServerHandle


class FakeFaults:
    """Kill, pause and disruption state an invariant reads, set directly by a test."""

    def __init__(self) -> None:
        self.killed_node_ids: set[str] = set()
        self.paused_node_ids: set[str] = set()
        self.disrupted: bool = False

    def is_killed(self, handle: ServerHandle) -> bool:
        return handle.node_id in self.killed_node_ids

    def is_paused(self, handle: ServerHandle) -> bool:
        return handle.node_id in self.paused_node_ids

    def has_active_disruption(self) -> bool:
        return self.disrupted
