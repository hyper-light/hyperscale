"""Test double for the ``ClusterHarness`` surface the continuous invariants read."""

from types import SimpleNamespace

from tests.simulation.harness.server_handle import ServerHandle
from tests.unit.simulation.harness.fake_faults import FakeFaults


class FakeCluster:
    """Holds real-state node handles; a test mutates them to inject violations."""

    def __init__(self, datacenter_ids: list[str]) -> None:
        self.handles: list[ServerHandle] = []
        self.faults = FakeFaults()
        self.stabilized: bool = True
        self.spec = SimpleNamespace(datacenters={datacenter_id: None for datacenter_id in datacenter_ids})

    def all_handles(self) -> list[ServerHandle]:
        return list(self.handles)

    def address_to_node_id(self, address: tuple[str, int], *, kind: str = "tcp") -> str | None:
        port_of = {"tcp": lambda handle: handle.tcp_port, "udp": lambda handle: handle.udp_port}[kind]
        owners = [handle.node_id for handle in self.handles if (handle.host, port_of(handle)) == address]
        return next(iter(owners), None)
