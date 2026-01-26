from dataclasses import dataclass, field

from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate import GateServer
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer

from tests.framework.specs.cluster_spec import ClusterSpec


@dataclass(slots=True)
class TestCluster:
    gates: list[GateServer] = field(default_factory=list)
    managers: dict[str, list[ManagerServer]] = field(default_factory=dict)
    workers: dict[str, list[WorkerServer]] = field(default_factory=dict)
    client: HyperscaleClient | None = None
    config: ClusterSpec | None = None

    def get_gate_leader(self) -> GateServer | None:
        for gate in self.gates:
            if gate.is_leader():
                return gate
        return None

    def get_manager_leader(self, datacenter_id: str) -> ManagerServer | None:
        for manager in self.managers.get(datacenter_id, []):
            if manager.is_leader():
                return manager
        return None

    def get_all_managers(self) -> list[ManagerServer]:
        all_managers: list[ManagerServer] = []
        for datacenter_managers in self.managers.values():
            all_managers.extend(datacenter_managers)
        return all_managers

    def get_all_workers(self) -> list[WorkerServer]:
        all_workers: list[WorkerServer] = []
        for datacenter_workers in self.workers.values():
            all_workers.extend(datacenter_workers)
        return all_workers
