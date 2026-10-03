"""
The gate has one view of each datacenter's health.

GateHealthCoordinator classifies a datacenter by merging the managers'
TCP heartbeats with the federated UDP probes (AD-33: a suspected DC is
degraded, a confirmed-unreachable one is degraded or unhealthy). The
router used that merged view, but the server's own
_classify_datacenter_health / _get_all_datacenter_health -- behind
admission's initializing check, legacy selection, ping and the
active-DC count -- read the TCP heartbeats alone. A DC the probes
suspected was "healthy" to everything but the router.

Driven through the real coordinator and the server's real
classification methods: a TCP-healthy DC the federated monitor
suspects is DEGRADED in every view.
"""

from types import SimpleNamespace

from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.nodes.gate.health_coordinator import GateHealthCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.swim.health.federated_health_monitor import DCReachability

REACHABLE_DC = "dc-a"
SUSPECTED_DC = "dc-b"


class TcpHealthy:
    """Every known DC's managers report healthy over TCP."""

    def known_datacenters(self) -> frozenset[str]:
        return frozenset({REACHABLE_DC, SUSPECTED_DC})

    def get_datacenter_health(self, datacenter_id: str) -> DatacenterStatus:
        return DatacenterStatus(dc_id=datacenter_id, health="healthy", available_capacity=8, manager_count=1)

    def get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        return {datacenter_id: self.get_datacenter_health(datacenter_id) for datacenter_id in self.known_datacenters()}


class FederatedProbes:
    def get_dc_health(self, datacenter_id: str):
        reachability = DCReachability.SUSPECTED if datacenter_id == SUSPECTED_DC else DCReachability.REACHABLE
        return SimpleNamespace(reachability=reachability, last_ack=None)


def make_gate() -> GateServer:
    tcp_health = TcpHealthy()
    coordinator = GateHealthCoordinator(
        clock=RealClock(),
        state=GateRuntimeState(),
        logger=None,
        task_runner=None,
        dc_health_manager=tcp_health,
        dc_health_monitor=FederatedProbes(),
        cross_dc_correlation=SimpleNamespace(
            register_partition_healed_callback=lambda callback: None,
            register_partition_detected_callback=lambda callback: None,
        ),
        track_manager=None,
        versioned_clock=None,
        manager_dispatcher=None,
        manager_health_config=None,
        datacenter_managers={REACHABLE_DC: [], SUSPECTED_DC: []},
        get_node_id=None,
        get_host=None,
        get_tcp_port=None,
        confirm_manager_for_dc=None,
        record_manager_heartbeat=None,
        resource_aggregator=None,
        resource_predictor=None,
    )
    gate = object.__new__(GateServer)
    gate._health_coordinator = coordinator
    gate._dc_health_manager = tcp_health
    return gate


def test_a_suspected_datacenter_is_degraded_in_every_view() -> None:
    gate = make_gate()

    assert gate._classify_datacenter_health(SUSPECTED_DC).health == "degraded"
    assert gate._classify_datacenter_health(REACHABLE_DC).health == "healthy"
    all_health = gate._get_all_datacenter_health()
    assert {dc: status.health for dc, status in all_health.items()} == {
        REACHABLE_DC: "healthy",
        SUSPECTED_DC: "degraded",
    }
    assert gate._health_coordinator.count_active_datacenters() == 2
