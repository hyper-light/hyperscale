"""``ManagerStateEmbedder`` -- pickled under the namespace
``hyperscale.distributed.swim.core.state_embedder`` (see that module)."""

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.models import WorkerHeartbeat, ManagerHeartbeat, GateHeartbeat
from hyperscale.distributed.models.coordinates import NetworkCoordinate
from hyperscale.distributed.health.tracker import HealthPiggyback

from .state_embedder_shared import _DEFAULT_CLOCK
from .state_embedder_shared import _PROBE_RTT_CACHE_MAX_SIZE


@dataclass(slots=True)
class ManagerStateEmbedder:
    """
    State embedder for Manager nodes.

    Embeds ManagerHeartbeat data and processes:
    - WorkerHeartbeat from workers
    - ManagerHeartbeat from peer managers
    - GateHeartbeat from gates

    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_datacenter: Callable returning datacenter ID.
        is_leader: Callable returning leadership status.
        get_term: Callable returning current leadership term.
        get_state_version: Callable returning state version.
        get_active_jobs: Callable returning active job count.
        get_active_workflows: Callable returning active workflow count.
        get_worker_count: Callable returning registered worker count.
        get_available_cores: Callable returning total available cores.
        get_manager_state: Callable returning ManagerState value (syncing/active).
        get_tcp_host: Callable returning TCP host address.
        get_tcp_port: Callable returning TCP port.
        get_udp_host: Callable returning UDP host address.
        get_udp_port: Callable returning UDP port.
        on_worker_heartbeat: Callable to handle received WorkerHeartbeat.
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat from peers.
        on_gate_heartbeat: Callable to handle received GateHeartbeat from gates.
        get_health_accepting_jobs: Callable returning whether manager accepts jobs.
        get_health_has_quorum: Callable returning whether manager has quorum.
        get_health_throughput: Callable returning current throughput.
        get_health_expected_throughput: Callable returning expected throughput.
        get_health_overload_state: Callable returning overload state.
    """

    get_node_id: Callable[[], str]
    get_datacenter: Callable[[], str]
    is_leader: Callable[[], bool]
    get_term: Callable[[], int]
    get_state_version: Callable[[], int]
    get_active_jobs: Callable[[], int]
    get_active_workflows: Callable[[], int]
    get_worker_count: Callable[[], int]
    get_healthy_worker_count: Callable[[], int]
    get_available_cores: Callable[[], int]
    get_total_cores: Callable[[], int]
    on_worker_heartbeat: Callable[[WorkerHeartbeat, tuple[str, int]], Awaitable[None]]
    on_manager_heartbeat: Callable[[ManagerHeartbeat, tuple[str, int]], Awaitable[None]] | None = (
        None
    )
    on_gate_heartbeat: Callable[[GateHeartbeat, tuple[str, int]], Awaitable[None]] | None = None
    get_manager_state: Callable[[], str] | None = None
    get_tcp_host: Callable[[], str] | None = None
    get_tcp_port: Callable[[], int] | None = None
    get_udp_host: Callable[[], str] | None = None
    get_udp_port: Callable[[], int] | None = None
    get_coordinate: Callable[[], NetworkCoordinate | None] | None = None
    on_peer_coordinate: Callable[[str, NetworkCoordinate, float], None] | None = None
    _probe_rtt_cache: dict[tuple[str, int], float] = field(
        default_factory=dict, init=False, repr=False
    )
    # Health piggyback fields (AD-19)
    get_health_accepting_jobs: Callable[[], bool] | None = None
    get_health_has_quorum: Callable[[], bool] | None = None
    get_health_throughput: Callable[[], float] | None = None
    get_health_expected_throughput: Callable[[], float] | None = None
    get_health_overload_state: Callable[[], str] | None = None
    # Gate leader tracking for propagation among managers
    get_current_gate_leader_id: Callable[[], str | None] | None = None
    get_current_gate_leader_host: Callable[[], str | None] | None = None
    get_current_gate_leader_port: Callable[[], int | None] | None = None
    get_known_gates: Callable[[], dict[str, tuple[str, int, str, int]]] | None = None
    # Job leadership tracking for worker notification
    get_job_leaderships: Callable[[], dict[str, tuple[int, int]]] | None = None
    # Whether the manager's durable storage can take its writes (gates
    # route around a manager that cannot)
    get_storage_writable: Callable[[], bool] | None = None

    def get_state(self) -> bytes | None:
        """Get ManagerHeartbeat to embed in SWIM messages."""
        heartbeat = ManagerHeartbeat(
            node_id=self.get_node_id(),
            datacenter=self.get_datacenter(),
            is_leader=self.is_leader(),
            term=self.get_term(),
            version=self.get_state_version(),
            active_jobs=self.get_active_jobs(),
            active_workflows=self.get_active_workflows(),
            worker_count=self.get_worker_count(),
            healthy_worker_count=self.get_healthy_worker_count(),
            available_cores=self.get_available_cores(),
            total_cores=self.get_total_cores(),
            state=self.get_manager_state() if self.get_manager_state else "active",
            tcp_host=self.get_tcp_host() if self.get_tcp_host else "",
            tcp_port=self.get_tcp_port() if self.get_tcp_port else 0,
            udp_host=self.get_udp_host() if self.get_udp_host else "",
            udp_port=self.get_udp_port() if self.get_udp_port else 0,
            coordinate=self.get_coordinate() if self.get_coordinate else None,
            # Health piggyback fields
            health_accepting_jobs=self.get_health_accepting_jobs()
            if self.get_health_accepting_jobs
            else True,
            health_has_quorum=self.get_health_has_quorum()
            if self.get_health_has_quorum
            else True,
            health_throughput=self.get_health_throughput()
            if self.get_health_throughput
            else 0.0,
            health_expected_throughput=self.get_health_expected_throughput()
            if self.get_health_expected_throughput
            else 0.0,
            health_overload_state=self.get_health_overload_state()
            if self.get_health_overload_state
            else "healthy",
            # Gate leader tracking for propagation among managers
            current_gate_leader_id=self.get_current_gate_leader_id()
            if self.get_current_gate_leader_id
            else None,
            current_gate_leader_host=self.get_current_gate_leader_host()
            if self.get_current_gate_leader_host
            else None,
            current_gate_leader_port=self.get_current_gate_leader_port()
            if self.get_current_gate_leader_port
            else None,
            known_gates=self.get_known_gates() if self.get_known_gates else {},
            # Job leadership for worker notification
            job_leaderships=self.get_job_leaderships()
            if self.get_job_leaderships
            else {},
            storage_writable=self.get_storage_writable()
            if self.get_storage_writable
            else True,
        )
        return heartbeat.dump()

    async def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from workers, peer managers, or gates.

        Undecodable state and handler failures propagate: the SWIM server
        logs them rather than this dropping them unseen.
        """
        # Unpickle once and dispatch based on actual type
        # This is necessary because load() doesn't validate type - it returns
        # whatever was pickled regardless of which class's load() was called
        obj = WorkerHeartbeat.load(state_data)  # Base unpickle

        manager_handler = self.on_manager_heartbeat
        gate_handler = self.on_gate_heartbeat

        if isinstance(obj, WorkerHeartbeat):
            await self.on_worker_heartbeat(obj, source_addr)
            if self.on_peer_coordinate and obj.coordinate:
                rtt_ms = self._probe_rtt_cache.pop(source_addr, None)
                if rtt_ms is not None:
                    self.on_peer_coordinate(obj.node_id, obj.coordinate, rtt_ms)
        elif isinstance(obj, ManagerHeartbeat) and manager_handler:
            if obj.node_id != self.get_node_id():
                await manager_handler(obj, source_addr)
                if self.on_peer_coordinate and obj.coordinate:
                    rtt_ms = self._probe_rtt_cache.pop(source_addr, None)
                    if rtt_ms is not None:
                        self.on_peer_coordinate(obj.node_id, obj.coordinate, rtt_ms)
        elif isinstance(obj, GateHeartbeat) and gate_handler:
            await gate_handler(obj, source_addr)
            if self.on_peer_coordinate and obj.coordinate:
                rtt_ms = self._probe_rtt_cache.pop(source_addr, None)
                if rtt_ms is not None:
                    self.on_peer_coordinate(obj.node_id, obj.coordinate, rtt_ms)

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """
        Get HealthPiggyback for gossip dissemination (Phase 6.1).

        Returns compact health state for O(log n) propagation on all SWIM
        messages, not just ACKs.
        """
        return HealthPiggyback(
            node_id=self.get_node_id(),
            node_type="manager",
            is_alive=True,
            accepting_work=self.get_health_accepting_jobs()
            if self.get_health_accepting_jobs
            else True,
            capacity=self.get_available_cores(),
            throughput=self.get_health_throughput()
            if self.get_health_throughput
            else 0.0,
            expected_throughput=self.get_health_expected_throughput()
            if self.get_health_expected_throughput
            else 0.0,
            overload_state=self.get_health_overload_state()
            if self.get_health_overload_state
            else "healthy",
            timestamp=_DEFAULT_CLOCK.monotonic(),
        )

    def record_probe_rtt(self, source_addr: tuple[str, int], rtt_ms: float) -> None:
        # Enforce max cache size to prevent unbounded memory growth
        if len(self._probe_rtt_cache) >= _PROBE_RTT_CACHE_MAX_SIZE:
            # Remove oldest entry (first key in dict)
            oldest_key = next(iter(self._probe_rtt_cache))
            del self._probe_rtt_cache[oldest_key]
        self._probe_rtt_cache[source_addr] = rtt_ms
