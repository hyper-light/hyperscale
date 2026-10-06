"""``GateStateEmbedder`` -- pickled under the namespace
``hyperscale.distributed.swim.core.state_embedder`` (see that module)."""

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.models import ManagerHeartbeat, GateHeartbeat
from hyperscale.distributed.models.coordinates import NetworkCoordinate
from hyperscale.distributed.health.tracker import HealthPiggyback
from typing import cast

from .state_embedder_shared import _DEFAULT_CLOCK
from .state_embedder_shared import _PROBE_RTT_CACHE_MAX_SIZE


@dataclass(slots=True)
class GateStateEmbedder:
    """
    State embedder for Gate nodes.

    Embeds GateHeartbeat data and processes:
    - ManagerHeartbeat from datacenter managers
    - GateHeartbeat from peer gates

    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_datacenter: Callable returning datacenter ID.
        is_leader: Callable returning leadership status.
        get_term: Callable returning current leadership term.
        get_state_version: Callable returning state version.
        get_gate_state: Callable returning GateState value.
        get_active_jobs: Callable returning active job count.
        get_active_datacenters: Callable returning active datacenter count.
        get_manager_count: Callable returning registered manager count.
        get_tcp_host: Callable returning TCP host for routing.
        get_tcp_port: Callable returning TCP port for routing.
        on_manager_heartbeat: Callable to handle received ManagerHeartbeat.
        on_gate_heartbeat: Callable to handle received GateHeartbeat from peers.
        get_known_managers: Callable returning piggybacked manager info.
        get_known_gates: Callable returning piggybacked gate info.
        get_job_leaderships: Callable returning job leadership info (like managers).
        get_job_dc_managers: Callable returning per-DC manager leaders for each job.
        get_health_has_dc_connectivity: Callable returning DC connectivity status.
        get_health_connected_dc_count: Callable returning connected DC count.
        get_health_throughput: Callable returning current throughput.
        get_health_expected_throughput: Callable returning expected throughput.
        get_health_overload_state: Callable returning overload state.
    """

    # Required fields (no defaults) - must come first
    get_node_id: Callable[[], str]
    get_datacenter: Callable[[], str]
    is_leader: Callable[[], bool]
    get_term: Callable[[], int]
    get_state_version: Callable[[], int]
    get_gate_state: Callable[[], str]
    get_active_jobs: Callable[[], int]
    get_active_datacenters: Callable[[], int]
    get_manager_count: Callable[[], int]
    on_manager_heartbeat: Callable[[ManagerHeartbeat, tuple[str, int]], Awaitable[None]]
    # Optional fields (with defaults)
    get_tcp_host: Callable[[], str] | None = None
    get_tcp_port: Callable[[], int] | None = None
    get_coordinate: Callable[[], NetworkCoordinate | None] | None = None
    on_peer_coordinate: Callable[[str, NetworkCoordinate, float], None] | None = None
    _probe_rtt_cache: dict[tuple[str, int], float] = field(
        default_factory=dict, init=False, repr=False
    )
    on_gate_heartbeat: Callable[[GateHeartbeat, tuple[str, int]], Awaitable[None]] | None = None
    # Piggybacking callbacks for discovery
    get_known_managers: (
        Callable[[], dict[str, tuple[str, int, str, int, str]]] | None
    ) = None
    get_known_gates: Callable[[], dict[str, tuple[str, int, str, int]]] | None = None
    # Job leadership piggybacking (like managers - Serf-style consistency)
    get_job_leaderships: Callable[[], dict[str, tuple[int, int]]] | None = None
    get_job_dc_managers: Callable[[], dict[str, dict[str, tuple[str, int]]]] | None = (
        None
    )
    # Health piggyback fields (AD-19)
    get_health_has_dc_connectivity: Callable[[], bool] | None = None
    get_health_connected_dc_count: Callable[[], int] | None = None
    get_health_throughput: Callable[[], float] | None = None
    get_health_expected_throughput: Callable[[], float] | None = None
    get_health_overload_state: Callable[[], str] | None = None
    # AD-19 addendum (Phase D): uniform LHM gossip across all heartbeat
    # tiers. Gate reports its raw LocalHealthMultiplier.score (0-8) so
    # cross_dc_correlation can correlate gate-tier stress alongside
    # manager/worker LHM and classify systemic load patterns.
    get_lhm_score: Callable[[], int] | None = None

    def get_state(self) -> bytes | None:
        """Get GateHeartbeat to embed in SWIM messages."""
        # Build piggybacked discovery info
        known_managers: dict[str, tuple[str, int, str, int, str]] = {}
        if self.get_known_managers:
            known_managers = self.get_known_managers()

        known_gates: dict[str, tuple[str, int, str, int]] = {}
        if self.get_known_gates:
            known_gates = self.get_known_gates()

        # Build job leadership piggybacking (Serf-style like managers)
        job_leaderships: dict[str, tuple[int, int]] = {}
        if self.get_job_leaderships:
            job_leaderships = self.get_job_leaderships()

        job_dc_managers: dict[str, dict[str, tuple[str, int]]] = {}
        if self.get_job_dc_managers:
            job_dc_managers = self.get_job_dc_managers()

        heartbeat = GateHeartbeat(
            node_id=self.get_node_id(),
            datacenter=self.get_datacenter(),
            is_leader=self.is_leader(),
            term=self.get_term(),
            version=self.get_state_version(),
            state=self.get_gate_state(),
            active_jobs=self.get_active_jobs(),
            active_datacenters=self.get_active_datacenters(),
            manager_count=self.get_manager_count(),
            tcp_host=self.get_tcp_host() if self.get_tcp_host else "",
            tcp_port=self.get_tcp_port() if self.get_tcp_port else 0,
            coordinate=self.get_coordinate() if self.get_coordinate else None,
            known_managers=known_managers,
            known_gates=known_gates,
            # Job leadership piggybacking (Serf-style like managers)
            job_leaderships=job_leaderships,
            job_dc_managers=job_dc_managers,
            # Health piggyback fields
            health_has_dc_connectivity=self.get_health_has_dc_connectivity()
            if self.get_health_has_dc_connectivity
            else True,
            health_connected_dc_count=self.get_health_connected_dc_count()
            if self.get_health_connected_dc_count
            else 0,
            health_throughput=self.get_health_throughput()
            if self.get_health_throughput
            else 0.0,
            health_expected_throughput=self.get_health_expected_throughput()
            if self.get_health_expected_throughput
            else 0.0,
            health_overload_state=self.get_health_overload_state()
            if self.get_health_overload_state
            else "healthy",
            # AD-19 addendum (Phase D): uniform LHM
            lhm_score=self.get_lhm_score() if self.get_lhm_score else 0,
        )
        return heartbeat.dump()

    async def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process embedded state from managers or peer gates.

        Undecodable state and handler failures propagate: the SWIM server
        logs them rather than this dropping them unseen.
        """

        # Unpickle once and dispatch based on actual type
        obj = cast(
            ManagerHeartbeat | GateHeartbeat, ManagerHeartbeat.load(state_data)
        )  # Base unpickle

        handler = self.on_gate_heartbeat

        if isinstance(obj, ManagerHeartbeat):
            await self.on_manager_heartbeat(obj, source_addr)
            if self.on_peer_coordinate and obj.coordinate:
                rtt_ms = self._probe_rtt_cache.pop(source_addr, None)
                if rtt_ms is not None:
                    self.on_peer_coordinate(obj.node_id, obj.coordinate, rtt_ms)
        elif isinstance(obj, GateHeartbeat) and handler:
            if obj.node_id != self.get_node_id():
                await handler(obj, source_addr)
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
        # Gates use connected DC count as capacity metric
        connected_dcs = (
            self.get_health_connected_dc_count()
            if self.get_health_connected_dc_count
            else 0
        )

        return HealthPiggyback(
            node_id=self.get_node_id(),
            node_type="gate",
            is_alive=True,
            accepting_work=self.get_health_has_dc_connectivity()
            if self.get_health_has_dc_connectivity
            else True,
            capacity=connected_dcs,
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
