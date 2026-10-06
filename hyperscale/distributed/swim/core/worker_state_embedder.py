"""``WorkerStateEmbedder`` -- pickled under the namespace
``hyperscale.distributed.swim.core.state_embedder`` (see that module)."""

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Callable
from hyperscale.distributed.models import WorkerHeartbeat, ManagerHeartbeat
from hyperscale.distributed.models.coordinates import NetworkCoordinate
from hyperscale.distributed.health.tracker import HealthPiggyback

from .state_embedder_shared import _DEFAULT_CLOCK
from .state_embedder_shared import _PROBE_RTT_CACHE_MAX_SIZE


@dataclass(slots=True)
class WorkerStateEmbedder:
    """
    State embedder for Worker nodes.

    Embeds WorkerHeartbeat data in SWIM messages so managers can
    passively learn worker capacity and status.

    Also processes ManagerHeartbeat from managers to track leadership
    changes without requiring TCP acks.

    Attributes:
        get_node_id: Callable returning the node's full ID.
        get_worker_state: Callable returning current WorkerState.
        get_available_cores: Callable returning available core count.
        get_queue_depth: Callable returning pending workflow count.
        get_cpu_percent: Callable returning CPU utilization.
        get_memory_percent: Callable returning memory utilization.
        get_state_version: Callable returning state version.
        get_active_workflows: Callable returning workflow ID -> status dict.
        get_tcp_host: Callable returning TCP host address.
        get_tcp_port: Callable returning TCP port.
        on_manager_heartbeat: Optional callback for received ManagerHeartbeat.
        get_health_accepting_work: Callable returning whether worker accepts work.
        get_health_throughput: Callable returning current throughput.
        get_health_expected_throughput: Callable returning expected throughput.
        get_health_overload_state: Callable returning overload state.
    """

    get_node_id: Callable[[], str]
    get_worker_state: Callable[[], str]
    get_available_cores: Callable[[], int]
    get_total_cores: Callable[[], int]
    get_queue_depth: Callable[[], int]
    get_cpu_percent: Callable[[], float]
    get_memory_percent: Callable[[], float]
    get_state_version: Callable[[], int]
    get_active_workflows: Callable[[], dict[str, str]]
    on_manager_heartbeat: Callable[[ManagerHeartbeat, tuple[str, int]], Awaitable[None]] | None = (
        None
    )
    get_tcp_host: Callable[[], str] | None = None
    get_tcp_port: Callable[[], int] | None = None
    get_coordinate: Callable[[], NetworkCoordinate | None] | None = None
    on_peer_coordinate: Callable[[str, NetworkCoordinate, float], None] | None = None
    _probe_rtt_cache: dict[tuple[str, int], float] = field(
        default_factory=dict, init=False, repr=False
    )
    # Health piggyback fields (AD-19)
    get_health_accepting_work: Callable[[], bool] | None = None
    get_health_throughput: Callable[[], float] | None = None
    get_health_expected_throughput: Callable[[], float] | None = None
    get_health_overload_state: Callable[[], str] | None = None
    # Extension request fields (AD-26)
    get_extension_requested: Callable[[], bool] | None = None
    get_extension_reason: Callable[[], str] | None = None
    get_extension_current_progress: Callable[[], float] | None = None
    # AD-26 Issue 4: Absolute metrics fields
    get_extension_completed_items: Callable[[], int] | None = None
    get_extension_total_items: Callable[[], int] | None = None
    # AD-26: Required fields for HealthcheckExtensionRequest
    get_extension_estimated_completion: Callable[[], float] | None = None
    get_extension_active_workflow_count: Callable[[], int] | None = None
    # Phase H3 — multi-dimensional WorkflowProgressSnapshot piggyback
    get_extension_step_transitions: Callable[[], int] | None = None
    get_extension_actions_completed: Callable[[], int] | None = None
    get_extension_snapshot_time: Callable[[], float] | None = None
    # Phase F1 — workflow id the extension request is for. Required
    # for the manager's H5 multi-witness routing path.
    get_extension_workflow_id: Callable[[], str] | None = None
    # AD-19 addendum (Phase D): uniform LHM gossip across all heartbeat
    # tiers. Worker reports its raw LocalHealthMultiplier.score (0-8)
    # so cross_dc_correlation can correlate worker-tier stress
    # alongside manager/gate stress.
    get_lhm_score: Callable[[], int] | None = None
    # The version of the available core count (read with it, no await
    # between): orders this heartbeat against the worker's other reports.
    get_cores_version: Callable[[], int] | None = None

    def get_state(self) -> bytes | None:
        """Get WorkerHeartbeat to embed in SWIM messages."""
        heartbeat = WorkerHeartbeat(
            node_id=self.get_node_id(),
            state=self.get_worker_state(),
            available_cores=self.get_available_cores(),
            cores_version=self.get_cores_version() if self.get_cores_version else 0,
            total_cores=self.get_total_cores(),
            queue_depth=self.get_queue_depth(),
            cpu_percent=self.get_cpu_percent(),
            memory_percent=self.get_memory_percent(),
            version=self.get_state_version(),
            active_workflows=self.get_active_workflows(),
            tcp_host=self.get_tcp_host() if self.get_tcp_host else "",
            tcp_port=self.get_tcp_port() if self.get_tcp_port else 0,
            coordinate=self.get_coordinate() if self.get_coordinate else None,
            # Health piggyback fields
            health_accepting_work=self.get_health_accepting_work()
            if self.get_health_accepting_work
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
            # Extension request fields (AD-26)
            extension_requested=self.get_extension_requested()
            if self.get_extension_requested
            else False,
            extension_reason=self.get_extension_reason()
            if self.get_extension_reason
            else "",
            extension_current_progress=self.get_extension_current_progress()
            if self.get_extension_current_progress
            else 0.0,
            # AD-26 Issue 4: Absolute metrics fields
            extension_completed_items=self.get_extension_completed_items()
            if self.get_extension_completed_items
            else 0,
            extension_total_items=self.get_extension_total_items()
            if self.get_extension_total_items
            else 0,
            # AD-26: Required fields for HealthcheckExtensionRequest
            extension_estimated_completion=self.get_extension_estimated_completion()
            if self.get_extension_estimated_completion
            else 0.0,
            extension_active_workflow_count=self.get_extension_active_workflow_count()
            if self.get_extension_active_workflow_count
            else 0,
            # Phase H3 — multi-dimensional progress snapshot piggyback
            extension_step_transitions=self.get_extension_step_transitions()
            if self.get_extension_step_transitions
            else 0,
            extension_actions_completed=self.get_extension_actions_completed()
            if self.get_extension_actions_completed
            else 0,
            extension_snapshot_time=self.get_extension_snapshot_time()
            if self.get_extension_snapshot_time
            else 0.0,
            extension_workflow_id=self.get_extension_workflow_id()
            if self.get_extension_workflow_id
            else "",
            # AD-19 addendum (Phase D): uniform LHM
            lhm_score=self.get_lhm_score() if self.get_lhm_score else 0,
        )
        return heartbeat.dump()

    async def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """Process ManagerHeartbeat from managers to track leadership.

        Undecodable state and handler failures propagate: the SWIM server
        logs them rather than this dropping them unseen.
        """
        if self.on_manager_heartbeat:
            obj = ManagerHeartbeat.load(state_data)  # Base unpickle
            if isinstance(obj, ManagerHeartbeat):
                await self.on_manager_heartbeat(obj, source_addr)
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
            node_type="worker",
            is_alive=True,
            accepting_work=self.get_health_accepting_work()
            if self.get_health_accepting_work
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
