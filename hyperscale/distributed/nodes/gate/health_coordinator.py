"""
Gate health coordination for GateServer.

Handles datacenter health monitoring and classification:
- Manager heartbeat processing
- Datacenter health classification (AD-16, AD-33)
- Federated health monitor integration
- Backpressure signal handling (AD-37)
- Cross-DC correlation detection
"""

import asyncio
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    DatacenterHealth,
    DatacenterStatus,
    ManagerHeartbeat,
)
from hyperscale.distributed.routing import DatacenterCandidate
from hyperscale.distributed.health import ManagerHealthConfig, ManagerHealthState
from hyperscale.distributed.datacenters import (
    DatacenterHealthManager,
    CrossDCCorrelationDetector,
)
from hyperscale.distributed.capacity import DatacenterCapacityAggregator
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.resources.datacenter_resource_view import (
    DatacenterResourceView,
)
from hyperscale.distributed.slo.resource_aware_predictor import (
    ResourceAwareSLOPredictor,
)
from hyperscale.distributed.swim.health import (
    FederatedHealthMonitor,
    DCHealthState,
    DCReachability,
)
from hyperscale.distributed.reliability import (
    BackpressureLevel,
    BackpressureSignal,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

from .state import GateRuntimeState

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


RecordManagerHeartbeat = Callable[
    [str, tuple[str, int], str, int, int, bool],
    None,
]


if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.server.events.lamport_clock import VersionedStateClock
    from hyperscale.distributed.datacenters.manager_dispatcher import ManagerDispatcher
    from hyperscale.distributed.taskex import TaskRunner


class GateHealthCoordinator:
    """
    Coordinates datacenter and manager health monitoring.

    Integrates multiple health signals:
    - TCP heartbeats from managers (DatacenterHealthManager)
    - UDP probes to DC leaders (FederatedHealthMonitor)
    - Backpressure signals from managers
    - Cross-DC correlation for failure detection
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        dc_health_manager: DatacenterHealthManager,
        dc_health_monitor: FederatedHealthMonitor,
        cross_dc_correlation: CrossDCCorrelationDetector,
        track_manager: Callable[[str, tuple[str, int]], None],
        versioned_clock: "VersionedStateClock",
        manager_dispatcher: "ManagerDispatcher",
        manager_health_config: "ManagerHealthConfig",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        confirm_manager_for_dc: Callable[[str, tuple[str, int]], "asyncio.Task"],
        record_manager_heartbeat: RecordManagerHeartbeat,
        capacity_aggregator: DatacenterCapacityAggregator | None = None,
        on_partition_healed: Callable[[list[str]], None] | None = None,
        on_partition_detected: Callable[[list[str]], None] | None = None,
        *,
        resource_aggregator: DatacenterResourceAggregator,
        resource_predictor: ResourceAwareSLOPredictor,
    ) -> None:
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._dc_health_manager: DatacenterHealthManager = dc_health_manager
        self._dc_health_monitor: FederatedHealthMonitor = dc_health_monitor
        self._cross_dc_correlation: CrossDCCorrelationDetector = cross_dc_correlation
        self._track_manager: Callable[[str, tuple[str, int]], None] = track_manager
        self._versioned_clock: "VersionedStateClock" = versioned_clock
        self._manager_dispatcher: "ManagerDispatcher" = manager_dispatcher
        self._manager_health_config: "ManagerHealthConfig" = manager_health_config
        self._datacenter_managers: dict[str, list[tuple[str, int]]] = (
            datacenter_managers
        )
        self._get_node_id: Callable[[], "NodeId"] = get_node_id
        self._get_host: Callable[[], str] = get_host
        self._get_tcp_port: Callable[[], int] = get_tcp_port
        self._confirm_manager_for_dc: Callable[
            [str, tuple[str, int]], "asyncio.Task"
        ] = confirm_manager_for_dc
        self._record_manager_heartbeat: RecordManagerHeartbeat = (
            record_manager_heartbeat
        )
        self._capacity_aggregator: DatacenterCapacityAggregator | None = (
            capacity_aggregator
        )
        # AD-41: per-datacenter resource pressure from managers' reports,
        # folded into routing through the AD-42 resource-aware predictor.
        self._resource_aggregator = resource_aggregator
        self._resource_predictor = resource_predictor
        self._on_partition_healed: Callable[[list[str]], None] | None = (
            on_partition_healed
        )
        self._on_partition_detected: Callable[[list[str]], None] | None = (
            on_partition_detected
        )
        self._partitioned_datacenters: set[str] = set()

        self._cross_dc_correlation.register_partition_healed_callback(
            self._handle_partition_healed
        )
        self._cross_dc_correlation.register_partition_detected_callback(
            self._handle_partition_detected
        )

    async def handle_embedded_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle ManagerHeartbeat received via SWIM message embedding.

        Uses versioned clock to reject stale updates.

        Args:
            heartbeat: Received manager heartbeat
            source_addr: UDP source address of the heartbeat
        """
        await self.ingest_manager_heartbeat(heartbeat, source_addr)

    async def ingest_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
        manager_addr: tuple[str, int] | None = None,
        *,
        use_version_clock: bool = True,
    ) -> tuple[str, tuple[str, int]] | None:
        """Ingest a manager heartbeat into every gate-side health store.

        Manager heartbeats arrive through TCP status updates, TCP
        registration, peer-gate discovery, and SWIM piggyback data. All
        paths must update the same canonical stores or routing sees
        contradictory state: capacity can look available while the
        datacenter health manager still has zero managers.
        """
        datacenter_id = heartbeat.datacenter
        resolved_manager_addr = manager_addr or self._resolve_manager_addr(
            heartbeat, source_addr
        )
        version_key = self._manager_version_key(heartbeat, resolved_manager_addr)

        if use_version_clock and await self._is_older_manager_version(
            version_key, heartbeat.version
        ):
            return None

        self._ensure_manager_known(datacenter_id, resolved_manager_addr)
        await self._state.update_manager_status(
            datacenter_id,
            resolved_manager_addr,
            heartbeat,
            _DEFAULT_CLOCK.monotonic(),
        )

        if self._capacity_aggregator is not None:
            self._capacity_aggregator.record_heartbeat(heartbeat)

        # Only the TCP status update carries a resource report; a SWIM
        # heartbeat without one leaves the manager's last report in place.
        if heartbeat.resource_report is not None:
            self._resource_aggregator.record(
                datacenter_id, resolved_manager_addr, heartbeat.resource_report
            )

        self._record_manager_heartbeat(
            datacenter_id,
            resolved_manager_addr,
            heartbeat.node_id,
            heartbeat.version,
            heartbeat.term,
            heartbeat.is_leader,
        )
        self._add_manager_to_discovery(
            datacenter_id,
            resolved_manager_addr,
            heartbeat.node_id,
        )
        await self._update_manager_health_state(
            datacenter_id,
            resolved_manager_addr,
            heartbeat,
        )

        self._task_runner.run(
            self._confirm_manager_for_dc,
            datacenter_id,
            resolved_manager_addr,
        )
        self._dc_health_manager.update_manager(
            datacenter_id,
            resolved_manager_addr,
            heartbeat,
        )

        if heartbeat.is_leader:
            self._manager_dispatcher.set_leader(
                datacenter_id,
                resolved_manager_addr,
            )
            self._update_federated_leader(
                datacenter_id,
                resolved_manager_addr,
                heartbeat,
            )

        self._record_cross_dc_signals(datacenter_id, heartbeat)
        if use_version_clock:
            await self._versioned_clock.update_entity(version_key, heartbeat.version)

        return datacenter_id, resolved_manager_addr

    def _update_federated_leader(
        self,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        heartbeat: ManagerHeartbeat,
    ) -> None:
        """Refresh the federated probe target from the current manager leader."""
        if not heartbeat.udp_host or heartbeat.udp_port <= 0:
            return

        tcp_host = heartbeat.tcp_host or manager_addr[0]
        tcp_port = heartbeat.tcp_port or manager_addr[1]
        self._dc_health_monitor.update_leader(
            datacenter=datacenter_id,
            leader_udp_addr=(heartbeat.udp_host, heartbeat.udp_port),
            leader_tcp_addr=(tcp_host, tcp_port),
            leader_node_id=heartbeat.node_id,
            leader_term=heartbeat.term,
        )

    def _resolve_manager_addr(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
    ) -> tuple[str, int]:
        if heartbeat.tcp_host and heartbeat.tcp_port > 0:
            return (heartbeat.tcp_host, heartbeat.tcp_port)
        return source_addr

    def _manager_version_key(
        self,
        heartbeat: ManagerHeartbeat,
        manager_addr: tuple[str, int],
    ) -> str:
        if heartbeat.node_id and not heartbeat.node_id.startswith("discovered-via-"):
            return f"mgr:{heartbeat.node_id}"
        return f"mgr:{manager_addr[0]}:{manager_addr[1]}"

    async def _is_older_manager_version(
        self,
        version_key: str,
        incoming_version: int,
    ) -> bool:
        current_version = await self._versioned_clock.get_entity_version(version_key)
        return current_version is not None and incoming_version < current_version

    def _ensure_manager_known(
        self,
        datacenter_id: str,
        manager_addr: tuple[str, int],
    ) -> None:
        managers = self._datacenter_managers.setdefault(datacenter_id, [])
        if manager_addr not in managers:
            managers.append(manager_addr)

    def _add_manager_to_discovery(
        self,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        node_id: str,
    ) -> None:
        # One discovery peer per manager, keyed by address — keying it by
        # node id here too put every manager in discovery twice.
        self._track_manager(datacenter_id, manager_addr)

    async def _update_manager_health_state(
        self,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        heartbeat: ManagerHeartbeat,
    ) -> None:
        manager_key = (datacenter_id, manager_addr)
        health_state = self._state._manager_health.get(manager_key)
        if not health_state:
            health_state = ManagerHealthState(
                manager_id=heartbeat.node_id,
                datacenter_id=datacenter_id,
                config=self._manager_health_config,
            )
            self._state._manager_health[manager_key] = health_state

        has_quorum = getattr(
            heartbeat,
            "health_has_quorum",
            getattr(heartbeat, "has_quorum", True),
        )
        accepting_jobs = getattr(
            heartbeat,
            "health_accepting_jobs",
            getattr(heartbeat, "accepting_jobs", True),
        )

        await health_state.update_liveness_async(success=True)
        await health_state.update_readiness_async(
            has_quorum=has_quorum,
            accepting=accepting_jobs,
            worker_count=heartbeat.healthy_worker_count,
        )

    def _record_cross_dc_signals(
        self,
        datacenter_id: str,
        heartbeat: ManagerHeartbeat,
    ) -> None:
        if heartbeat.workers_with_extensions > 0:
            self._cross_dc_correlation.record_extension(
                datacenter_id=datacenter_id,
                worker_id=f"{datacenter_id}:{heartbeat.node_id}",
                extension_count=heartbeat.workers_with_extensions,
                reason="aggregated from manager heartbeat",
            )
        if heartbeat.lhm_score > 0:
            self._cross_dc_correlation.record_lhm_score(
                datacenter_id=datacenter_id,
                lhm_score=heartbeat.lhm_score,
                node_type="manager",
            )
        worker_lhm_score = getattr(heartbeat, "worker_max_lhm_score", 0)
        if worker_lhm_score > 0:
            self._cross_dc_correlation.record_lhm_score(
                datacenter_id=datacenter_id,
                lhm_score=worker_lhm_score,
                node_type="worker",
            )

    def record_peer_lhm_score(
        self,
        datacenter_id: str,
        lhm_score: int,
        node_type: str,
    ) -> None:
        """Record an LHM score reported by an external tier (Phase D).

        Public entry point for gate-peer ``GateHeartbeat`` LHM
        ingestion. Centralizes routing into ``cross_dc_correlation``
        so callers don't reach into private state. Skipped silently
        when ``lhm_score`` is non-positive (treat 0 as "no signal").
        """
        if lhm_score <= 0:
            return
        self._cross_dc_correlation.record_lhm_score(
            datacenter_id=datacenter_id,
            lhm_score=lhm_score,
            node_type=node_type,
        )

    def classify_datacenter_health(self, datacenter_id: str) -> DatacenterStatus:
        """
        Classify datacenter health based on TCP heartbeats and UDP probes.

        AD-33 Fix 4: Integrates FederatedHealthMonitor's UDP probe results
        with DatacenterHealthManager's TCP heartbeat data.

        Health classification combines two signals:
        1. TCP heartbeats from managers (DatacenterHealthManager)
        2. UDP probes to DC leader (FederatedHealthMonitor)

        Args:
            datacenter_id: Datacenter to classify

        Returns:
            DatacenterStatus with health classification
        """
        tcp_status = self._dc_health_manager.get_datacenter_health(datacenter_id)
        federated_health = self._dc_health_monitor.get_dc_health(datacenter_id)

        if federated_health is None:
            return tcp_status

        if federated_health.reachability == DCReachability.UNKNOWN:
            return tcp_status

        if federated_health.reachability == DCReachability.UNREACHABLE:
            return self._merge_unreachable_federated_health(
                datacenter_id,
                tcp_status,
                federated_health,
            )

        if federated_health.reachability == DCReachability.SUSPECTED:
            if tcp_status.health == DatacenterHealth.UNHEALTHY.value:
                return tcp_status

            return DatacenterStatus(
                dc_id=datacenter_id,
                health=DatacenterHealth.DEGRADED.value,
                available_capacity=tcp_status.available_capacity,
                queue_depth=tcp_status.queue_depth,
                manager_count=tcp_status.manager_count,
                worker_count=tcp_status.worker_count,
                last_update=tcp_status.last_update,
            )

        if federated_health.last_ack:
            reported_health = federated_health.last_ack.dc_health
            if (
                reported_health == "UNHEALTHY"
                and tcp_status.health != DatacenterHealth.UNHEALTHY.value
            ):
                return DatacenterStatus(
                    dc_id=datacenter_id,
                    health=DatacenterHealth.UNHEALTHY.value,
                    available_capacity=0,
                    queue_depth=tcp_status.queue_depth,
                    manager_count=federated_health.last_ack.healthy_managers,
                    worker_count=federated_health.last_ack.healthy_workers,
                    last_update=tcp_status.last_update,
                )
            if (
                reported_health == "DEGRADED"
                and tcp_status.health == DatacenterHealth.HEALTHY.value
            ):
                return DatacenterStatus(
                    dc_id=datacenter_id,
                    health=DatacenterHealth.DEGRADED.value,
                    available_capacity=federated_health.last_ack.available_cores,
                    queue_depth=tcp_status.queue_depth,
                    manager_count=federated_health.last_ack.healthy_managers,
                    worker_count=federated_health.last_ack.healthy_workers,
                    last_update=tcp_status.last_update,
                )
            if (
                reported_health == "BUSY"
                and tcp_status.health == DatacenterHealth.HEALTHY.value
            ):
                return DatacenterStatus(
                    dc_id=datacenter_id,
                    health=DatacenterHealth.BUSY.value,
                    available_capacity=federated_health.last_ack.available_cores,
                    queue_depth=tcp_status.queue_depth,
                    manager_count=federated_health.last_ack.healthy_managers,
                    worker_count=federated_health.last_ack.healthy_workers,
                    last_update=tcp_status.last_update,
                )

        return tcp_status

    def _merge_unreachable_federated_health(
        self,
        datacenter_id: str,
        tcp_status: DatacenterStatus,
        federated_health: DCHealthState,
    ) -> DatacenterStatus:
        """Apply a confirmed federated reachability failure to TCP health."""
        if not federated_health.has_successful_probe:
            return tcp_status

        if tcp_status.health in (
            DatacenterHealth.HEALTHY.value,
            DatacenterHealth.BUSY.value,
        ):
            return DatacenterStatus(
                dc_id=datacenter_id,
                health=DatacenterHealth.DEGRADED.value,
                available_capacity=tcp_status.available_capacity,
                queue_depth=tcp_status.queue_depth,
                manager_count=tcp_status.manager_count,
                worker_count=tcp_status.worker_count,
                last_update=tcp_status.last_update,
            )

        return DatacenterStatus(
            dc_id=datacenter_id,
            health=DatacenterHealth.UNHEALTHY.value,
            available_capacity=0,
            queue_depth=tcp_status.queue_depth,
            manager_count=tcp_status.manager_count,
            worker_count=0,
            last_update=tcp_status.last_update,
        )

    def get_all_datacenter_health(
        self,
        datacenter_ids: list[str],
        is_dc_ready_for_health: Callable[[str], bool],
    ) -> dict[str, DatacenterStatus]:
        """
        Get health classification for all registered datacenters.

        Only classifies DCs that have achieved READY or PARTIAL registration
        status (AD-27).

        Args:
            datacenter_ids: List of datacenter IDs to classify
            is_dc_ready_for_health: Callback to check if DC is ready for classification

        Returns:
            Dict mapping datacenter_id -> DatacenterStatus
        """
        return {
            dc_id: self.classify_datacenter_health(dc_id)
            for dc_id in datacenter_ids
            if is_dc_ready_for_health(dc_id)
        }

    def get_best_manager_heartbeat(
        self,
        datacenter_id: str,
    ) -> tuple[ManagerHeartbeat | None, int, int]:
        """
        Get the most authoritative manager heartbeat for a datacenter.

        Strategy:
        1. Prefer the LEADER's heartbeat if fresh (within 30s)
        2. Fall back to any fresh manager heartbeat
        3. Return None if no fresh heartbeats

        Args:
            datacenter_id: Datacenter to query

        Returns:
            Tuple of (best_heartbeat, alive_manager_count, total_manager_count)
        """
        manager_statuses = self._state._datacenter_manager_status.get(datacenter_id, {})
        now = _DEFAULT_CLOCK.monotonic()
        heartbeat_timeout = 30.0

        best_heartbeat: ManagerHeartbeat | None = None
        leader_heartbeat: ManagerHeartbeat | None = None
        alive_count = 0

        for manager_addr, heartbeat in manager_statuses.items():
            last_seen = self._state._manager_last_status.get(manager_addr, 0)
            is_fresh = (now - last_seen) < heartbeat_timeout

            if is_fresh:
                alive_count += 1

                if heartbeat.is_leader:
                    leader_heartbeat = heartbeat

                if best_heartbeat is None:
                    best_heartbeat = heartbeat

        if leader_heartbeat is not None:
            best_heartbeat = leader_heartbeat

        return best_heartbeat, alive_count, len(manager_statuses)

    def count_active_datacenters(self) -> int:
        count = 0
        for (
            datacenter_id,
            status,
        ) in self._dc_health_manager.get_all_datacenter_health().items():
            if status.health != DatacenterHealth.UNHEALTHY.value:
                count += 1
        return count

    def get_known_managers_for_piggyback(
        self,
    ) -> dict[str, tuple[str, int, str, int, str]]:
        """
        Get known managers for piggybacking in SWIM heartbeats.

        Returns:
            Dict mapping manager_id -> (tcp_host, tcp_port, udp_host, udp_port, datacenter)
        """
        result: dict[str, tuple[str, int, str, int, str]] = {}
        for dc_id, manager_status in self._state._datacenter_manager_status.items():
            for manager_addr, heartbeat in manager_status.items():
                if heartbeat.node_id:
                    tcp_host = heartbeat.tcp_host or manager_addr[0]
                    tcp_port = heartbeat.tcp_port or manager_addr[1]
                    udp_host = heartbeat.udp_host or manager_addr[0]
                    udp_port = heartbeat.udp_port or manager_addr[1]
                    result[heartbeat.node_id] = (
                        tcp_host,
                        tcp_port,
                        udp_host,
                        udp_port,
                        dc_id,
                    )
        return result

    def _handle_partition_healed(
        self,
        healed_datacenters: list[str],
        timestamp: float,
    ) -> None:
        self._partitioned_datacenters.clear()
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Partition healed for datacenters: {healed_datacenters}",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().full,
            ),
        )

        if self._on_partition_healed:
            try:
                self._on_partition_healed(healed_datacenters)
            except Exception as error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Partition healed callback failed: {error}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().full,
                    ),
                )

    def _handle_partition_detected(
        self,
        affected_datacenters: list[str],
        timestamp: float,
    ) -> None:
        self._partitioned_datacenters = set(affected_datacenters)
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Partition detected affecting datacenters: {affected_datacenters}",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().full,
            ),
        )

        if self._on_partition_detected:
            try:
                self._on_partition_detected(affected_datacenters)
            except Exception as error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Partition detected callback failed: {error}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().full,
                    ),
                )

    def datacenter_resource_view(self, datacenter_id: str) -> DatacenterResourceView | None:
        """AD-41: the DC's current resource pressure (None until a manager
        that knows the DC's capacity has reported)."""
        return self._resource_aggregator.view(datacenter_id)

    def _datacenter_routing_factor(self, datacenter_id: str) -> float:
        """The DC's AD-42 SLO routing factor, adjusted by its AD-41 resource
        pressure (AD-42 Part 9) when a fresh resource view exists."""
        slo_routing_factor = self._state.get_dc_slo_routing_factor(datacenter_id)
        if (view := self.datacenter_resource_view(datacenter_id)) is None:
            return slo_routing_factor
        return self._resource_predictor.predict_slo_risk(
            cpu_pressure=view.cpu_pressure,
            cpu_uncertainty=view.workload_cpu_uncertainty,
            memory_pressure=view.memory_pressure,
            memory_uncertainty=view.workload_memory_uncertainty,
            current_slo_score=slo_routing_factor,
        )

    def build_datacenter_candidates(
        self,
        datacenter_ids: list[str],
    ) -> list[DatacenterCandidate]:
        """
        Build datacenter candidates for job routing.

        Creates DatacenterCandidate objects with health and capacity info
        for the job router to use in datacenter selection.

        Integrates DatacenterCapacityAggregator (AD-43) to enrich candidates
        with aggregated capacity metrics from manager heartbeats.

        Args:
            datacenter_ids: List of datacenter IDs to build candidates for

        Returns:
            List of DatacenterCandidate objects with health/capacity metrics
        """
        candidates: list[DatacenterCandidate] = []
        for datacenter_id in datacenter_ids:
            status = self.classify_datacenter_health(datacenter_id)
            health_bucket = status.health.upper()
            if status.health == DatacenterHealth.UNHEALTHY.value:
                correlation_decision = self._cross_dc_correlation.check_correlation(
                    datacenter_id
                )
                if correlation_decision.should_delay_eviction:
                    health_bucket = DatacenterHealth.DEGRADED.value.upper()

            if datacenter_id in self._partitioned_datacenters:
                health_bucket = DatacenterHealth.DEGRADED.value.upper()

            available_cores = status.available_capacity
            total_cores = status.available_capacity + status.queue_depth
            queue_depth = status.queue_depth

            if self._capacity_aggregator is not None:
                capacity = self._capacity_aggregator.get_capacity(
                    datacenter_id, health_bucket.lower()
                )
                if capacity.total_cores > 0:
                    available_cores = capacity.available_cores
                    total_cores = capacity.total_cores
                    queue_depth = capacity.pending_workflow_count

            slo_routing_factor = self._datacenter_routing_factor(datacenter_id)
            candidates.append(
                DatacenterCandidate(
                    datacenter_id=datacenter_id,
                    health_bucket=health_bucket,
                    available_cores=available_cores,
                    total_cores=total_cores,
                    queue_depth=queue_depth,
                    lhm_multiplier=1.0,
                    circuit_breaker_pressure=0.0,
                    total_managers=status.manager_count,
                    healthy_managers=status.manager_count,
                    health_severity_weight=getattr(
                        status, "health_severity_weight", 1.0
                    ),
                    worker_overload_ratio=getattr(status, "worker_overload_ratio", 0.0),
                    overloaded_worker_count=getattr(
                        status, "overloaded_worker_count", 0
                    ),
                    slo_routing_factor=slo_routing_factor,
                )
            )
        return candidates

    def check_and_notify_partition_healed(self) -> bool:
        return self._cross_dc_correlation.check_partition_healed()

    def is_in_partition(self) -> bool:
        return self._cross_dc_correlation.is_in_partition()

    def get_time_since_partition_healed(self) -> float | None:
        return self._cross_dc_correlation.get_time_since_partition_healed()

    def legacy_select_datacenters(
        self,
        count: int,
        dc_health: dict[str, DatacenterStatus],
        datacenter_manager_count: int,
        preferred: list[str] | None = None,
    ) -> tuple[list[str], list[str], str]:
        if not dc_health:
            if datacenter_manager_count > 0:
                return ([], [], "initializing")
            return ([], [], "unhealthy")

        # An explicit ``datacenters=[...]`` list on the submission is a
        # placement CONSTRAINT (mirroring the AD-36 router): selection
        # only considers the listed datacenters. This selector
        # previously accepted ``preferred`` and never read it, so a
        # dc-pinned job could silently run anywhere.
        preferred_set = set(preferred) if preferred else None

        def _in_scope(datacenter_id: str) -> bool:
            return preferred_set is None or datacenter_id in preferred_set

        healthy = [
            dc
            for dc, status in dc_health.items()
            if status.health == DatacenterHealth.HEALTHY.value and _in_scope(dc)
        ]
        busy = [
            dc
            for dc, status in dc_health.items()
            if status.health == DatacenterHealth.BUSY.value and _in_scope(dc)
        ]
        degraded = [
            dc
            for dc, status in dc_health.items()
            if status.health == DatacenterHealth.DEGRADED.value and _in_scope(dc)
        ]

        if healthy:
            worst_health = "healthy"
        elif busy:
            worst_health = "busy"
        elif degraded:
            worst_health = "degraded"
        else:
            # Nothing usable. Distinguish "still coming up" from "down":
            # any in-scope datacenter in its pre-first-heartbeat window
            # makes the aggregate INITIALIZING (submissions rejected as
            # transient, clients retry) rather than UNHEALTHY (jobs
            # failed). Scoped to the constraint so a pinned job's
            # rejection reflects the PINNED datacenters' state, not an
            # unrelated datacenter that happens to be booting.
            initializing = any(
                status.health == DatacenterHealth.INITIALIZING.value
                for dc, status in dc_health.items()
                if _in_scope(dc)
            )
            return ([], [], "initializing" if initializing else "unhealthy")

        all_usable = healthy + busy + degraded
        primary = all_usable[:count]
        fallback = all_usable[count:]

        return (primary, fallback, worst_health)
