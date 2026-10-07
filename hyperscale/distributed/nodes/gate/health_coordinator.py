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
from dataclasses import replace
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
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.resources.datacenter_resource_view import (
    DatacenterResourceView,
)
from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
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

from hyperscale.distributed.runtime import Clock



RecordManagerHeartbeat = Callable[
    [str, tuple[str, int], str, int, int, bool],
    None,
]


# Manager heartbeat fields only the manager's TCP status report carries: a
# heartbeat embedded in SWIM leaves them at their defaults -- its UDP
# budget has no room for them (swim/core/state_embedder.py). The resource
# report is the one more, and a SWIM heartbeat's None already leaves the
# last one in place.
TCP_REPORTED_MANAGER_FIELDS = (
    "cluster_id",
    "environment_id",
    "overloaded_worker_count",
    "stressed_worker_count",
    "busy_worker_count",
    "lhm_score",
    "worker_max_lhm_score",
    "pending_workflow_count",
    "pending_duration_seconds",
    "active_remaining_seconds",
    "cores_freeing_schedule",
    "slo_p50_ms",
    "slo_p95_ms",
    "slo_p99_ms",
    "slo_sample_count",
    "slo_compliance_score",
    "slo_routing_factor",
    "slo_updated_at",
)

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.server.events.lamport_clock import VersionedStateClock
    from hyperscale.distributed.taskex import TaskRunner


# Health a datacenter's latency SLO can grade it down to, worst last.
_SLO_GRADED_HEALTH_SEVERITY: dict[str, int] = {
    DatacenterHealth.HEALTHY.value: 0,
    DatacenterHealth.BUSY.value: 1,
    DatacenterHealth.DEGRADED.value: 2,
    DatacenterHealth.UNHEALTHY.value: 3,
}


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
        manager_health_config: "ManagerHealthConfig",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        confirm_manager_for_dc: Callable[[str, tuple[str, int]], "asyncio.Task"],
        record_manager_heartbeat: RecordManagerHeartbeat,
        clock: Clock,
        on_partition_healed: Callable[[list[str]], None] | None = None,
        on_partition_detected: Callable[[list[str]], None] | None = None,
        *,
        capacity_aggregator: DatacenterCapacityAggregator,
        circuit_breaker_manager: CircuitBreakerManager,
        resource_aggregator: DatacenterResourceAggregator,
        resource_predictor: ResourceAwareSLOPredictor,
        latency_slo: LatencySLO,
        slo_health_classifier: SLOHealthClassifier,
    ) -> None:
        self._clock: Clock = clock
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._dc_health_manager: DatacenterHealthManager = dc_health_manager
        self._dc_health_monitor: FederatedHealthMonitor = dc_health_monitor
        self._cross_dc_correlation: CrossDCCorrelationDetector = cross_dc_correlation
        self._track_manager: Callable[[str, tuple[str, int]], None] = track_manager
        self._versioned_clock: "VersionedStateClock" = versioned_clock
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
        self._capacity_aggregator: DatacenterCapacityAggregator = capacity_aggregator
        self._circuit_breaker_manager: CircuitBreakerManager = circuit_breaker_manager
        # AD-41: per-datacenter resource pressure from managers' reports,
        # folded into routing through the AD-42 resource-aware predictor.
        self._resource_aggregator = resource_aggregator
        self._resource_predictor = resource_predictor
        # AD-42: a datacenter missing its latency SLO for long enough is
        # graded down by it (composite health: the worse of the managers'
        # health and the SLO's).
        self._latency_slo = latency_slo
        self._slo_health_classifier = slo_health_classifier
        self._on_partition_healed: Callable[[list[str]], None] | None = (
            on_partition_healed
        )
        self._on_partition_detected: Callable[[list[str]], None] | None = (
            on_partition_detected
        )
        self._partitioned_datacenters: set[str] = set()
        # Each manager's last full (TCP) report and when it arrived, kept
        # while it is fresh: a SWIM heartbeat carries its fields on.
        self._manager_reports: dict[tuple[str, int], tuple[ManagerHeartbeat, float]] = {}

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

        Uses versioned clock to reject stale updates. The heartbeat leaves
        the fields only the TCP report carries at their defaults
        (``TCP_REPORTED_MANAGER_FIELDS``): ingested as it arrived, every
        SWIM heartbeat between two reports -- several a second -- zeroed
        the manager's capacity backlog, worker health and SLO at the gate,
        at the same version. It carries them on from the manager's last
        report while that is fresh; once none is, it says what SWIM knows.

        Args:
            heartbeat: Received manager heartbeat
            source_addr: UDP source address of the heartbeat
        """
        manager_addr = self._resolve_manager_addr(heartbeat, source_addr)
        if (report := self._manager_reports.get(manager_addr)) is not None:
            last_report, reported_at = report
            if (
                self._clock.monotonic() - reported_at
                <= self._capacity_aggregator.staleness_threshold_seconds
            ):
                heartbeat = replace(
                    heartbeat,
                    **{
                        field_name: getattr(last_report, field_name)
                        for field_name in TCP_REPORTED_MANAGER_FIELDS
                    },
                )
            else:
                del self._manager_reports[manager_addr]
        await self.ingest_manager_heartbeat(heartbeat, source_addr, embedded=True)

    async def ingest_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
        manager_addr: tuple[str, int] | None = None,
        *,
        use_version_clock: bool = True,
        embedded: bool = False,
    ) -> tuple[str, tuple[str, int]] | None:
        """Ingest a manager heartbeat into every gate-side health store.

        Manager heartbeats arrive through TCP status updates, TCP
        registration, peer-gate discovery, and SWIM piggyback data. All
        paths must update the same canonical stores or routing sees
        contradictory state: capacity can look available while the
        datacenter health manager still has zero managers. A heartbeat
        not ``embedded`` in SWIM is a full report: SWIM heartbeats carry
        its TCP-only fields on while it is fresh.
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
        if not embedded:
            now = self._clock.monotonic()
            staleness_threshold_seconds = self._capacity_aggregator.staleness_threshold_seconds
            self._manager_reports = {
                reporting_addr: report
                for reporting_addr, report in self._manager_reports.items()
                if now - report[1] <= staleness_threshold_seconds
            }
            self._manager_reports[resolved_manager_addr] = (heartbeat, now)
        await self._state.update_manager_status(
            datacenter_id,
            resolved_manager_addr,
            heartbeat,
            self._clock.monotonic(),
        )

        self._capacity_aggregator.record_heartbeat(heartbeat)

        # Only the TCP status update carries a resource report; a SWIM
        # heartbeat without one leaves the manager's last report in place.
        if heartbeat.resource_report is not None:
            # Straight from its manager: made as it was sent.
            self._resource_aggregator.record(
                datacenter_id,
                resolved_manager_addr,
                heartbeat.resource_report,
                age_seconds=0.0,
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

        await health_state.update_liveness_async(success=True)
        await health_state.update_readiness_async(
            has_quorum=heartbeat.health_has_quorum,
            accepting=heartbeat.health_accepting_jobs,
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
        if heartbeat.worker_max_lhm_score > 0:
            self._cross_dc_correlation.record_lhm_score(
                datacenter_id=datacenter_id,
                lhm_score=heartbeat.worker_max_lhm_score,
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
        """Classify a datacenter's health: the worse of what its managers'
        heartbeats and probes show and what its latency SLO compliance
        does (AD-42). An initializing datacenter is judged by reachability
        alone; one with too few latency observations to judge is not
        graded by them."""
        status = self._classify_datacenter_reachability(datacenter_id)
        if status.health not in _SLO_GRADED_HEALTH_SEVERITY:
            return status
        observation = self._state.get_dc_latency_observation(datacenter_id)
        if observation is None or observation.sample_count < self._latency_slo.min_sample_count:
            self._slo_health_classifier.forget(datacenter_id)
            return status
        slo_health = self._slo_health_classifier.compute_health_signal(
            datacenter_id,
            self._latency_slo,
            observation,
            self._clock.monotonic(),
        ).lower()
        if _SLO_GRADED_HEALTH_SEVERITY[slo_health] > _SLO_GRADED_HEALTH_SEVERITY[status.health]:
            return replace(status, health=slo_health)
        return status

    def _classify_datacenter_reachability(self, datacenter_id: str) -> DatacenterStatus:
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
                tcp_status,
                federated_health,
            )

        # A merged status keeps every TCP-derived field it does not
        # override: building a fresh status here reset the overload
        # signals (health severity weight, overload ratios) to their
        # neutral defaults, so a suspected datacenter shed its overload
        # penalty in routing.
        if federated_health.reachability == DCReachability.SUSPECTED:
            if tcp_status.health == DatacenterHealth.UNHEALTHY.value:
                return tcp_status

            return replace(tcp_status, health=DatacenterHealth.DEGRADED.value)

        if federated_health.last_ack:
            reported_health = federated_health.last_ack.dc_health
            if (
                reported_health == "UNHEALTHY"
                and tcp_status.health != DatacenterHealth.UNHEALTHY.value
            ):
                return replace(
                    tcp_status,
                    health=DatacenterHealth.UNHEALTHY.value,
                    available_capacity=0,
                    manager_count=federated_health.last_ack.healthy_managers,
                    worker_count=federated_health.last_ack.healthy_workers,
                )
            if (
                reported_health == "DEGRADED"
                and tcp_status.health == DatacenterHealth.HEALTHY.value
            ):
                return replace(
                    tcp_status,
                    health=DatacenterHealth.DEGRADED.value,
                    available_capacity=federated_health.last_ack.available_cores,
                    manager_count=federated_health.last_ack.healthy_managers,
                    worker_count=federated_health.last_ack.healthy_workers,
                )
            if (
                reported_health == "BUSY"
                and tcp_status.health == DatacenterHealth.HEALTHY.value
            ):
                return replace(
                    tcp_status,
                    health=DatacenterHealth.BUSY.value,
                    available_capacity=federated_health.last_ack.available_cores,
                    manager_count=federated_health.last_ack.healthy_managers,
                    worker_count=federated_health.last_ack.healthy_workers,
                )

        return tcp_status

    def _merge_unreachable_federated_health(
        self,
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
            return replace(tcp_status, health=DatacenterHealth.DEGRADED.value)

        return replace(
            tcp_status,
            health=DatacenterHealth.UNHEALTHY.value,
            available_capacity=0,
            worker_count=0,
        )

    def get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        """Every known datacenter's health, classified as
        classify_datacenter_health does (TCP heartbeats merged with the
        federated UDP probes) -- the one view routing, admission, ping and
        the active-DC count share."""
        return {
            datacenter_id: self.classify_datacenter_health(datacenter_id)
            for datacenter_id in self._dc_health_manager.known_datacenters()
        }

    def get_best_manager_heartbeat(
        self,
        datacenter_id: str,
    ) -> tuple[ManagerHeartbeat | None, int, int]:
        """
        Get the most authoritative manager heartbeat for a datacenter.

        Strategy:
        1. Prefer the LEADER's heartbeat while its manager is not suspected
        2. Fall back to any fresh manager heartbeat
        3. Return None if no fresh heartbeats

        Args:
            datacenter_id: Datacenter to query

        Returns:
            Tuple of (best_heartbeat, alive_manager_count, total_manager_count)
        """
        # One judgement of a manager's liveness: the datacenter health
        # manager's phi-accrual detectors (AD-52 section 8).
        return self._dc_health_manager.get_best_manager_heartbeat(datacenter_id)

    def count_active_datacenters(self) -> int:
        """Datacenters this gate reaches: their managers' heartbeats arrive.
        One whose heartbeats stopped (UNHEALTHY) or never began
        (INITIALIZING) is not reached."""
        return sum(
            1
            for status in self.get_all_datacenter_health().values()
            if status.health
            not in (DatacenterHealth.UNHEALTHY.value, DatacenterHealth.INITIALIZING.value)
        )

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
        # Uncertainties on the pressures' own scale: a share of capacity.
        # Without a capacity to measure against, nothing is known.
        return self._resource_predictor.predict_slo_risk(
            cpu_pressure=view.cpu_pressure,
            cpu_uncertainty_pressure=self._uncertainty_share(
                view.workload_cpu_uncertainty,
                view.cpu_capacity_percent,
            ),
            memory_pressure=view.memory_pressure,
            memory_uncertainty_pressure=self._uncertainty_share(
                view.workload_memory_uncertainty,
                view.memory_capacity_bytes,
            ),
            current_slo_score=slo_routing_factor,
        )

    @staticmethod
    def _uncertainty_share(uncertainty: float, capacity: float) -> float:
        """AD-42 Part 9: an uncertainty as a share of capacity; infinite when no
        capacity is known to measure it against."""
        return uncertainty / capacity if capacity > 0 else float("inf")

    def build_datacenter_candidates(
        self,
        datacenter_ids: list[str],
    ) -> list[DatacenterCandidate]:
        """
        Build the router's view of each datacenter.

        Health is the merged classification (TCP heartbeats and federated
        probes), held at DEGRADED while a correlated failure or a partition
        makes an UNHEALTHY verdict suspect. Capacity is the datacenter's
        AD-43 aggregate. Managers are those the gate dispatches to; the
        ones whose circuit is open are the circuit-breaker pressure.
        """
        return [
            self._build_datacenter_candidate(datacenter_id)
            for datacenter_id in datacenter_ids
        ]

    def _build_datacenter_candidate(self, datacenter_id: str) -> DatacenterCandidate:
        """One datacenter's router view: its held health bucket, AD-43 capacity
        and circuit-breaker pressure."""
        status = self.classify_datacenter_health(datacenter_id)
        health_bucket = self._datacenter_health_bucket(datacenter_id, status)

        capacity = self._capacity_aggregator.get_capacity(datacenter_id)
        managers = self._datacenter_managers.get(datacenter_id, [])
        open_circuit_count = self._circuit_breaker_manager.count_open_circuits(
            managers
        )
        return DatacenterCandidate(
            datacenter_id=datacenter_id,
            health_bucket=health_bucket,
            available_cores=capacity.available_cores,
            total_cores=capacity.total_cores,
            queue_depth=capacity.pending_workflow_count,
            total_managers=len(managers),
            healthy_managers=len(managers) - open_circuit_count,
            circuit_breaker_pressure=(
                open_circuit_count / len(managers) if managers else 0.0
            ),
            health_severity_weight=status.health_severity_weight,
            slo_routing_factor=self._datacenter_routing_factor(datacenter_id),
        )

    def _datacenter_health_bucket(
        self,
        datacenter_id: str,
        status: DatacenterStatus,
    ) -> str:
        """The routing health bucket: DEGRADED while a correlated failure delays
        an UNHEALTHY eviction or the datacenter is partitioned (AD-33 Part 6)."""
        if (
            self._correlation_delays_eviction(datacenter_id, status)
            or datacenter_id in self._partitioned_datacenters
        ):
            return DatacenterHealth.DEGRADED.value.upper()
        return status.health.upper()

    def _correlation_delays_eviction(
        self,
        datacenter_id: str,
        status: DatacenterStatus,
    ) -> bool:
        """Whether an UNHEALTHY datacenter's eviction waits on a correlated cross-DC failure."""
        return (
            status.health == DatacenterHealth.UNHEALTHY.value
            and self._cross_dc_correlation.check_correlation(
                datacenter_id
            ).should_delay_eviction
        )

    def sample_datacenter_correlation(self) -> None:
        """Give the cross-datacenter correlation detector one sample of
        every datacenter's merged health -- an UNHEALTHY datacenter is a
        failure, a reachable one a recovery, one never heard from yet no
        evidence -- then check whether a partition it detected has healed.

        The detector confirms a failure, and a recovery, only over time
        (AD-33 Part 6): it must hear each datacenter's health as often as
        that can change, not only its transitions. Unfed, it never saw a
        correlated failure, so a suspect datacenter was never held at
        DEGRADED during one (AD-36 then took a network-wide blip for lost
        datacenters); and a partition it detected never healed -- its
        datacenters stayed held at DEGRADED for good.
        """
        for datacenter_id, managers in self._datacenter_managers.items():
            self._sample_datacenter_health(datacenter_id, managers)
        if self._cross_dc_correlation.is_in_partition():
            self._cross_dc_correlation.check_partition_healed()

    def _sample_datacenter_health(
        self,
        datacenter_id: str,
        managers: list[tuple[str, int]],
    ) -> None:
        """Feed one datacenter's merged health to the correlation detector: UNHEALTHY
        is a failure, a reachable health a recovery, anything else no evidence."""
        match self.classify_datacenter_health(datacenter_id).health:
            case DatacenterHealth.UNHEALTHY.value:
                self._cross_dc_correlation.record_failure(
                    datacenter_id, "unhealthy", len(managers)
                )
            case (
                DatacenterHealth.HEALTHY.value
                | DatacenterHealth.BUSY.value
                | DatacenterHealth.DEGRADED.value
            ):
                self._cross_dc_correlation.record_recovery(datacenter_id)

    def check_and_notify_partition_healed(self) -> bool:
        return self._cross_dc_correlation.check_partition_healed()

    def is_in_partition(self) -> bool:
        return self._cross_dc_correlation.is_in_partition()

    def get_time_since_partition_healed(self) -> float | None:
        return self._cross_dc_correlation.get_time_since_partition_healed()

