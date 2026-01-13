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
import time
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    DatacenterHealth,
    DatacenterStatus,
    ManagerHeartbeat,
)
from hyperscale.distributed.health import ManagerHealthState
from hyperscale.distributed.datacenters import DatacenterHealthManager
from hyperscale.distributed.swim.health import (
    FederatedHealthMonitor,
    DCReachability,
)
from hyperscale.distributed.reliability import (
    BackpressureLevel,
    BackpressureSignal,
)
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerInfo

from .state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.correlation import CrossDCCorrelationDetector
    from hyperscale.distributed.versioning import VersionedClock
    from hyperscale.distributed.dispatch import ManagerDispatcher
    from taskex import TaskRunner


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
        cross_dc_correlation: "CrossDCCorrelationDetector",
        dc_manager_discovery: dict[str, DiscoveryService],
        versioned_clock: "VersionedClock",
        manager_dispatcher: "ManagerDispatcher",
        manager_health_config: dict,
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        confirm_manager_for_dc: Callable[[str, tuple[str, int]], "asyncio.Task"],
        on_partition_healed: Callable[[list[str]], None] | None = None,
        on_partition_detected: Callable[[list[str]], None] | None = None,
    ) -> None:
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._dc_health_manager = dc_health_manager
        self._dc_health_monitor = dc_health_monitor
        self._cross_dc_correlation = cross_dc_correlation
        self._dc_manager_discovery = dc_manager_discovery
        self._versioned_clock = versioned_clock
        self._manager_dispatcher = manager_dispatcher
        self._manager_health_config = manager_health_config
        self._get_node_id = get_node_id
        self._get_host = get_host
        self._get_tcp_port = get_tcp_port
        self._confirm_manager_for_dc = confirm_manager_for_dc
        self._on_partition_healed = on_partition_healed
        self._on_partition_detected = on_partition_detected

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
        dc_key = f"dc:{heartbeat.datacenter}"
        if await self._versioned_clock.is_entity_stale(dc_key, heartbeat.version):
            return

        datacenter_id = heartbeat.datacenter
        manager_addr = (
            (heartbeat.tcp_host, heartbeat.tcp_port)
            if heartbeat.tcp_host
            else source_addr
        )

        if datacenter_id not in self._state._datacenter_manager_status:
            self._state._datacenter_manager_status[datacenter_id] = {}
        self._state._datacenter_manager_status[datacenter_id][manager_addr] = heartbeat
        self._state._manager_last_status[manager_addr] = time.monotonic()

        if datacenter_id in self._dc_manager_discovery:
            discovery = self._dc_manager_discovery[datacenter_id]
            peer_id = (
                heartbeat.node_id
                if heartbeat.node_id
                else f"{manager_addr[0]}:{manager_addr[1]}"
            )
            discovery.add_peer(
                peer_id=peer_id,
                host=manager_addr[0],
                port=manager_addr[1],
                role="manager",
                datacenter_id=datacenter_id,
            )

        manager_key = (datacenter_id, manager_addr)
        health_state = self._state._manager_health.get(manager_key)
        if not health_state:
            health_state = ManagerHealthState(
                manager_id=heartbeat.node_id,
                datacenter_id=datacenter_id,
                config=self._manager_health_config,
            )
            self._state._manager_health[manager_key] = health_state

        health_state.update_liveness(success=True)
        health_state.update_readiness(
            has_quorum=heartbeat.has_quorum,
            accepting=heartbeat.accepting_jobs,
            worker_count=heartbeat.healthy_worker_count,
        )

        self._task_runner.run(self._confirm_manager_for_dc, datacenter_id, manager_addr)

        self._dc_health_manager.update_manager(datacenter_id, manager_addr, heartbeat)

        if heartbeat.is_leader:
            self._manager_dispatcher.set_leader(datacenter_id, manager_addr)

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
            )

        self._task_runner.run(
            self._versioned_clock.update_entity, dc_key, heartbeat.version
        )

    def handle_manager_backpressure_signal(
        self,
        manager_addr: tuple[str, int],
        datacenter_id: str,
        signal: BackpressureSignal,
    ) -> None:
        """
        Handle backpressure signal from a manager (AD-37).

        Updates per-manager and per-DC backpressure tracking.

        Args:
            manager_addr: Manager TCP address
            datacenter_id: Datacenter ID
            signal: Backpressure signal from manager
        """
        self._state._manager_backpressure[manager_addr] = signal.level

        if signal.suggested_delay_ms > self._state._backpressure_delay_ms:
            self._state._backpressure_delay_ms = signal.suggested_delay_ms

        self._update_dc_backpressure(datacenter_id)

    def _update_dc_backpressure(self, datacenter_id: str) -> None:
        """
        Update the aggregated backpressure level for a datacenter.

        Takes the maximum backpressure level across all managers in the DC.

        Args:
            datacenter_id: Datacenter to update
        """
        dc_managers = self._state._datacenter_manager_status.get(datacenter_id, {})
        if not dc_managers:
            self._state._dc_backpressure[datacenter_id] = BackpressureLevel.NONE
            return

        max_level = BackpressureLevel.NONE
        for manager_addr in dc_managers.keys():
            level = self._state._manager_backpressure.get(
                manager_addr, BackpressureLevel.NONE
            )
            if level.value > max_level.value:
                max_level = level

        self._state._dc_backpressure[datacenter_id] = max_level

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

        if federated_health.reachability == DCReachability.UNREACHABLE:
            return DatacenterStatus(
                dc_id=datacenter_id,
                health=DatacenterHealth.UNHEALTHY.value,
                available_capacity=0,
                queue_depth=tcp_status.queue_depth,
                manager_count=tcp_status.manager_count,
                worker_count=0,
                last_update=tcp_status.last_update,
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
        now = time.monotonic()
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
        """
        Count datacenters with at least one fresh manager heartbeat.

        A datacenter is active if any manager has sent a heartbeat in the last 60s.

        Returns:
            Number of active datacenters
        """
        now = time.monotonic()
        active_count = 0
        for datacenter_id in self._state._datacenter_manager_status:
            for manager_addr in self._state._datacenter_manager_status[datacenter_id]:
                if now - self._state._manager_last_status.get(manager_addr, 0) < 60.0:
                    active_count += 1
                    break
        return active_count

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
            except Exception:
                pass

    def _handle_partition_detected(
        self,
        affected_datacenters: list[str],
        timestamp: float,
    ) -> None:
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
            except Exception:
                pass

    def check_and_notify_partition_healed(self) -> bool:
        return self._cross_dc_correlation.check_partition_healed()

    def is_in_partition(self) -> bool:
        return self._cross_dc_correlation.is_in_partition()

    def get_time_since_partition_healed(self) -> float | None:
        return self._cross_dc_correlation.get_time_since_partition_healed()
