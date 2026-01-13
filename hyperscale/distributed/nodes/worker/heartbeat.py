"""
Worker heartbeat handling module.

Handles manager heartbeats from SWIM and peer confirmation logic.
Extracted from worker_impl.py for modularity.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import ManagerHeartbeat, ManagerInfo
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerInfo

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from .registry import WorkerRegistry


class WorkerHeartbeatHandler:
    """
    Handles manager heartbeat processing for worker.

    Processes heartbeats from SWIM message embedding, updates manager
    tracking, and handles job leadership claims.
    """

    def __init__(
        self,
        registry: "WorkerRegistry",
        logger: "Logger | None" = None,
    ) -> None:
        """
        Initialize heartbeat handler.

        Args:
            registry: WorkerRegistry for manager tracking
            logger: Logger instance
        """
        self._registry = registry
        self._logger = logger

        # Callbacks for registration and job leadership updates
        self._on_new_manager_discovered: callable | None = None
        self._on_job_leadership_update: callable | None = None

    def set_callbacks(
        self,
        on_new_manager_discovered: callable | None = None,
        on_job_leadership_update: callable | None = None,
    ) -> None:
        """
        Set callbacks for heartbeat events.

        Args:
            on_new_manager_discovered: Called when new manager found via heartbeat
            on_job_leadership_update: Called when job leadership changes detected
        """
        self._on_new_manager_discovered = on_new_manager_discovered
        self._on_job_leadership_update = on_job_leadership_update

    def process_manager_heartbeat(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
        confirm_peer: callable,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> None:
        """
        Process manager heartbeat from SWIM.

        Updates manager tracking, handles leadership changes, and
        processes job leadership claims.

        Args:
            heartbeat: ManagerHeartbeat from SWIM
            source_addr: Source UDP address
            confirm_peer: Function to confirm peer in SWIM
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
        """
        # Confirm peer in SWIM layer (AD-29)
        confirm_peer(source_addr)

        manager_id = heartbeat.node_id
        existing_manager = self._registry.get_manager(manager_id)

        if existing_manager:
            self._update_existing_manager(
                heartbeat,
                manager_id,
                existing_manager,
                node_host,
                node_port,
                node_id_short,
                task_runner_run,
            )
        else:
            self._register_new_manager(
                heartbeat,
                manager_id,
                source_addr,
                node_host,
                node_port,
                node_id_short,
                task_runner_run,
            )

        # Process job leadership claims
        if heartbeat.job_leaderships:
            self._process_job_leadership_claims(
                heartbeat,
                source_addr,
                node_host,
                node_port,
                node_id_short,
                task_runner_run,
            )

    def _update_existing_manager(
        self,
        heartbeat: ManagerHeartbeat,
        manager_id: str,
        existing_manager: ManagerInfo,
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> None:
        """Update existing manager info from heartbeat if leadership changed."""
        if heartbeat.is_leader == existing_manager.is_leader:
            return

        # Update manager info with new leadership status
        updated_manager = ManagerInfo(
            node_id=existing_manager.node_id,
            tcp_host=existing_manager.tcp_host,
            tcp_port=existing_manager.tcp_port,
            udp_host=existing_manager.udp_host,
            udp_port=existing_manager.udp_port,
            datacenter=heartbeat.datacenter,
            is_leader=heartbeat.is_leader,
        )
        self._registry.add_manager(manager_id, updated_manager)

        # If this manager became the leader, switch primary
        if heartbeat.is_leader and self._registry._primary_manager_id != manager_id:
            old_primary = self._registry._primary_manager_id
            self._registry.set_primary_manager(manager_id)

            if self._logger:
                task_runner_run(
                    self._logger.log,
                    ServerInfo(
                        message=f"Leadership change via SWIM: {old_primary} -> {manager_id}",
                        node_host=node_host,
                        node_port=node_port,
                        node_id=node_id_short,
                    ),
                )

    def _register_new_manager(
        self,
        heartbeat: ManagerHeartbeat,
        manager_id: str,
        source_addr: tuple[str, int],
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> None:
        """Register a new manager discovered via SWIM heartbeat."""
        tcp_host = heartbeat.tcp_host or source_addr[0]
        tcp_port = heartbeat.tcp_port or (source_addr[1] - 1)

        new_manager = ManagerInfo(
            node_id=manager_id,
            tcp_host=tcp_host,
            tcp_port=tcp_port,
            udp_host=source_addr[0],
            udp_port=source_addr[1],
            datacenter=heartbeat.datacenter,
            is_leader=heartbeat.is_leader,
        )
        self._registry.add_manager(manager_id, new_manager)

        if self._logger:
            task_runner_run(
                self._logger.log,
                ServerInfo(
                    message=f"Discovered new manager via SWIM: {manager_id} (leader={heartbeat.is_leader})",
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                ),
            )

        # Trigger callback for new manager registration
        if self._on_new_manager_discovered:
            task_runner_run(
                self._on_new_manager_discovered,
                (new_manager.tcp_host, new_manager.tcp_port),
            )

        # If this is a leader and we don't have a primary, use it
        if heartbeat.is_leader and not self._registry._primary_manager_id:
            self._registry.set_primary_manager(manager_id)

    def _process_job_leadership_claims(
        self,
        heartbeat: ManagerHeartbeat,
        source_addr: tuple[str, int],
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> None:
        """
        Process job leadership claims from heartbeat.

        Updates workflow job leader routing for workflows belonging
        to jobs this manager claims leadership of.

        Args:
            heartbeat: ManagerHeartbeat with job_leaderships
            source_addr: Source UDP address
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
        """
        if not self._on_job_leadership_update:
            return

        # Get TCP address for routing
        tcp_host = heartbeat.tcp_host or source_addr[0]
        tcp_port = heartbeat.tcp_port or (source_addr[1] - 1)
        manager_tcp_addr = (tcp_host, tcp_port)

        # Notify callback with job leaderships and manager address
        self._on_job_leadership_update(
            heartbeat.job_leaderships,
            manager_tcp_addr,
            node_host,
            node_port,
            node_id_short,
            task_runner_run,
        )

    def on_peer_confirmed(
        self,
        peer: tuple[str, int],
        node_host: str,
        node_port: int,
        node_id_short: str,
        task_runner_run: callable,
    ) -> None:
        """
        Handle peer confirmation from SWIM (AD-29).

        Called when a peer is confirmed via successful SWIM communication.
        This is the only place where managers should be added to healthy set.

        Args:
            peer: UDP address of confirmed peer
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            task_runner_run: Function to run async tasks
        """
        manager_id = self._registry.find_manager_by_udp_addr(peer)
        if not manager_id:
            return

        task_runner_run(self._registry.mark_manager_healthy, manager_id)

        if self._logger:
            task_runner_run(
                self._logger.log,
                ServerDebug(
                    message=f"AD-29: Manager {manager_id[:8]}... confirmed via SWIM, added to healthy set",
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                ),
            )
