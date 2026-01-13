"""
Worker registration module.

Handles registration with managers and processing registration responses.
Extracted from worker_impl.py for modularity.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    ManagerInfo,
    ManagerToWorkerRegistration,
    ManagerToWorkerRegistrationAck,
    NodeInfo,
    RegistrationResponse,
    WorkerRegistration,
)
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NegotiatedCapabilities,
    NodeCapabilities,
    ProtocolVersion,
)
from hyperscale.distributed.reliability import (
    RetryConfig,
    RetryExecutor,
    JitterStrategy,
)
from hyperscale.distributed.swim.core import CircuitState
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerInfo,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from hyperscale.distributed.discovery import DiscoveryService
    from .registry import WorkerRegistry


class WorkerRegistrationHandler:
    """
    Handles worker registration with managers.

    Manages initial registration, bidirectional registration processing,
    and negotiated capabilities storage.
    """

    def __init__(
        self,
        registry: "WorkerRegistry",
        discovery_service: "DiscoveryService",
        logger: "Logger | None" = None,
        node_capabilities: NodeCapabilities | None = None,
    ) -> None:
        """
        Initialize registration handler.

        Args:
            registry: WorkerRegistry for manager tracking
            discovery_service: DiscoveryService for peer management (AD-28)
            logger: Logger instance
            node_capabilities: Node capabilities for protocol negotiation
        """
        self._registry: "WorkerRegistry" = registry
        self._discovery_service: "DiscoveryService" = discovery_service
        self._logger: "Logger | None" = logger
        self._node_capabilities: NodeCapabilities = (
            node_capabilities or NodeCapabilities.current(node_version="")
        )

        # Negotiated capabilities (AD-25)
        self._negotiated_capabilities: NegotiatedCapabilities | None = None

    def set_node_capabilities(self, capabilities: NodeCapabilities) -> None:
        """Update node capabilities after node ID is available."""
        self._node_capabilities = capabilities

    @property
    def negotiated_capabilities(self) -> NegotiatedCapabilities | None:
        """Get negotiated capabilities from last registration."""
        return self._negotiated_capabilities

    async def register_with_manager(
        self,
        manager_addr: tuple[str, int],
        node_info: NodeInfo,
        total_cores: int,
        available_cores: int,
        memory_mb: int,
        available_memory_mb: int,
        cluster_id: str,
        environment_id: str,
        send_func: callable,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ) -> bool:
        """
        Register this worker with a manager.

        Uses exponential backoff with jitter for retries.

        Args:
            manager_addr: Manager (host, port) tuple
            node_info: This worker's node information
            total_cores: Total CPU cores
            available_cores: Available CPU cores
            memory_mb: Total memory in MB
            available_memory_mb: Available memory in MB
            cluster_id: Cluster identifier
            environment_id: Environment identifier
            send_func: Function to send registration data
            max_retries: Maximum retry attempts
            base_delay: Base delay for exponential backoff

        Returns:
            True if registration succeeded
        """
        circuit = self._registry.get_or_create_circuit_by_addr(manager_addr)

        if circuit.circuit_state == CircuitState.OPEN:
            if self._logger:
                await self._logger.log(
                    ServerError(
                        message=f"Cannot register with {manager_addr}: circuit breaker is OPEN",
                        node_host=node_info.host,
                        node_port=node_info.port,
                        node_id=node_info.node_id[:8]
                        if node_info.node_id
                        else "unknown",
                    )
                )
            return False

        capabilities_str = ",".join(sorted(self._node_capabilities.capabilities))

        registration = WorkerRegistration(
            node=node_info,
            total_cores=total_cores,
            available_cores=available_cores,
            memory_mb=memory_mb,
            available_memory_mb=available_memory_mb,
            cluster_id=cluster_id,
            environment_id=environment_id,
            protocol_version_major=self._node_capabilities.protocol_version.major,
            protocol_version_minor=self._node_capabilities.protocol_version.minor,
            capabilities=capabilities_str,
        )

        retry_config = RetryConfig(
            max_attempts=max_retries + 1,
            base_delay=base_delay,
            max_delay=base_delay * (2**max_retries),
            jitter=JitterStrategy.FULL,
        )
        executor = RetryExecutor(retry_config)

        async def attempt_registration() -> bool:
            result = await send_func(manager_addr, registration.dump(), timeout=5.0)
            if isinstance(result, Exception):
                raise result
            return True

        try:
            await executor.execute(attempt_registration, "worker_registration")
            circuit.record_success()
            return True

        except Exception as error:
            circuit.record_error()
            if self._logger:
                await self._logger.log(
                    ServerError(
                        message=f"Failed to register with manager {manager_addr} after {max_retries + 1} attempts: {error}",
                        node_host=node_info.host,
                        node_port=node_info.port,
                        node_id=node_info.node_id[:8]
                        if node_info.node_id
                        else "unknown",
                    )
                )
            return False

    def process_registration_response(
        self,
        data: bytes,
        node_host: str,
        node_port: int,
        node_id_short: str,
        add_unconfirmed_peer: callable,
        add_to_probe_scheduler: callable,
    ) -> tuple[bool, str | None]:
        """
        Process registration response from manager.

        Updates known managers and negotiated capabilities.

        Args:
            data: Serialized RegistrationResponse
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            add_unconfirmed_peer: Function to add unconfirmed SWIM peer
            add_to_probe_scheduler: Function to add peer to probe scheduler

        Returns:
            Tuple of (accepted, primary_manager_id)
        """
        try:
            response = RegistrationResponse.load(data)

            if not response.accepted:
                return (False, None)

            # Update known managers
            self._update_known_managers(
                response.healthy_managers,
                add_unconfirmed_peer,
                add_to_probe_scheduler,
            )

            # Find primary manager (prefer leader)
            primary_manager_id = response.manager_id
            for manager in response.healthy_managers:
                if manager.is_leader:
                    primary_manager_id = manager.node_id
                    break

            self._registry.set_primary_manager(primary_manager_id)

            # Store negotiated capabilities (AD-25)
            manager_version = ProtocolVersion(
                response.protocol_version_major,
                response.protocol_version_minor,
            )

            negotiated_features = (
                set(response.capabilities.split(","))
                if response.capabilities
                else set()
            )
            negotiated_features.discard("")

            self._negotiated_capabilities = NegotiatedCapabilities(
                local_version=CURRENT_PROTOCOL_VERSION,
                remote_version=manager_version,
                common_features=negotiated_features,
                compatible=True,
            )

            return (True, primary_manager_id)

        except Exception:
            return (False, None)

    def process_manager_registration(
        self,
        data: bytes,
        node_id_full: str,
        total_cores: int,
        available_cores: int,
        add_unconfirmed_peer: callable,
        add_to_probe_scheduler: callable,
    ) -> bytes:
        """
        Process registration request from a manager.

        Enables bidirectional registration for faster cluster formation.

        Args:
            data: Serialized ManagerToWorkerRegistration
            node_id_full: This worker's full node ID
            total_cores: Total CPU cores
            available_cores: Available CPU cores
            add_unconfirmed_peer: Function to add unconfirmed SWIM peer
            add_to_probe_scheduler: Function to add peer to probe scheduler

        Returns:
            Serialized ManagerToWorkerRegistrationAck
        """
        try:
            registration = ManagerToWorkerRegistration.load(data)

            # Add this manager to known managers
            self._registry.add_manager(
                registration.manager.node_id,
                registration.manager,
            )

            # Add to discovery service (AD-28)
            self._discovery_service.add_peer(
                peer_id=registration.manager.node_id,
                host=registration.manager.tcp_host,
                port=registration.manager.tcp_port,
                role="manager",
                datacenter_id=registration.manager.datacenter or "",
            )

            # Update known managers from registration
            if registration.known_managers:
                self._update_known_managers(
                    registration.known_managers,
                    add_unconfirmed_peer,
                    add_to_probe_scheduler,
                )

            # Update primary if this is the leader
            if registration.is_leader:
                self._registry.set_primary_manager(registration.manager.node_id)

            # Add manager's UDP address to SWIM (AD-29)
            manager_udp_addr = (
                registration.manager.udp_host,
                registration.manager.udp_port,
            )
            if manager_udp_addr[0] and manager_udp_addr[1]:
                add_unconfirmed_peer(manager_udp_addr)
                add_to_probe_scheduler(manager_udp_addr)

            return ManagerToWorkerRegistrationAck(
                accepted=True,
                worker_id=node_id_full,
                total_cores=total_cores,
                available_cores=available_cores,
            ).dump()

        except Exception as error:
            return ManagerToWorkerRegistrationAck(
                accepted=False,
                worker_id=node_id_full,
                error=str(error),
            ).dump()

    def _update_known_managers(
        self,
        managers: list[ManagerInfo],
        add_unconfirmed_peer: callable,
        add_to_probe_scheduler: callable,
    ) -> None:
        """
        Update known managers from a list.

        Args:
            managers: List of ManagerInfo to add
            add_unconfirmed_peer: Function to add unconfirmed SWIM peer
            add_to_probe_scheduler: Function to add peer to probe scheduler
        """
        for manager in managers:
            self._registry.add_manager(manager.node_id, manager)

            # Track as unconfirmed peer (AD-29)
            if manager.udp_host and manager.udp_port:
                manager_udp_addr = (manager.udp_host, manager.udp_port)
                add_unconfirmed_peer(manager_udp_addr)
                add_to_probe_scheduler(manager_udp_addr)

            # Add to discovery service (AD-28)
            self._discovery_service.add_peer(
                peer_id=manager.node_id,
                host=manager.tcp_host,
                port=manager.tcp_port,
                role="manager",
                datacenter_id=manager.datacenter or "",
            )
