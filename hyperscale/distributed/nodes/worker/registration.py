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
    ServerWarning,
)
from collections.abc import Awaitable, Callable

# Per-attempt bound on one worker_register round trip. Named so callers
# that must outwait a whole registration (e.g. `hyperscale join`) derive
# their budget from it instead of repeating the number.
REGISTRATION_ATTEMPT_TIMEOUT_SECONDS = 5.0

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
        send_func: Callable[[tuple[str, int], bytes, float], Awaitable[bytes | Exception]],
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
            await self._log_circuit_open(manager_addr, node_info)
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
            result = await send_func(
                manager_addr,
                registration.dump(),
                REGISTRATION_ATTEMPT_TIMEOUT_SECONDS,
            )
            if isinstance(result, Exception):
                raise result
            return True

        try:
            await executor.execute(attempt_registration, "worker_registration")
            circuit.record_success()
            return True

        except Exception as error:
            circuit.record_error()
            await self._log_registration_failure(manager_addr, max_retries, error, node_info)
            return False

    async def _log_circuit_open(self, manager_addr: tuple[str, int], node_info: NodeInfo) -> None:
        """Log a registration refused because the manager's circuit is OPEN."""
        if self._logger:
            await self._logger.log(
                ServerError(
                    message=f"Cannot register with {manager_addr}: circuit breaker is OPEN",
                    node_host=node_info.host,
                    node_port=node_info.port,
                    node_id=self._short_node_id(node_info),
                )
            )

    async def _log_registration_failure(
        self,
        manager_addr: tuple[str, int],
        max_retries: int,
        error: Exception,
        node_info: NodeInfo,
    ) -> None:
        """Log a registration that failed after every retry."""
        if self._logger:
            await self._logger.log(
                ServerError(
                    message=f"Failed to register with manager {manager_addr} after {max_retries + 1} attempts: {error}",
                    node_host=node_info.host,
                    node_port=node_info.port,
                    node_id=self._short_node_id(node_info),
                )
            )

    @staticmethod
    def _short_node_id(node_info: NodeInfo) -> str:
        """The node id's first eight characters, or "unknown" without one."""
        return node_info.node_id[:8] if node_info.node_id else "unknown"

    async def process_registration_response(
        self,
        data: bytes,
        node_host: str,
        node_port: int,
        node_id_short: str,
        add_unconfirmed_peer: Callable[[tuple[str, int]], Awaitable[None]],
        add_to_probe_scheduler: Callable[[tuple[str, int]], None],
        mark_registered: Callable[[tuple[str, int]], None] | None = None,
    ) -> tuple[bool, str | None]:
        """
        Process registration response from manager.

        Updates known managers and negotiated capabilities.

        Args:
            data: Serialized RegistrationResponse
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
            add_unconfirmed_peer: Async coroutine to add an unconfirmed
                SWIM peer (HealthAwareServer.add_unconfirmed_peer). It
                must be awaited; the previous sync invocation silently
                discarded the coroutine and the manager was never
                added to the worker's incarnation tracker.
            add_to_probe_scheduler: Sync function to add peer to probe scheduler

        Returns:
            Tuple of (accepted, primary_manager_id)
        """
        try:
            return await self._apply_registration_response(
                RegistrationResponse.load(data),
                node_host,
                node_port,
                node_id_short,
                add_unconfirmed_peer,
                add_to_probe_scheduler,
                mark_registered,
            )

        except Exception as error:
            await self._log_registration_response_failure(error, node_host, node_port, node_id_short)
            return (False, None)

    async def _apply_registration_response(
        self,
        response: RegistrationResponse,
        node_host: str,
        node_port: int,
        node_id_short: str,
        add_unconfirmed_peer: Callable[[tuple[str, int]], Awaitable[None]],
        add_to_probe_scheduler: Callable[[tuple[str, int]], None],
        mark_registered: Callable[[tuple[str, int]], None] | None,
    ) -> tuple[bool, str | None]:
        """Adopt an accepted registration response's managers, primary and capabilities."""
        if not response.accepted:
            await self._log_registration_rejected(response, node_host, node_port, node_id_short)
            return (False, None)

        # The responder is direct evidence for its own address; the
        # rest of its manager list is hearsay.
        self._confirm_responder(response)

        # Update known managers
        await self._update_known_managers(
            response.healthy_managers,
            add_unconfirmed_peer,
            add_to_probe_scheduler,
            mark_registered=mark_registered,
        )
        await self._mark_managers_healthy(response.healthy_managers)

        # Find primary manager (prefer leader)
        primary_manager_id = self._primary_from_response(response)

        self._registry.set_primary_manager(primary_manager_id)

        # Store negotiated capabilities (AD-25)
        self._store_negotiated_capabilities(response)

        return (True, primary_manager_id)

    async def _log_registration_rejected(
        self,
        response: RegistrationResponse,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log a manager's rejection of this worker's registration."""
        if self._logger:
            await self._logger.log(
                ServerWarning(
                    message=(
                        "Manager rejected worker registration: "
                        f"{response.error or 'no error provided'}"
                    ),
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                )
            )

    async def _log_registration_response_failure(
        self,
        error: Exception,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log a registration response that could not be processed."""
        if self._logger:
            await self._logger.log(
                ServerError(
                    message=(
                        "Failed to process manager registration response: "
                        f"{type(error).__name__}: {error}"
                    ),
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                )
            )

    def _confirm_responder(self, response: RegistrationResponse) -> None:
        """Confirm the responding manager from its own healthy-manager entry."""
        for manager in response.healthy_managers:
            if manager.node_id == response.manager_id:
                self._registry.confirm_manager(manager.node_id, manager)

    async def _mark_managers_healthy(self, managers: list[ManagerInfo]) -> None:
        """Mark every listed manager healthy in the registry."""
        for manager in managers:
            await self._registry.mark_manager_healthy(manager.node_id)

    @staticmethod
    def _primary_from_response(response: RegistrationResponse) -> str:
        """The listed leader, else the responding manager."""
        primary_manager_id = response.manager_id
        for manager in response.healthy_managers:
            if manager.is_leader:
                primary_manager_id = manager.node_id
                break
        return primary_manager_id

    def _store_negotiated_capabilities(self, response: RegistrationResponse) -> None:
        """Record the protocol version and features negotiated with the manager (AD-25)."""
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

    async def process_manager_registration(
        self,
        data: bytes,
        node_id_full: str,
        total_cores: int,
        available_cores: int,
        add_unconfirmed_peer: Callable[[tuple[str, int]], Awaitable[None]],
        add_to_probe_scheduler: Callable[[tuple[str, int]], None],
        mark_registered: Callable[[tuple[str, int]], None] | None = None,
    ) -> bytes:
        """
        Process registration request from a manager.

        Enables bidirectional registration for faster cluster formation.

        Args:
            data: Serialized ManagerToWorkerRegistration
            node_id_full: This worker's full node ID
            total_cores: Total CPU cores
            available_cores: Available CPU cores
            add_unconfirmed_peer: Async coroutine for adding an unconfirmed
                SWIM peer; must be awaited. Previously called sync, which
                left the manager invisible to this worker's incarnation
                tracker (manager was added via TCP registry but never
                appeared in node_states for the SWIM layer).
            add_to_probe_scheduler: Sync function to add peer to probe scheduler

        Returns:
            Serialized ManagerToWorkerRegistrationAck
        """
        try:
            registration = ManagerToWorkerRegistration.load(data)

            await self._apply_manager_registration(
                registration,
                add_unconfirmed_peer,
                add_to_probe_scheduler,
                mark_registered,
            )

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

    async def _apply_manager_registration(
        self,
        registration: ManagerToWorkerRegistration,
        add_unconfirmed_peer: Callable[[tuple[str, int]], Awaitable[None]],
        add_to_probe_scheduler: Callable[[tuple[str, int]], None],
        mark_registered: Callable[[tuple[str, int]], None] | None,
    ) -> None:
        """Adopt a registering manager, its known managers and its SWIM address."""
        # The registering manager is direct evidence for its address.
        self._registry.confirm_manager(
            registration.manager.node_id,
            registration.manager,
        )

        # Add to discovery service (AD-28)
        self._add_manager_to_discovery(registration.manager)

        # Update known managers from registration
        if registration.known_managers:
            await self._update_known_managers(
                registration.known_managers,
                add_unconfirmed_peer,
                add_to_probe_scheduler,
                mark_registered=mark_registered,
            )

        # Update primary if this is the leader
        if registration.is_leader:
            self._registry.set_primary_manager(registration.manager.node_id)

        # Add manager's UDP address to SWIM (AD-29). Explicit
        # registration handshake (manager → worker direction): the
        # manager has just registered with us and we know about it as
        # an authoritative cluster member. Mark it registered so
        # SUSPECT can fire if SWIM later detects it dead.
        await self._track_manager_udp(
            registration.manager,
            add_unconfirmed_peer,
            add_to_probe_scheduler,
            mark_registered,
        )

    async def _update_known_managers(
        self,
        managers: list[ManagerInfo],
        add_unconfirmed_peer: Callable[[tuple[str, int]], Awaitable[None]],
        add_to_probe_scheduler: Callable[[tuple[str, int]], None],
        mark_registered: Callable[[tuple[str, int]], None] | None = None,
    ) -> None:
        """
        Update known managers from a list.

        ``add_unconfirmed_peer`` is the async
        ``HealthAwareServer.add_unconfirmed_peer`` coroutine — it must be
        awaited, otherwise the manager is silently never recorded in
        the worker's incarnation tracker (AD-29 UNCONFIRMED state) and
        cannot be confirmed via SWIM probes.

        Args:
            managers: List of ManagerInfo to add
            add_unconfirmed_peer: Async coroutine to add unconfirmed SWIM peer
            add_to_probe_scheduler: Sync function to add peer to probe scheduler
            mark_registered: Sync function to mark a peer as registered.
                Called only when this method is reached via an explicit
                registration handshake completion (caller passes the
                callback in those cases; passive observers omit it).
        """
        for manager in managers:
            self._registry.add_manager(manager.node_id, manager)

            # Track as unconfirmed peer (AD-29)
            await self._track_manager_udp(
                manager,
                add_unconfirmed_peer,
                add_to_probe_scheduler,
                mark_registered,
            )

            # Add to discovery service (AD-28)
            self._add_manager_to_discovery(manager)

    async def _track_manager_udp(
        self,
        manager: ManagerInfo,
        add_unconfirmed_peer: Callable[[tuple[str, int]], Awaitable[None]],
        add_to_probe_scheduler: Callable[[tuple[str, int]], None],
        mark_registered: Callable[[tuple[str, int]], None] | None,
    ) -> None:
        """Add a manager with a UDP address as an unconfirmed, probed SWIM peer (AD-29)."""
        if manager.udp_host and manager.udp_port:
            manager_udp_addr = (manager.udp_host, manager.udp_port)
            await add_unconfirmed_peer(manager_udp_addr)
            add_to_probe_scheduler(manager_udp_addr)
            self._mark_manager_registered(manager_udp_addr, mark_registered)

    @staticmethod
    def _mark_manager_registered(
        manager_udp_addr: tuple[str, int],
        mark_registered: Callable[[tuple[str, int]], None] | None,
    ) -> None:
        """Mark the peer registered when reached through a registration handshake."""
        if mark_registered is not None:
            mark_registered(manager_udp_addr)

    def _add_manager_to_discovery(self, manager: ManagerInfo) -> None:
        """Add a manager to the discovery service (AD-28)."""
        self._discovery_service.add_peer(
            peer_id=manager.node_id,
            host=manager.tcp_host,
            port=manager.tcp_port,
            role="manager",
            datacenter_id=manager.datacenter or "",
        )
