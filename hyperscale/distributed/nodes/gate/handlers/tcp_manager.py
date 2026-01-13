"""
TCP handlers for manager registration and status operations.

Handles manager-facing operations:
- Manager registration
- Manager status updates
- Manager discovery broadcasts
"""

import asyncio
import time
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    GateInfo,
    ManagerDiscoveryBroadcast,
    ManagerHeartbeat,
    ManagerRegistrationResponse,
)
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    ProtocolVersion,
    negotiate_capabilities,
)
from hyperscale.distributed.reliability import BackpressureLevel, BackpressureSignal
from hyperscale.distributed.discovery.security import RoleValidator
from hyperscale.distributed.discovery.security.role_validator import (
    NodeRole as SecurityNodeRole,
)
from hyperscale.distributed.server.protocol.utils import get_peer_certificate_der
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerWarning,
)

from ..state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.env import Env
    from taskex import TaskRunner


class GateManagerHandler:
    """
    Handles manager registration and status operations.

    Provides TCP handler methods for manager-facing operations.
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        env: "Env",
        datacenter_managers: dict[str, list[tuple[str, int]]],
        role_validator: RoleValidator,
        node_capabilities: NodeCapabilities,
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        get_healthy_gates: Callable[[], list[GateInfo]],
        record_manager_heartbeat: Callable[[str, tuple[str, int], str, int], None],
        handle_manager_backpressure_signal: Callable,
        update_dc_backpressure: Callable[[str], None],
        broadcast_manager_discovery: Callable,
    ) -> None:
        """
        Initialize the manager handler.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            env: Environment configuration
            datacenter_managers: DC -> manager addresses mapping
            role_validator: Role-based access validator
            node_capabilities: This gate's capabilities
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            get_healthy_gates: Callback to get healthy gate list
            record_manager_heartbeat: Callback to record manager heartbeat
            handle_manager_backpressure_signal: Callback for backpressure handling
            update_dc_backpressure: Callback to update DC backpressure
            broadcast_manager_discovery: Callback to broadcast discovery
        """
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._env = env
        self._datacenter_managers = datacenter_managers
        self._role_validator = role_validator
        self._node_capabilities = node_capabilities
        self._get_node_id = get_node_id
        self._get_host = get_host
        self._get_tcp_port = get_tcp_port
        self._get_healthy_gates = get_healthy_gates
        self._record_manager_heartbeat = record_manager_heartbeat
        self._handle_manager_backpressure_signal = handle_manager_backpressure_signal
        self._update_dc_backpressure = update_dc_backpressure
        self._broadcast_manager_discovery = broadcast_manager_discovery

    async def handle_status_update(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle manager status update via TCP.

        This is NOT a healthcheck - DC liveness is tracked via per-manager heartbeat freshness.
        This contains job progress and worker capacity information.

        Args:
            addr: Manager address
            data: Serialized ManagerHeartbeat
            handle_exception: Callback for exception handling

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            status = ManagerHeartbeat.load(data)

            datacenter_id = status.datacenter
            manager_addr = (status.tcp_host, status.tcp_port)

            await self._state.update_manager_status(
                datacenter_id, manager_addr, status, time.monotonic()
            )

            self._record_manager_heartbeat(
                datacenter_id, manager_addr, status.node_id, status.version
            )

            if status.backpressure_level > 0 or status.backpressure_delay_ms > 0:
                backpressure_signal = BackpressureSignal(
                    level=BackpressureLevel(status.backpressure_level),
                    suggested_delay_ms=status.backpressure_delay_ms,
                )
                self._handle_manager_backpressure_signal(
                    manager_addr, datacenter_id, backpressure_signal
                )
            elif manager_addr in self._state._manager_backpressure:
                self._state._manager_backpressure[manager_addr] = BackpressureLevel.NONE
                self._update_dc_backpressure(datacenter_id)

            return b"ok"

        except Exception as error:
            await handle_exception(error, "manager_status_update")
            return b"error"

    async def handle_register(
        self,
        addr: tuple[str, int],
        data: bytes,
        transport: asyncio.Transport,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle manager registration.

        Managers register with gates at startup to discover all healthy gates.
        Includes cluster isolation validation, protocol negotiation, and
        role-based mTLS validation (AD-25, AD-28).

        Args:
            addr: Manager address
            data: Serialized ManagerHeartbeat
            transport: TCP transport for certificate extraction
            handle_exception: Callback for exception handling

        Returns:
            Serialized ManagerRegistrationResponse
        """
        try:
            heartbeat = ManagerHeartbeat.load(data)

            datacenter_id = heartbeat.datacenter
            manager_addr = (heartbeat.tcp_host, heartbeat.tcp_port)

            # Cluster isolation validation (AD-28 Issue 2)
            if heartbeat.cluster_id != self._env.CLUSTER_ID:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Manager {heartbeat.node_id} rejected: cluster_id mismatch "
                        f"(manager={heartbeat.cluster_id}, gate={self._env.CLUSTER_ID})",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )
                return ManagerRegistrationResponse(
                    accepted=False,
                    gate_id=self._get_node_id().full,
                    healthy_gates=[],
                    error=f"Cluster isolation violation: manager cluster_id '{heartbeat.cluster_id}' "
                    f"does not match gate cluster_id '{self._env.CLUSTER_ID}'",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            if heartbeat.environment_id != self._env.ENVIRONMENT_ID:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Manager {heartbeat.node_id} rejected: environment_id mismatch "
                        f"(manager={heartbeat.environment_id}, gate={self._env.ENVIRONMENT_ID})",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )
                return ManagerRegistrationResponse(
                    accepted=False,
                    gate_id=self._get_node_id().full,
                    healthy_gates=[],
                    error=f"Environment isolation violation: manager environment_id '{heartbeat.environment_id}' "
                    f"does not match gate environment_id '{self._env.ENVIRONMENT_ID}'",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Role-based mTLS validation (AD-28 Issue 1)
            cert_der = get_peer_certificate_der(transport)
            if cert_der is not None:
                claims = RoleValidator.extract_claims_from_cert(
                    cert_der,
                    default_cluster=self._env.CLUSTER_ID,
                    default_environment=self._env.ENVIRONMENT_ID,
                )

                validation_result = self._role_validator.validate_claims(claims)
                if not validation_result.allowed:
                    self._task_runner.run(
                        self._logger.log,
                        ServerWarning(
                            message=f"Manager {heartbeat.node_id} rejected: certificate claims validation failed - {validation_result.reason}",
                            node_host=self._get_host(),
                            node_port=self._get_tcp_port(),
                            node_id=self._get_node_id().short,
                        ),
                    )
                    return ManagerRegistrationResponse(
                        accepted=False,
                        gate_id=self._get_node_id().full,
                        healthy_gates=[],
                        error=f"Certificate claims validation failed: {validation_result.reason}",
                        protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                        protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                    ).dump()

                if not self._role_validator.is_allowed(
                    claims.role, SecurityNodeRole.GATE
                ):
                    self._task_runner.run(
                        self._logger.log,
                        ServerWarning(
                            message=f"Manager {heartbeat.node_id} rejected: role-based access denied ({claims.role.value}->gate not allowed)",
                            node_host=self._get_host(),
                            node_port=self._get_tcp_port(),
                            node_id=self._get_node_id().short,
                        ),
                    )
                    return ManagerRegistrationResponse(
                        accepted=False,
                        gate_id=self._get_node_id().full,
                        healthy_gates=[],
                        error=f"Role-based access denied: {claims.role.value} cannot register with gates",
                        protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                        protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                    ).dump()
            else:
                if not self._role_validator.is_allowed(
                    SecurityNodeRole.MANAGER, SecurityNodeRole.GATE
                ):
                    self._task_runner.run(
                        self._logger.log,
                        ServerWarning(
                            message=f"Manager {heartbeat.node_id} registration rejected: role-based access denied (manager->gate not allowed)",
                            node_host=self._get_host(),
                            node_port=self._get_tcp_port(),
                            node_id=self._get_node_id().short,
                        ),
                    )
                    return ManagerRegistrationResponse(
                        accepted=False,
                        gate_id=self._get_node_id().full,
                        healthy_gates=[],
                        error="Role-based access denied: managers cannot register with gates in this configuration",
                        protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                        protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                    ).dump()

            # Protocol version negotiation (AD-25)
            manager_version = ProtocolVersion(
                major=getattr(heartbeat, "protocol_version_major", 1),
                minor=getattr(heartbeat, "protocol_version_minor", 0),
            )
            manager_caps_str = getattr(heartbeat, "capabilities", "")
            manager_capabilities = (
                set(manager_caps_str.split(",")) if manager_caps_str else set()
            )

            manager_node_caps = NodeCapabilities(
                protocol_version=manager_version,
                capabilities=manager_capabilities,
                node_version=heartbeat.node_id,
            )

            negotiated = negotiate_capabilities(
                self._node_capabilities, manager_node_caps
            )

            if not negotiated.compatible:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Manager registration rejected: incompatible protocol version "
                        f"{manager_version} (we are {CURRENT_PROTOCOL_VERSION})",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )
                return ManagerRegistrationResponse(
                    accepted=False,
                    gate_id=self._get_node_id().full,
                    healthy_gates=[],
                    error=f"Incompatible protocol version: {manager_version} vs {CURRENT_PROTOCOL_VERSION}",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            self._state._manager_negotiated_caps[manager_addr] = negotiated

            if datacenter_id not in self._state._datacenter_manager_status:
                self._state._datacenter_manager_status[datacenter_id] = {}
            self._state._datacenter_manager_status[datacenter_id][manager_addr] = (
                heartbeat
            )
            self._state._manager_last_status[manager_addr] = time.monotonic()

            if datacenter_id not in self._datacenter_managers:
                self._datacenter_managers[datacenter_id] = []
            if manager_addr not in self._datacenter_managers[datacenter_id]:
                self._datacenter_managers[datacenter_id].append(manager_addr)

            self._record_manager_heartbeat(
                datacenter_id, manager_addr, heartbeat.node_id, heartbeat.version
            )

            if heartbeat.backpressure_level > 0 or heartbeat.backpressure_delay_ms > 0:
                backpressure_signal = BackpressureSignal(
                    level=BackpressureLevel(heartbeat.backpressure_level),
                    suggested_delay_ms=heartbeat.backpressure_delay_ms,
                )
                self._handle_manager_backpressure_signal(
                    manager_addr, datacenter_id, backpressure_signal
                )

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Manager registered: {heartbeat.node_id} from DC {datacenter_id} "
                    f"({heartbeat.worker_count} workers, protocol {manager_version}, "
                    f"{len(negotiated.common_features)} features)",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )

            negotiated_caps_str = ",".join(sorted(negotiated.common_features))
            response = ManagerRegistrationResponse(
                accepted=True,
                gate_id=self._get_node_id().full,
                healthy_gates=self._get_healthy_gates(),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                capabilities=negotiated_caps_str,
            )

            self._task_runner.run(
                self._broadcast_manager_discovery,
                datacenter_id,
                manager_addr,
                None,
                heartbeat.worker_count,
                getattr(heartbeat, "healthy_worker_count", heartbeat.worker_count),
                heartbeat.available_cores,
                getattr(heartbeat, "total_cores", 0),
            )

            return response.dump()

        except Exception as error:
            await handle_exception(error, "manager_register")
            return ManagerRegistrationResponse(
                accepted=False,
                gate_id=self._get_node_id().full,
                healthy_gates=[],
                error=str(error),
            ).dump()

    async def handle_discovery(
        self,
        addr: tuple[str, int],
        data: bytes,
        datacenter_manager_udp: dict[str, list[tuple[str, int]]],
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle manager discovery broadcast from a peer gate.

        When another gate receives a manager registration, it broadcasts
        to all peers. This handler adds the manager to our tracking.

        Args:
            addr: Source gate address
            data: Serialized ManagerDiscoveryBroadcast
            datacenter_manager_udp: DC -> manager UDP addresses mapping
            handle_exception: Callback for exception handling

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            broadcast = ManagerDiscoveryBroadcast.load(data)

            datacenter_id = broadcast.datacenter
            manager_addr = tuple(broadcast.manager_tcp_addr)

            dc_managers = self._datacenter_managers.setdefault(datacenter_id, [])
            dc_manager_status = self._state._datacenter_manager_status.setdefault(
                datacenter_id, {}
            )

            if manager_addr not in dc_managers:
                dc_managers.append(manager_addr)

                if broadcast.manager_udp_addr:
                    dc_udp = datacenter_manager_udp.setdefault(datacenter_id, [])
                    udp_addr = tuple(broadcast.manager_udp_addr)
                    if udp_addr not in dc_udp:
                        dc_udp.append(udp_addr)

                self._task_runner.run(
                    self._logger.log,
                    ServerInfo(
                        message=f"Discovered manager {manager_addr} in DC {datacenter_id} via gate {broadcast.source_gate_id}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )

            synthetic_heartbeat = ManagerHeartbeat(
                node_id=f"discovered-via-{broadcast.source_gate_id}",
                datacenter=datacenter_id,
                is_leader=False,
                term=0,
                version=0,
                active_jobs=0,
                active_workflows=0,
                worker_count=broadcast.worker_count,
                healthy_worker_count=broadcast.healthy_worker_count,
                available_cores=broadcast.available_cores,
                total_cores=broadcast.total_cores,
                state="active",
            )
            dc_manager_status[manager_addr] = synthetic_heartbeat
            self._state._manager_last_status[manager_addr] = time.monotonic()

            return b"ok"

        except Exception as error:
            await handle_exception(error, "manager_discovery")
            return b"error"
