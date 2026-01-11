"""
TCP handler for worker registration.

Handles worker registration requests and validates cluster/environment isolation.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkerRegistration,
    RegistrationResponse,
)
from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION
from hyperscale.distributed.discovery.security.role_validator import (
    RoleValidator,
)
from hyperscale.distributed.server.protocol.utils import get_peer_certificate_der
from hyperscale.logging.hyperscale_logging_models import ServerWarning, ServerInfo

if TYPE_CHECKING:
    import asyncio
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


class WorkerRegistrationHandler:
    """
    Handle worker registration requests.

    Validates cluster/environment isolation (AD-28) and mTLS claims
    before accepting worker registration.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        role_validator: RoleValidator,
        node_id: str,
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._role_validator = role_validator
        self._node_id = node_id
        self._task_runner = task_runner

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
        transport: "asyncio.Transport",
    ) -> bytes:
        """
        Process worker registration request.

        Args:
            addr: Source address
            data: Serialized WorkerRegistration message
            clock_time: Logical clock time
            transport: Transport for mTLS certificate extraction

        Returns:
            Serialized RegistrationResponse
        """
        try:
            registration = WorkerRegistration.load(data)

            # Cluster isolation validation (AD-28 Issue 2)
            if registration.cluster_id != self._config.cluster_id:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Worker {registration.node.node_id} rejected: cluster_id mismatch",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )
                return RegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id,
                    healthy_managers=[],
                    error=f"Cluster isolation violation: cluster_id mismatch",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            if registration.environment_id != self._config.environment_id:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Worker {registration.node.node_id} rejected: environment_id mismatch",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )
                return RegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id,
                    healthy_managers=[],
                    error=f"Environment isolation violation: environment_id mismatch",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Role-based mTLS validation (AD-28 Issue 1)
            cert_der = get_peer_certificate_der(transport)
            if cert_der is not None:
                claims = RoleValidator.extract_claims_from_cert(
                    cert_der,
                    default_cluster=self._config.cluster_id,
                    default_environment=self._config.environment_id,
                )

                validation_result = self._role_validator.validate_claims(claims)
                if not validation_result.allowed:
                    self._task_runner.run(
                        self._logger.log,
                        ServerWarning(
                            message=f"Worker {registration.node.node_id} rejected: certificate claims failed",
                            node_host=self._config.host,
                            node_port=self._config.tcp_port,
                            node_id=self._node_id,
                        )
                    )
                    return RegistrationResponse(
                        accepted=False,
                        manager_id=self._node_id,
                        healthy_managers=[],
                        error=f"Certificate validation failed: {validation_result.reason}",
                        protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                        protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                    ).dump()

            elif self._config.mtls_strict_mode:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Worker {registration.node.node_id} rejected: no certificate in strict mode",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )
                return RegistrationResponse(
                    accepted=False,
                    manager_id=self._node_id,
                    healthy_managers=[],
                    error="mTLS strict mode requires valid certificate",
                    protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                    protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
                ).dump()

            # Registration accepted - store worker
            worker_id = registration.node.node_id
            self._state._workers[worker_id] = registration
            tcp_addr = (registration.node.host, registration.node.tcp_port)
            udp_addr = (registration.node.host, registration.node.udp_port)
            self._state._worker_addr_to_id[tcp_addr] = worker_id
            self._state._worker_addr_to_id[udp_addr] = worker_id

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Worker {worker_id[:8]}... registered with {registration.node.total_cores} cores",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )

            return RegistrationResponse(
                accepted=True,
                manager_id=self._node_id,
                healthy_managers=[],
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()

        except Exception as e:
            return RegistrationResponse(
                accepted=False,
                manager_id=self._node_id,
                healthy_managers=[],
                error=str(e),
                protocol_version_major=CURRENT_PROTOCOL_VERSION.major,
                protocol_version_minor=CURRENT_PROTOCOL_VERSION.minor,
            ).dump()
