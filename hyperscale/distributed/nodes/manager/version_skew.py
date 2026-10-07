"""
Manager version skew handling (AD-25).

Provides protocol versioning and capability negotiation for rolling upgrades
and backwards-compatible communication with workers, gates, and peer managers.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.protocol.version import (
    ProtocolVersion,
    NodeCapabilities,
    NegotiatedCapabilities,
    negotiate_capabilities,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

from .models.manager_version_metrics import ManagerVersionMetrics

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class ManagerVersionSkewHandler:
    """
    Handles protocol version skew for the manager server (AD-25).

    Provides:
    - Capability negotiation with workers, gates, and peer managers
    - Feature availability checking based on negotiated capabilities
    - Version compatibility validation
    - Graceful degradation for older protocol versions

    Compatibility Rules (per AD-25):
    - Same MAJOR version: compatible
    - Different MAJOR version: reject connection
    - Newer MINOR → older: use older's feature set
    - Older MINOR → newer: newer ignores unknown capabilities
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner

        self._local_capabilities: NodeCapabilities = NodeCapabilities.current(
            node_version=f"hyperscale-manager-{config.version}"
            if hasattr(config, "version")
            else "hyperscale-manager"
        )

        # Gates' negotiated capabilities live in ManagerState (one store,
        # cleared with the gate).

    @property
    def protocol_version(self) -> ProtocolVersion:
        """Get our protocol version."""
        return self._local_capabilities.protocol_version

    @property
    def capabilities(self) -> set[str]:
        """Get our advertised capabilities."""
        return self._local_capabilities.capabilities

    def get_local_capabilities(self) -> NodeCapabilities:
        """Get our full capabilities for handshake."""
        return self._local_capabilities

    async def negotiate_with_gate(
        self,
        gate_id: str,
        remote_capabilities: NodeCapabilities,
    ) -> NegotiatedCapabilities:
        """
        Negotiate capabilities with a gate.

        Args:
            gate_id: Gate node ID
            remote_capabilities: Gate's advertised capabilities

        Returns:
            NegotiatedCapabilities with the negotiation result

        Raises:
            ValueError: If protocol versions are incompatible
        """
        result = negotiate_capabilities(
            self._local_capabilities,
            remote_capabilities,
        )

        if not result.compatible:
            await self._logger.log(
                ServerWarning(
                    message=f"Incompatible protocol version from gate {gate_id[:8]}...: "
                    f"{remote_capabilities.protocol_version} (ours: {self.protocol_version})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            raise ValueError(
                f"Incompatible protocol versions: "
                f"{self.protocol_version} vs {remote_capabilities.protocol_version}"
            )

        self._state.set_gate_negotiated_caps(gate_id, result)

        await self._logger.log(
            ServerInfo(
                message=f"Negotiated {len(result.common_features)} features with gate {gate_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        return result

    def gate_supports_feature(self, gate_id: str, feature: str) -> bool:
        """
        Check if a gate supports a specific feature.

        Args:
            gate_id: Gate node ID
            feature: Feature name to check

        Returns:
            True if the feature is available with this gate
        """
        caps = self._state.get_gate_negotiated_caps(gate_id)
        if caps is None:
            return False
        return caps.supports(feature)

    def get_gate_capabilities(self, gate_id: str) -> NegotiatedCapabilities | None:
        """Get negotiated capabilities for a gate."""
        return self._state.get_gate_negotiated_caps(gate_id)

    def remove_gate(self, gate_id: str) -> None:
        """Remove negotiated capabilities when gate disconnects."""
        self._state._gate_negotiated_caps.pop(gate_id, None)

    def negotiate_with_client(
        self,
        client_version: ProtocolVersion,
        client_capabilities: str,
    ) -> str | None:
        """A submitting client's negotiated features, comma-joined -- or None
        when its major version is incompatible (AD-25: reject)."""
        if client_version.major != self.protocol_version.major:
            return None
        client_features = set(client_capabilities.split(",")) if client_capabilities else set()
        return ",".join(sorted(client_features & get_features_for_version(self.protocol_version)))

    def is_version_compatible(self, remote_version: ProtocolVersion) -> bool:
        """
        Check if a remote version is compatible with ours.

        Args:
            remote_version: Remote protocol version

        Returns:
            True if versions are compatible (same major version)
        """
        return self.protocol_version.is_compatible_with(remote_version)

    def get_common_features_with_all_gates(self) -> set[str]:
        """
        Get features supported by ALL connected gates.

        Returns:
            Set of features supported by all gates
        """
        if not self._state._gate_negotiated_caps:
            return set()

        common = set(self.capabilities)
        for caps in self._state._gate_negotiated_caps.values():
            common &= caps.common_features

        return common

    def get_version_metrics(self) -> ManagerVersionMetrics:
        """Get version skew metrics."""
        gate_versions: dict[str, int] = {}
        for caps in self._state._gate_negotiated_caps.values():
            version_str = str(caps.remote_version)
            gate_versions[version_str] = gate_versions.get(version_str, 0) + 1

        return {
            "local_version": str(self.protocol_version),
            "local_feature_count": len(self.capabilities),
            "gate_count": len(self._state._gate_negotiated_caps),
            "gate_versions": gate_versions,
        }
