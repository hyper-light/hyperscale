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

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
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
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

        # Our capabilities
        self._local_capabilities = NodeCapabilities.current(
            node_version=f"hyperscale-manager-{config.version}"
            if hasattr(config, "version")
            else "hyperscale-manager"
        )

        # Negotiated capabilities per peer (node_id -> NegotiatedCapabilities)
        self._worker_capabilities: dict[str, NegotiatedCapabilities] = {}
        self._gate_capabilities: dict[str, NegotiatedCapabilities] = {}
        self._peer_manager_capabilities: dict[str, NegotiatedCapabilities] = {}

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

    def negotiate_with_worker(
        self,
        worker_id: str,
        remote_capabilities: NodeCapabilities,
    ) -> NegotiatedCapabilities:
        """
        Negotiate capabilities with a worker.

        Args:
            worker_id: Worker node ID
            remote_capabilities: Worker's advertised capabilities

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
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Incompatible protocol version from worker {worker_id[:8]}...: "
                    f"{remote_capabilities.protocol_version} (ours: {self.protocol_version})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
            raise ValueError(
                f"Incompatible protocol versions: "
                f"{self.protocol_version} vs {remote_capabilities.protocol_version}"
            )

        self._worker_capabilities[worker_id] = result

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Negotiated {len(result.common_features)} features with worker {worker_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

        return result

    def negotiate_with_gate(
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
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Incompatible protocol version from gate {gate_id[:8]}...: "
                    f"{remote_capabilities.protocol_version} (ours: {self.protocol_version})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
            raise ValueError(
                f"Incompatible protocol versions: "
                f"{self.protocol_version} vs {remote_capabilities.protocol_version}"
            )

        self._gate_capabilities[gate_id] = result
        # Also store in state for access by other components
        self._state._gate_negotiated_caps[gate_id] = result

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Negotiated {len(result.common_features)} features with gate {gate_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

        return result

    def negotiate_with_peer_manager(
        self,
        peer_id: str,
        remote_capabilities: NodeCapabilities,
    ) -> NegotiatedCapabilities:
        """
        Negotiate capabilities with a peer manager.

        Args:
            peer_id: Peer manager node ID
            remote_capabilities: Peer's advertised capabilities

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
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Incompatible protocol version from peer manager {peer_id[:8]}...: "
                    f"{remote_capabilities.protocol_version} (ours: {self.protocol_version})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
            raise ValueError(
                f"Incompatible protocol versions: "
                f"{self.protocol_version} vs {remote_capabilities.protocol_version}"
            )

        self._peer_manager_capabilities[peer_id] = result

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Negotiated {len(result.common_features)} features with peer manager {peer_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

        return result

    def worker_supports_feature(self, worker_id: str, feature: str) -> bool:
        """
        Check if a worker supports a specific feature.

        Args:
            worker_id: Worker node ID
            feature: Feature name to check

        Returns:
            True if the feature is available with this worker
        """
        caps = self._worker_capabilities.get(worker_id)
        if caps is None:
            return False
        return caps.supports(feature)

    def gate_supports_feature(self, gate_id: str, feature: str) -> bool:
        """
        Check if a gate supports a specific feature.

        Args:
            gate_id: Gate node ID
            feature: Feature name to check

        Returns:
            True if the feature is available with this gate
        """
        caps = self._gate_capabilities.get(gate_id)
        if caps is None:
            return False
        return caps.supports(feature)

    def peer_supports_feature(self, peer_id: str, feature: str) -> bool:
        """
        Check if a peer manager supports a specific feature.

        Args:
            peer_id: Peer manager node ID
            feature: Feature name to check

        Returns:
            True if the feature is available with this peer
        """
        caps = self._peer_manager_capabilities.get(peer_id)
        if caps is None:
            return False
        return caps.supports(feature)

    def get_worker_capabilities(self, worker_id: str) -> NegotiatedCapabilities | None:
        """Get negotiated capabilities for a worker."""
        return self._worker_capabilities.get(worker_id)

    def get_gate_capabilities(self, gate_id: str) -> NegotiatedCapabilities | None:
        """Get negotiated capabilities for a gate."""
        return self._gate_capabilities.get(gate_id)

    def get_peer_capabilities(self, peer_id: str) -> NegotiatedCapabilities | None:
        """Get negotiated capabilities for a peer manager."""
        return self._peer_manager_capabilities.get(peer_id)

    def remove_worker(self, worker_id: str) -> None:
        """Remove negotiated capabilities when worker disconnects."""
        self._worker_capabilities.pop(worker_id, None)

    def remove_gate(self, gate_id: str) -> None:
        """Remove negotiated capabilities when gate disconnects."""
        self._gate_capabilities.pop(gate_id, None)
        self._state._gate_negotiated_caps.pop(gate_id, None)

    def remove_peer(self, peer_id: str) -> None:
        """Remove negotiated capabilities when peer disconnects."""
        self._peer_manager_capabilities.pop(peer_id, None)

    def is_version_compatible(self, remote_version: ProtocolVersion) -> bool:
        """
        Check if a remote version is compatible with ours.

        Args:
            remote_version: Remote protocol version

        Returns:
            True if versions are compatible (same major version)
        """
        return self.protocol_version.is_compatible_with(remote_version)

    def get_common_features_with_all_workers(self) -> set[str]:
        """
        Get features supported by ALL connected workers.

        Useful for determining which features can be used globally.

        Returns:
            Set of features supported by all workers
        """
        if not self._worker_capabilities:
            return set()

        # Start with our features
        common = set(self.capabilities)

        # Intersect with each worker's negotiated features
        for caps in self._worker_capabilities.values():
            common &= caps.common_features

        return common

    def get_common_features_with_all_gates(self) -> set[str]:
        """
        Get features supported by ALL connected gates.

        Returns:
            Set of features supported by all gates
        """
        if not self._gate_capabilities:
            return set()

        common = set(self.capabilities)
        for caps in self._gate_capabilities.values():
            common &= caps.common_features

        return common

    def get_version_metrics(self) -> dict:
        """Get version skew metrics."""
        worker_versions: dict[str, int] = {}
        gate_versions: dict[str, int] = {}
        peer_versions: dict[str, int] = {}

        for caps in self._worker_capabilities.values():
            version_str = str(caps.remote_version)
            worker_versions[version_str] = worker_versions.get(version_str, 0) + 1

        for caps in self._gate_capabilities.values():
            version_str = str(caps.remote_version)
            gate_versions[version_str] = gate_versions.get(version_str, 0) + 1

        for caps in self._peer_manager_capabilities.values():
            version_str = str(caps.remote_version)
            peer_versions[version_str] = peer_versions.get(version_str, 0) + 1

        return {
            "local_version": str(self.protocol_version),
            "local_feature_count": len(self.capabilities),
            "worker_count": len(self._worker_capabilities),
            "worker_versions": worker_versions,
            "gate_count": len(self._gate_capabilities),
            "gate_versions": gate_versions,
            "peer_count": len(self._peer_manager_capabilities),
            "peer_versions": peer_versions,
        }
