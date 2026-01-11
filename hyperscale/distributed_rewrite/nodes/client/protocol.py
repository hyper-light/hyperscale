"""
Protocol negotiation for HyperscaleClient.

Handles version negotiation, capability detection, and server compatibility validation.
Implements AD-25 (Protocol Version Negotiation).
"""

from hyperscale.distributed_rewrite.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    ProtocolVersion,
    NegotiatedCapabilities,
    get_features_for_version,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging import Logger


class ClientProtocol:
    """
    Manages protocol version negotiation and capabilities (AD-25).

    Tracks negotiated capabilities per server (manager/gate) to ensure
    compatibility and feature availability.

    Protocol negotiation flow:
    1. Client sends: CURRENT_PROTOCOL_VERSION + capabilities string
    2. Server responds: server version + server capabilities
    3. Client extracts common features and stores NegotiatedCapabilities
    """

    def __init__(self, state: ClientState, logger: Logger) -> None:
        self._state = state
        self._logger = logger
        # Build our capabilities string once
        self._capabilities_str = ','.join(
            sorted(get_features_for_version(CURRENT_PROTOCOL_VERSION))
        )

    def get_client_capabilities_string(self) -> str:
        """
        Get the client's capabilities string.

        Returns:
            Comma-separated list of supported features
        """
        return self._capabilities_str

    def get_client_protocol_version(self) -> ProtocolVersion:
        """
        Get the client's protocol version.

        Returns:
            Current protocol version
        """
        return CURRENT_PROTOCOL_VERSION

    def negotiate_capabilities(
        self,
        server_addr: tuple[str, int],
        server_version_major: int,
        server_version_minor: int,
        server_capabilities_str: str,
    ) -> NegotiatedCapabilities:
        """
        Negotiate capabilities with a server.

        Extracts server's protocol version and capabilities, determines
        common features, and stores the negotiated result.

        Args:
            server_addr: Server (host, port) tuple
            server_version_major: Server's protocol major version
            server_version_minor: Server's protocol minor version
            server_capabilities_str: Server's comma-separated capabilities

        Returns:
            NegotiatedCapabilities with common features
        """
        server_version = ProtocolVersion(
            major=server_version_major,
            minor=server_version_minor,
        )

        # Parse server capabilities
        server_features = (
            set(server_capabilities_str.split(','))
            if server_capabilities_str
            else set()
        )

        # Get client features
        client_features = set(get_features_for_version(CURRENT_PROTOCOL_VERSION))

        # Determine common features
        common_features = client_features & server_features

        # Create negotiated capabilities
        negotiated = NegotiatedCapabilities(
            local_version=CURRENT_PROTOCOL_VERSION,
            remote_version=server_version,
            common_features=common_features,
            compatible=True,  # Assume compatible if we can negotiate
        )

        # Store in state
        self._state._server_negotiated_caps[server_addr] = negotiated

        return negotiated

    def get_negotiated_capabilities(
        self,
        server_addr: tuple[str, int],
    ) -> NegotiatedCapabilities | None:
        """
        Get previously negotiated capabilities for a server.

        Args:
            server_addr: Server (host, port) tuple

        Returns:
            NegotiatedCapabilities if previously negotiated, else None
        """
        return self._state._server_negotiated_caps.get(server_addr)

    def has_feature(
        self,
        server_addr: tuple[str, int],
        feature: str,
    ) -> bool:
        """
        Check if a feature is supported by a server.

        Args:
            server_addr: Server (host, port) tuple
            feature: Feature name to check

        Returns:
            True if feature is in common features
        """
        negotiated = self.get_negotiated_capabilities(server_addr)
        if not negotiated:
            return False
        return feature in negotiated.common_features

    def validate_server_compatibility(
        self,
        server_addr: tuple[str, int],
        required_features: set[str] | None = None,
    ) -> tuple[bool, str]:
        """
        Validate server compatibility based on negotiated capabilities.

        Args:
            server_addr: Server (host, port) tuple
            required_features: Optional set of required features

        Returns:
            (is_compatible, reason) tuple
        """
        negotiated = self.get_negotiated_capabilities(server_addr)

        if not negotiated:
            return (False, "No negotiated capabilities found")

        if not negotiated.compatible:
            return (False, "Server marked as incompatible")

        if required_features:
            missing = required_features - negotiated.common_features
            if missing:
                return (
                    False,
                    f"Missing required features: {', '.join(sorted(missing))}",
                )

        return (True, "Compatible")

    def handle_rate_limit_response(self, response_data: bytes) -> bool:
        """
        Handle rate limit response from server.

        Placeholder for rate limit response processing.
        Currently returns True if response indicates rate limiting.

        Args:
            response_data: Response bytes from server

        Returns:
            True if rate limited
        """
        # Check for rate limit indicators
        if response_data in (b'rate_limited', b'RATE_LIMITED'):
            return True
        return False
