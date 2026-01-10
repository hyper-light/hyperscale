"""
Client configuration for HyperscaleClient.

Loads environment settings, defines constants, and provides configuration
for timeouts, intervals, retry policies, and protocol negotiation.
"""

import os
from dataclasses import dataclass


# Transient errors that should trigger retry logic
TRANSIENT_ERRORS = frozenset({
    "syncing",
    "not ready",
    "election in progress",
    "no leader",
    "split brain",
})


@dataclass(slots=True)
class ClientConfig:
    """
    Configuration for HyperscaleClient.

    Combines environment variables, derived constants, and default settings
    for client operation.
    """

    # Network configuration
    host: str
    tcp_port: int
    env: str

    # Target servers
    managers: list[tuple[str, int]]
    gates: list[tuple[str, int]]

    # Orphan job tracking (from environment)
    orphan_grace_period_seconds: float = float(
        os.getenv("CLIENT_ORPHAN_GRACE_PERIOD", "120.0")
    )
    orphan_check_interval_seconds: float = float(
        os.getenv("CLIENT_ORPHAN_CHECK_INTERVAL", "30.0")
    )

    # Response freshness timeout (from environment)
    response_freshness_timeout_seconds: float = float(
        os.getenv("CLIENT_RESPONSE_FRESHNESS_TIMEOUT", "5.0")
    )

    # Leadership retry policy defaults
    leadership_max_retries: int = 3
    leadership_retry_delay_seconds: float = 0.5
    leadership_exponential_backoff: bool = True
    leadership_max_delay_seconds: float = 5.0

    # Job submission retry policy
    submission_max_retries: int = 5
    submission_max_redirects_per_attempt: int = 3

    # Rate limiter configuration
    rate_limit_enabled: bool = True
    rate_limit_health_gated: bool = True

    # Protocol negotiation
    negotiate_capabilities: bool = True

    # Local reporter types (file-based reporters handled by client)
    local_reporter_types: set[str] = None

    def __post_init__(self) -> None:
        """Initialize derived fields."""
        if self.local_reporter_types is None:
            from hyperscale.reporting.common import ReporterTypes

            self.local_reporter_types = {
                ReporterTypes.JSON.name,
                ReporterTypes.CSV.name,
                ReporterTypes.XML.name,
            }


def create_client_config(
    host: str,
    port: int,
    env: str = "local",
    managers: list[tuple[str, int]] | None = None,
    gates: list[tuple[str, int]] | None = None,
) -> ClientConfig:
    """
    Create client configuration with defaults.

    Args:
        host: Client host address
        port: Client TCP port
        env: Environment name (local, dev, prod, etc.)
        managers: List of manager (host, port) tuples
        gates: List of gate (host, port) tuples

    Returns:
        ClientConfig instance
    """
    return ClientConfig(
        host=host,
        tcp_port=port,
        env=env,
        managers=managers or [],
        gates=gates or [],
    )
