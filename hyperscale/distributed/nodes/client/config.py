"""
Client configuration for HyperscaleClient, read from the client's Env.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.env import Env
from hyperscale.reporting.common import ReporterTypes


# Transient errors that should trigger retry logic (AD-21, AD-32)
# Includes cluster state errors and load shedding/rate limiting patterns
# The transient-rejection vocabulary is a protocol-level contract shared
# by every hop that classifies JobAck rejections (client submitters AND
# the gate's datacenter dispatch); it lives in
# ``hyperscale.distributed.protocol.transient_errors`` and is re-exported
# here for the existing client-side importers.
from hyperscale.distributed.protocol.transient_errors import (
    TRANSIENT_ERRORS as TRANSIENT_ERRORS,
)


def _file_reporter_types() -> set[ReporterTypes]:
    return {ReporterTypes.JSON, ReporterTypes.CSV, ReporterTypes.XML}


@dataclass(slots=True)
class ClientConfig:
    """Configuration for HyperscaleClient."""

    # Network configuration
    host: str
    tcp_port: int

    # Target servers
    managers: list[tuple[str, int]]
    gates: list[tuple[str, int]]

    # Orphan job tracking
    orphan_grace_period_seconds: float
    orphan_check_interval_seconds: float

    # Response freshness after a leadership change
    response_freshness_timeout_seconds: float

    # Request timeouts
    status_query_timeout_seconds: float
    submission_timeout_seconds: float

    # Job submission retry policy
    submission_max_retries: int
    submission_max_redirects_per_attempt: int

    # Wait for a completed job's in-flight workflow results
    result_drain_timeout_seconds: float

    # How long a finished job stays tracked
    job_retention_seconds: float

    # How long one local reporter may take to connect and submit (and,
    # separately, to close)
    reporter_submission_timeout_seconds: float

    # Reporters the client itself writes (file-based), compared with each
    # reporting config's ``reporter_type`` member.
    local_reporter_types: set[ReporterTypes] = field(default_factory=_file_reporter_types)

    @classmethod
    def from_env(
        cls,
        env: Env,
        host: str,
        tcp_port: int,
        managers: list[tuple[str, int]],
        gates: list[tuple[str, int]],
    ) -> "ClientConfig":
        return cls(
            host=host,
            tcp_port=tcp_port,
            managers=managers,
            gates=gates,
            orphan_grace_period_seconds=env.CLIENT_ORPHAN_GRACE_PERIOD,
            orphan_check_interval_seconds=env.CLIENT_ORPHAN_CHECK_INTERVAL,
            response_freshness_timeout_seconds=env.CLIENT_RESPONSE_FRESHNESS_TIMEOUT,
            status_query_timeout_seconds=env.CLIENT_STATUS_QUERY_TIMEOUT,
            submission_timeout_seconds=env.CLIENT_SUBMISSION_TIMEOUT,
            submission_max_retries=env.CLIENT_SUBMISSION_MAX_RETRIES,
            submission_max_redirects_per_attempt=env.CLIENT_SUBMISSION_MAX_REDIRECTS,
            result_drain_timeout_seconds=env.CLIENT_RESULT_DRAIN_TIMEOUT,
            job_retention_seconds=env.CLIENT_JOB_RETENTION_SECONDS,
            reporter_submission_timeout_seconds=env.REPORTER_SUBMISSION_TIMEOUT_SECONDS,
        )
