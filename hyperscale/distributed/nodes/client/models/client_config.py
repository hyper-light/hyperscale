"""
``ClientConfig``: configuration for HyperscaleClient, read from the client's Env.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.env import Env
from hyperscale.reporting.common import ReporterTypes


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

    # Base of the exponential, jittered back-off (AD-21) between retries
    # of a transient refusal that carries no ``retry_after_seconds`` hint.
    # It is one OVERLOAD_SAMPLE_INTERVAL_SECONDS: a gate or manager cannot
    # change its admission verdict faster than it re-samples its load, so
    # an un-hinted retry sooner than that meets the same verdict. A hinted
    # refusal waits its hint instead.
    retry_base_delay_seconds: float

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
            retry_base_delay_seconds=env.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            result_drain_timeout_seconds=env.CLIENT_RESULT_DRAIN_TIMEOUT,
            job_retention_seconds=env.CLIENT_JOB_RETENTION_SECONDS,
            reporter_submission_timeout_seconds=env.REPORTER_SUBMISSION_TIMEOUT_SECONDS,
        )
