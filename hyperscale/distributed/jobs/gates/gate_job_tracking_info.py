"""``GateJobTrackingInfo`` -- pickled under the namespace
``hyperscale.distributed.jobs.gates.gate_job_timeout_tracker`` (see that module)."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class GateJobTrackingInfo:
    """
    Gate's view of a job across all DCs (AD-34 Part 5).

    Tracks per-DC progress, timeouts, and extension data to enable
    global timeout decisions.
    """

    job_id: str
    """Job identifier."""

    submitted_at: float
    """Global start time (monotonic)."""

    timeout_seconds: float
    """Job timeout in seconds."""

    target_datacenters: list[str]
    """DCs where this job is running."""

    dc_status: dict[str, str] = field(default_factory=dict)
    """DC -> "running" | "completed" | "failed" | "timed_out" | "cancelled"."""

    dc_last_progress: dict[str, float] = field(default_factory=dict)
    """DC -> last progress timestamp (monotonic)."""

    dc_manager_addrs: dict[str, tuple[str, int]] = field(default_factory=dict)
    """DC -> current manager (host, port) for sending timeout decisions."""

    dc_fence_tokens: dict[str, int] = field(default_factory=dict)
    """DC -> manager's fence token (for stale rejection)."""

    # Extension tracking (AD-26 integration)
    dc_total_extensions: dict[str, float] = field(default_factory=dict)
    """DC -> total extension seconds granted."""

    dc_max_extension: dict[str, float] = field(default_factory=dict)
    """DC -> largest single extension granted."""

    dc_workers_with_extensions: dict[str, int] = field(default_factory=dict)
    """DC -> count of workers with active extensions."""

    # Global timeout state
    globally_timed_out: bool = False
    """Whether gate has declared global timeout."""

    timeout_reason: str = ""
    """Reason for global timeout."""

    timeout_fence_token: int = 0
    """Gate's fence token for this timeout decision."""
