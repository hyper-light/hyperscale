from dataclasses import dataclass


@dataclass(slots=True)
class ViolationState:
    """One workflow's ongoing violation of one budgeted resource."""

    worker_id: str
    job_id: str
    started_at: float
    warning_sent: bool = False
    warned_at: float | None = None
    throttled_at: float | None = None
    certain_since: float | None = None
    kill_requested_at: float | None = None
    kill_attempts: int = 0
