from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class WorkflowThrottleRequest(Message):
    """AD-41 THROTTLE, manager -> worker: cut a running workflow's
    concurrency to ``scale`` of its operating point, or -- ``scale`` None
    -- restore it."""

    job_id: str
    workflow_id: str
    scale: float | None = None
    reason: str = ""
