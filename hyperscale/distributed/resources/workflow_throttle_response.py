from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class WorkflowThrottleResponse(Message):
    """A worker's answer to a ``WorkflowThrottleRequest``: whether any of
    its executors applied it (False when the workflow is not running
    there, or runs no concurrency-gated loop -- an ACTION workflow), and
    the workflow's total concurrency cap afterwards when one applies."""

    job_id: str
    workflow_id: str
    applied: bool = False
    concurrency_cap: int | None = None
    error: str = ""
