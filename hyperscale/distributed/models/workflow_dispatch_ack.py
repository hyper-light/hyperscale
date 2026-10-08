"""Wire model ``WorkflowDispatchAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class WorkflowDispatchAck(Message):
    """
    Worker acknowledgment of workflow dispatch.
    """

    workflow_id: str  # Workflow identifier
    accepted: bool  # Whether worker accepted
    error: str | None = None  # Error message if rejected
    cores_assigned: int = 0  # Actual cores assigned
    # The worker's core availability version once it allocated the
    # workflow's cores: any report of its free cores at or past it
    # reflects them.
    cores_version: int = 0
