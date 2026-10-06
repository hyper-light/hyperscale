"""Wire model ``StepStats`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class StepStats(Message):
    """
    Statistics for a single workflow step.
    """

    step_name: str  # Step method name
    completed_count: int = 0  # Successful executions
    failed_count: int = 0  # Failed executions
    total_count: int = 0  # Total attempts
