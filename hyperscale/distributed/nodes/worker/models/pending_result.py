"""``PendingResult`` -- pickled under the namespace
``hyperscale.distributed.nodes.worker.progress`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import WorkflowFinalResult


@dataclass(slots=True)
class PendingResult:
    final_result: WorkflowFinalResult
    enqueued_at: float
    retry_count: int = 0
    next_retry_at: float = 0.0
