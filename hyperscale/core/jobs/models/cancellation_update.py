from .workflow_cancellation_status import WorkflowCancellationStatus
from .workflow_cancellation_update import WorkflowCancellationUpdate


class CancellationUpdate:

    __slots__ = (
        "run_id",
        "workflow_name",
        "errors",
        "cancelled",
        "not_found",
        "in_progress",
        "requested",
        "failed",
        "expected_cancellations"
    )

    def __init__(
        self,
        run_id: int,
        workflow_name: str,
        cancellation_status_counts: dict[WorkflowCancellationStatus, list[WorkflowCancellationUpdate]],
        expected_cancellations: int,

    ):
        self.run_id = run_id
        self.workflow_name = workflow_name
        self.cancelled = cancellation_status_counts[WorkflowCancellationStatus.CANCELLED]
        self.in_progress = cancellation_status_counts[WorkflowCancellationStatus.IN_PROGRESS]
        self.not_found = cancellation_status_counts[WorkflowCancellationStatus.NOT_FOUND]
        self.requested = cancellation_status_counts[WorkflowCancellationStatus.REQUESTED]
        self.failed = cancellation_status_counts[WorkflowCancellationStatus.FAILED]
        self.expected_cancellations = expected_cancellations
