"""
Gate cancellation coordination module (AD-20).

Provides infrastructure for coordinating job cancellation across DCs.

Note: The actual cancellation coordination logic is currently inline in
gate.py. This module documents the cancellation flow and exports the
relevant message types.

Cancellation Flow:
1. Client sends JobCancelRequest to gate
2. Gate forwards CancelJob to all DC managers
3. Managers cancel workflows, send WorkflowCancellationStatus updates
4. Managers send JobCancellationComplete when done
5. Gate aggregates and sends final status to client
"""

from hyperscale.distributed_rewrite.models import (
    CancelJob,
    CancelAck,
    JobCancelRequest,
    JobCancelResponse,
    JobCancellationComplete,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkflowCancellationStatus,
)

__all__ = [
    "CancelJob",
    "CancelAck",
    "JobCancelRequest",
    "JobCancelResponse",
    "JobCancellationComplete",
    "SingleWorkflowCancelRequest",
    "SingleWorkflowCancelResponse",
    "WorkflowCancellationStatus",
]
