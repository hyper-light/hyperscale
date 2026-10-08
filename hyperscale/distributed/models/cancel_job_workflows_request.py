"""Wire model ``CancelJobWorkflowsRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class CancelJobWorkflowsRequest(Message):
    """
    Request to cancel every workflow on a worker that belongs to a
    given job.

    Sent from Manager -> Worker when the manager wants to cancel a
    job but doesn't have the per-workflow registry populated locally
    — typically right after a leader-failover takeover whose
    peer/worker state-sync left ``job.workflows`` empty on the new
    leader. The worker iterates its ``_active_workflows`` and cancels
    any workflow whose ``progress.job_id`` matches, then returns the
    set of workflow ids it actually cancelled. The new leader uses
    this to seed its cancellation-pending tracker so the canonical
    ``workflow_cancellation_complete`` → ``_push_cancellation_complete_to_origin``
    chain still drives the client-facing completion notification
    — no more "fire the completion push speculatively and hope the
    workers stop on their own" shortcut.

    Idempotent on the worker side: cancelling a workflow that's
    already terminal is a no-op (the cancel handler's
    ``already_completed`` short-circuit). Workers without any
    matching workflow return an empty ``cancelled_workflow_ids``
    list.
    """

    job_id: str  # Job whose workflows should be cancelled
    fence_token: int = 0  # Optional fence for leadership consistency
    requester_id: str = ""
    timestamp: float = 0.0
    reason: str = ""
