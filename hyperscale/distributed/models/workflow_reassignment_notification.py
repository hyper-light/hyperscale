"""Wire model ``WorkflowReassignmentNotification`` -- pickled under the wire namespace
``hyperscale.distributed.models.worker_state`` (see that module)."""

import sys
from dataclasses import dataclass

from .message import Message
from .worker_state_constants import _DELIM
from .worker_state_constants import _REASSIGNMENT_REASON_BYTES_CACHE


@dataclass(slots=True, kw_only=True)
class WorkflowReassignmentNotification(Message):
    """
    Notification of workflow reassignment after worker failure.

    Sent via TCP to peer managers when workflows are requeued
    from a failed worker. Enables peers to:
    - Update their tracking of workflow locations
    - Avoid sending results to stale worker assignments
    - Maintain consistent view of workflow state

    This is informational (not authoritative) - the job leader
    remains the source of truth for workflow state.
    """

    job_id: str
    workflow_id: str
    sub_workflow_token: str
    failed_worker_id: str
    reason: str  # "worker_dead", "worker_evicted", "worker_overloaded", "rebalance"
    originating_manager_id: str
    timestamp: float
    datacenter: str = ""

    def to_bytes(self) -> bytes:
        """Serialize for TCP transmission."""
        reason_bytes = _REASSIGNMENT_REASON_BYTES_CACHE.get(self.reason)
        if reason_bytes is None:
            reason_bytes = self.reason.encode()

        parts = [
            self.job_id.encode(),
            self.workflow_id.encode(),
            self.sub_workflow_token.encode(),
            self.failed_worker_id.encode(),
            reason_bytes,
            self.originating_manager_id.encode(),
            f"{self.timestamp:.6f}".encode(),
            self.datacenter.encode(),
        ]

        return _DELIM.join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkflowReassignmentNotification | None":
        """Deserialize from TCP transmission."""
        try:
            decoded = data.decode()
            parts = decoded.split(":", maxsplit=7)

            if len(parts) < 8:
                return None

            return cls(
                job_id=sys.intern(parts[0]),
                workflow_id=sys.intern(parts[1]),
                sub_workflow_token=sys.intern(parts[2]),
                failed_worker_id=sys.intern(parts[3]),
                reason=parts[4],
                originating_manager_id=sys.intern(parts[5]),
                timestamp=float(parts[6]),
                # An empty field is already "" -- taken as is.
                datacenter=parts[7],
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None
