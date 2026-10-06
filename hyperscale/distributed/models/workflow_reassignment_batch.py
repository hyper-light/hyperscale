"""Wire model ``WorkflowReassignmentBatch`` -- pickled under the wire namespace
``hyperscale.distributed.models.worker_state`` (see that module)."""

import sys
from dataclasses import dataclass

from .message import Message
from .worker_state_constants import _REASSIGNMENT_REASON_BYTES_CACHE


@dataclass(slots=True, kw_only=True)
class WorkflowReassignmentBatch(Message):
    """
    Batch of workflow reassignment notifications.

    Used when multiple workflows need reassignment (e.g., worker death
    affecting multiple running workflows). Reduces TCP overhead.
    """

    originating_manager_id: str
    failed_worker_id: str
    reason: str
    timestamp: float
    datacenter: str
    reassignments: list[
        tuple[str, str, str]
    ]  # (job_id, workflow_id, sub_workflow_token)

    def to_bytes(self) -> bytes:
        """Serialize for TCP transmission."""
        reason_bytes = _REASSIGNMENT_REASON_BYTES_CACHE.get(self.reason)
        if reason_bytes is None:
            reason_bytes = self.reason.encode()

        # Header: manager_id|worker_id|reason|timestamp|datacenter|count
        header_parts = [
            self.originating_manager_id.encode(),
            self.failed_worker_id.encode(),
            reason_bytes,
            f"{self.timestamp:.6f}".encode(),
            self.datacenter.encode(),
            str(len(self.reassignments)).encode(),
        ]
        header = b"|".join(header_parts)

        # Each reassignment: job_id:workflow_id:sub_token
        reassignment_parts = [
            f"{job_id}:{workflow_id}:{sub_token}".encode()
            for job_id, workflow_id, sub_token in self.reassignments
        ]

        # Combine: header||reassignment1||reassignment2||...
        all_parts = [header] + reassignment_parts
        return b"||".join(all_parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkflowReassignmentBatch | None":
        """Deserialize from TCP transmission."""
        try:
            parts = data.split(b"||")
            if not parts:
                return None

            # Parse header
            header = parts[0].split(b"|")
            if len(header) < 6:
                return None

            originating_manager_id = sys.intern(header[0].decode())
            failed_worker_id = sys.intern(header[1].decode())
            reason = header[2].decode()
            timestamp = float(header[3].decode())
            datacenter = header[4].decode()
            count = int(header[5].decode())

            # Parse reassignments
            reassignments: list[tuple[str, str, str]] = []
            for reassignment_bytes in parts[1 : count + 1]:
                reassignment_parts = reassignment_bytes.decode().split(":", maxsplit=2)
                if len(reassignment_parts) == 3:
                    reassignments.append(
                        (
                            sys.intern(reassignment_parts[0]),
                            sys.intern(reassignment_parts[1]),
                            sys.intern(reassignment_parts[2]),
                        )
                    )

            return cls(
                originating_manager_id=originating_manager_id,
                failed_worker_id=failed_worker_id,
                reason=reason,
                timestamp=timestamp,
                datacenter=datacenter,
                reassignments=reassignments,
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None
