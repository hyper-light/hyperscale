"""
Provision state tracking.

Tracks state for quorum-based workflow provisioning during dispatch.
"""

from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class ProvisionState:
    """
    State for tracking quorum-based workflow provisioning.

    Used during workflow dispatch to coordinate confirmation across
    manager peers before committing the dispatch.
    """

    workflow_id: str
    job_id: str
    worker_id: str
    cores_requested: int
    initiated_at: float = field(default_factory=time.monotonic)
    confirmed_nodes: frozenset[str] = field(default_factory=frozenset)
    timeout_seconds: float = 5.0

    def add_confirmation(self, node_id: str) -> "ProvisionState":
        """
        Add a confirmation from a peer node.

        Returns a new state with the updated confirmations set.
        """
        return ProvisionState(
            workflow_id=self.workflow_id,
            job_id=self.job_id,
            worker_id=self.worker_id,
            cores_requested=self.cores_requested,
            initiated_at=self.initiated_at,
            confirmed_nodes=self.confirmed_nodes | {node_id},
            timeout_seconds=self.timeout_seconds,
        )

    def has_quorum(self, quorum_size: int) -> bool:
        """Check if quorum has been achieved."""
        return len(self.confirmed_nodes) >= quorum_size

    @property
    def is_timed_out(self) -> bool:
        """Check if provision request has timed out."""
        return (time.monotonic() - self.initiated_at) > self.timeout_seconds

    @property
    def confirmation_count(self) -> int:
        """Number of confirmations received."""
        return len(self.confirmed_nodes)
