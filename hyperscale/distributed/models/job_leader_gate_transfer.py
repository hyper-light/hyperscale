"""Wire model ``JobLeaderGateTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeaderGateTransfer(Message):
    """
    Notification that job leadership has transferred to a new gate.

    Sent from the new job leader gate to all managers in relevant DCs
    when gate failure triggers job ownership transfer. Managers update
    their origin_gate_addr to route results to the new leader.

    This is part of Direct DC-to-Job-Leader Routing:
    - Gate-A fails while owning job-123
    - The SWIM cluster leader gate takes over with a higher fence token
    - Gate-B sends JobLeaderGateTransfer to managers
    - Managers update _job_origin_gates[job-123] = Gate-B address
    """

    job_id: str  # Job being transferred
    new_gate_id: str  # Node ID of new job leader gate
    new_gate_addr: tuple[str, int]  # TCP address of new leader gate
    fence_token: int  # Incremented fence token for consistency
    old_gate_id: str | None = None  # Node ID of old leader gate (if known)
