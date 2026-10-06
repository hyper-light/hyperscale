"""Wire model ``JobTimeoutReport`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .job_global_timeout import JobGlobalTimeout


@dataclass(slots=True)
class JobTimeoutReport(Message):
    """
    Manager → Gate: DC-local timeout detected (AD-34 multi-DC coordination).

    Sent when manager detects job timeout or stuck workflows in its datacenter.
    Gate aggregates timeout reports from all DCs to declare global timeout.

    Manager sends this but does NOT mark job failed locally - waits for gate's
    global timeout decision (JobGlobalTimeout).
    """

    job_id: str
    datacenter: str
    manager_id: str
    manager_host: str
    manager_port: int
    reason: str  # "timeout" | "stuck" | other descriptive reason
    elapsed_seconds: float
    fence_token: int
