"""Wire model ``JobProgressReport`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobProgressReport(Message):
    """
    Manager → Gate: Periodic progress report (AD-34 multi-DC coordination).

    Sent every ~10 seconds during job execution to keep gate informed of
    DC-local progress. Used by gate to detect global timeouts and stuck DCs.

    Extension Integration (AD-26):
    - total_extensions_granted: Total seconds of extensions granted in this DC
    - max_worker_extension: Largest extension granted to any single worker
    - workers_with_extensions: Count of workers currently with active extensions
    """

    job_id: str
    datacenter: str
    manager_id: str
    manager_host: str  # For gate to send replies
    manager_port: int
    workflows_total: int
    workflows_completed: int
    workflows_failed: int
    has_recent_progress: bool  # Any workflow progressed in last 10s
    timestamp: float
    fence_token: int  # Manager's fence token

    # Extension tracking (AD-26 integration)
    total_extensions_granted: float = 0.0  # Total seconds granted to workers
    max_worker_extension: float = 0.0  # Largest extension granted
    workers_with_extensions: int = 0  # Count of workers with active extensions
