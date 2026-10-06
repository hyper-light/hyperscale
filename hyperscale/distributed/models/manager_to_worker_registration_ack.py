"""Wire model ``ManagerToWorkerRegistrationAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class ManagerToWorkerRegistrationAck(Message):
    """
    Acknowledgment from worker to manager registration.
    """

    accepted: bool  # Whether registration was accepted
    worker_id: str  # Worker's node_id
    total_cores: int = 0  # Worker's total cores
    available_cores: int = 0  # Worker's available cores
    error: str | None = None  # Error message if not accepted
