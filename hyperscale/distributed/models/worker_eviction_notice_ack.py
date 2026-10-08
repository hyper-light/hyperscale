"""Wire model ``WorkerEvictionNoticeAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .worker_eviction_notice import WorkerEvictionNotice


@dataclass(slots=True)
class WorkerEvictionNoticeAck(Message):
    """
    Worker's acknowledgment of a WorkerEvictionNotice.

    Receipt of this ack discharges the manager's notice obligation —
    the worker now KNOWS it was deregistered (re-registration
    independently discharges it too, covering a lost ack).

    Sent from: Worker -> Manager
    """

    worker_id: str  # Acknowledging worker's node id
    will_reregister: bool  # Whether the worker is scheduling re-registration
