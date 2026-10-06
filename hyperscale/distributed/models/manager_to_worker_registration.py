"""Wire model ``ManagerToWorkerRegistration`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message
from .manager_info import ManagerInfo


@dataclass(slots=True, kw_only=True)
class ManagerToWorkerRegistration(Message):
    """
    Registration request from manager to worker.

    Enables bidirectional registration: workers register with managers,
    AND managers can register with workers discovered via state sync.
    This speeds up cluster formation by allowing managers to proactively
    reach out to workers they learn about from peer managers.
    """

    manager: ManagerInfo  # Registering manager's info
    is_leader: bool  # Whether this manager is the cluster leader
    term: int  # Current leadership term
    known_managers: list[ManagerInfo] = field(
        default_factory=list
    )  # Other managers worker should know
