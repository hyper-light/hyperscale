"""Wire model ``WorkerEvictionNotice`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class WorkerEvictionNotice(Message):
    """
    Notification that a manager has deregistered (evicted) a worker.

    Eviction was previously one-sided: the manager dropped the worker
    from its registry and detached its SWIM membership, but the worker
    — still receiving the manager's heartbeats — believed the
    relationship was healthy and never re-registered, diverging
    silently forever. This notice closes the loop: the manager pushes
    it at deregistration time and re-sends it with backoff while the
    obligation is outstanding; the worker responds by marking the
    manager unhealthy and re-registering.

    Sent from: Manager -> Worker
    """

    manager_id: str  # Evicting manager's node id (worker registry key)
    manager_tcp_host: str  # Manager TCP host the worker should re-register with
    manager_tcp_port: int  # Manager TCP port the worker should re-register with
    worker_id: str  # The deregistered worker's node id
    reason: str  # Why the worker was deregistered (e.g. "worker_failure")
