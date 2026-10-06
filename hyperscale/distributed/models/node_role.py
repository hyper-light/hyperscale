"""Wire model ``NodeRole`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class NodeRole(str, Enum):
    """Role of a node in the distributed system."""

    CLIENT = "client"
    GATE = "gate"
    MANAGER = "manager"
    WORKER = "worker"
