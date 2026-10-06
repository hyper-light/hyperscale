"""Wire model ``DatacenterStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class DatacenterStatus(Message):
    """
    Status of a datacenter for routing decisions.

    Used by gates to classify datacenter health and make
    intelligent routing decisions with fallback support.

    See AD-16 in docs/architecture.md for design rationale.
    """

    dc_id: str
    health: str
    available_capacity: int = 0
    manager_count: int = 0
    worker_count: int = 0
    last_update: float = 0.0
    overloaded_worker_count: int = 0
    stressed_worker_count: int = 0
    busy_worker_count: int = 0
    worker_overload_ratio: float = 0.0
    health_severity_weight: float = 1.0
    overloaded_manager_count: int = 0
    stressed_manager_count: int = 0
    busy_manager_count: int = 0
    manager_overload_ratio: float = 0.0
    leader_overloaded: bool = False
