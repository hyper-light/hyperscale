"""``VersionedState`` -- pickled under the namespace
``hyperscale.distributed.server.events.lamport_clock`` (see that module)."""

from dataclasses import dataclass
from typing import TypeVar, Generic

EntityT = TypeVar("EntityT")


@dataclass(slots=True)
class VersionedState(Generic[EntityT]):
    """
    State with a version number for staleness detection.

    Attributes:
        entity_id: The ID of the entity this state belongs to.
        version: The Lamport clock time when this state was created.
        data: The actual state data.
    """

    entity_id: str
    version: int
    data: EntityT
