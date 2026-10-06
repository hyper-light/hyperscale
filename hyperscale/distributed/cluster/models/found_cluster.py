from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class FoundCluster(Message):
    """A founding proposed to a founder (AD-52 formation): the new
    cluster's uuid and the member ids that found it. A founder adopts the
    first founding it is offered that names it, and no other."""

    cluster_uuid: str
    founding_voters: list[str]
    founders_digest: str
