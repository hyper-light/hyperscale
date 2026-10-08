from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class FoundClusterReply(Message):
    """Whether a founder adopted a proposed founding (AD-52 formation)."""

    member_id: str
    adopted: bool
