from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterHello(Message):
    """A node's greeting to a founder of its cluster (AD-52 formation):
    who it is in the membership group, and the founding cohort it was
    configured with (``founders_digest``) -- nodes configured with
    different cohorts would count different quorums."""

    member_id: str
    founders_digest: str
