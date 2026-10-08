from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterStatusRequest(Message):
    """Ask for a cluster's membership as of now (AD-52 section 11): the
    group's leader answers after confirming, by ReadIndex, that it still
    leads; a member that is not the leader passes it on once
    (``forwarded``)."""

    forwarded: bool = False
