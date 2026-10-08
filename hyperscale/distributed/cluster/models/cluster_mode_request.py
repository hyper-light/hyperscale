from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterModeRequest(Message):
    """Set the cluster's mode (AD-52 section 13) -- the whole state, so a
    repeated request changes nothing: ``open``; ``frozen``, no membership
    change commits; ``read-only``, frozen and refusing job submissions too.
    A member that is not the group's leader passes it on once
    (``forwarded``)."""

    mode: str
    forwarded: bool = False
