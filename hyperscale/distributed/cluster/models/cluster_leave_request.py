from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterLeaveRequest(Message):
    """Release the membership of whoever holds ``host:port`` (AD-52
    section 13).

    A member draining itself names itself (``member_id``): only that member
    is released, never a newer process at its address. An operator's
    force-remove names only the address: the group's leader first greets
    it, and refuses while the holder still answers -- a live member would
    only claim its address again. A member that is not the leader passes a
    drain on to it (``forwarded``, never passed on twice) and answers a
    force-remove with the leader, for the operator to ask: the leader's
    greeting of a silent address can take a whole request timeout, which a
    forwarding hop of the same timeout would cut off.
    """

    host: str
    port: int
    member_id: str | None = None
    forwarded: bool = False
