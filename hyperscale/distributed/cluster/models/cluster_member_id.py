from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class ClusterMemberId:
    """
    A member of a cluster's membership group (AD-52): the process
    incarnation's node id, the TCP address it is reached at, and which
    participation of that incarnation this is.

    The address travels inside the id, so a configuration of member ids is
    also the group's address book. A process that leaves a group comes back
    under a new participation: a Raft member must never vote twice in one
    term, and with no storage to remember its votes only a new id makes
    that certain.

    Text form: ``{node_id}@{host}:{port}#{participation}`` -- the port and
    participation peeled off from the right, so a host may carry colons
    (IPv6).
    """

    node_id: str
    host: str
    port: int
    participation: int

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)

    def __str__(self) -> str:
        return f"{self.node_id}@{self.host}:{self.port}#{self.participation}"

    @classmethod
    def parse(cls, text: str) -> ClusterMemberId:
        """The member id ``text`` names.

        Raises:
            ValueError: ``text`` is not a member id.
        """
        identity, separator, participation = text.rpartition("#")
        node_id, at_sign, address = identity.rpartition("@")
        host, colon, port = address.rpartition(":")
        if not all((separator, at_sign, colon, node_id, host)):
            raise ValueError(f"not a cluster member id: {text!r}")
        return cls(node_id=node_id, host=host, port=int(port), participation=int(participation))
