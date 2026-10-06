import msgspec


class RaftIdentity(msgspec.Struct, frozen=True):
    """Who a node's Raft store belongs to (D1 P4): the node id it resumes
    (``NodeId.full``, start time included), how many times it has left a
    membership group (its participation), and a stamp drawn once, when the
    identity was made, that every store file of this identity carries."""

    format_version: int
    node_id_full: str
    participation: int
    stamp: bytes
