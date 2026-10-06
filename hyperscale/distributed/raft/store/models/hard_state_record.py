import msgspec


class HardStateRecord(msgspec.Struct, frozen=True, tag="hard_state", array_like=True):
    """A group's current term and the vote cast in it (Raft Figure 2)."""

    group_id: str
    term: int
    voted_for: str | None
