import msgspec


class TruncateFromRecord(msgspec.Struct, frozen=True, tag="truncate_from", array_like=True):
    """A group's log lost every entry from ``index`` on (a conflict with
    its leader's log, Raft section 5.3)."""

    group_id: str
    index: int
