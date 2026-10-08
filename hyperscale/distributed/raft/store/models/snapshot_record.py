import msgspec


class SnapshotRecord(msgspec.Struct, frozen=True, tag="snapshot", array_like=True):
    """A group's state through ``last_index`` (Raft section 7): the log
    before it is no longer needed. ``configuration`` is the
    ``RaftConfiguration.dump()`` in effect at ``last_index``."""

    group_id: str
    last_index: int
    last_term: int
    configuration: bytes
    state: bytes
