import msgspec


class GroupReleasedRecord(msgspec.Struct, frozen=True, tag="group_released", array_like=True):
    """A group this node no longer takes part in: nothing of it is
    recovered, and compaction drops it."""

    group_id: str
