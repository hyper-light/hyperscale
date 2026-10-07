import msgspec


class KeyedStateReleasedRecord(msgspec.Struct, frozen=True, tag="keyed_state_released", array_like=True):
    """``key`` in ``namespace`` holds no state from ``version`` on: nothing
    of it is recovered unless a higher version was written, and
    compaction drops it."""

    namespace: str
    key: str
    version: int
