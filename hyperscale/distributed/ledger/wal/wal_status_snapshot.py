from __future__ import annotations

import msgspec


class WALStatusSnapshot(msgspec.Struct, frozen=True):
    next_lsn: int
    last_synced_lsn: int
    pending_count: int
    closed: bool

    @classmethod
    def initial(cls) -> WALStatusSnapshot:
        return cls(
            next_lsn=0,
            last_synced_lsn=-1,
            pending_count=0,
            closed=False,
        )
