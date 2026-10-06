"""``WALEntryState`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.entry_state`` (see that module)."""

from enum import IntEnum


class WALEntryState(IntEnum):
    """
    State machine for WAL entries tracking durability progress.

    Transitions: PENDING -> REGIONAL -> GLOBAL -> APPLIED -> COMPACTED
    """

    PENDING = 0
    REGIONAL = 1
    GLOBAL = 2
    APPLIED = 3
    COMPACTED = 4
