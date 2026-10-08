import msgspec

from hyperscale.distributed.raft.models import RaftLogEntry


class EntriesRecord(msgspec.Struct, frozen=True, tag="entries", array_like=True):
    """Entries appended to a group's log, the first one directly after the
    log's last."""

    group_id: str
    entries: list[RaftLogEntry]
