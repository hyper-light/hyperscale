"""``RaftDebug`` -- pickled under the namespace
``hyperscale.distributed.raft.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class RaftDebug(Entry, kw_only=True):
    """Debug-level logging for Raft consensus operations."""
    node_id: str
    job_id: str = ""
    term: int = 0
    role: str = ""
    commit_index: int = 0
    level: LogLevel = LogLevel.DEBUG
