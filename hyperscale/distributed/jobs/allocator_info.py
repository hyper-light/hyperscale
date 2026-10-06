"""``AllocatorInfo`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class AllocatorInfo(Entry, kw_only=True):
    """Info-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.INFO
