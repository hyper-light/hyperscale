"""``AllocatorTrace`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class AllocatorTrace(Entry, kw_only=True):
    """Trace-level logging for CoreAllocator operations."""
    worker_id: str
    workflow_id: str
    total_cores: int
    available_cores: int
    active_workflows: int
    level: LogLevel = LogLevel.TRACE
