"""``WorkerPoolDebug`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class WorkerPoolDebug(Entry, kw_only=True):
    """Debug-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.DEBUG
