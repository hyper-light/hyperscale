"""``WorkerPoolError`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class WorkerPoolError(Entry, kw_only=True):
    """Error-level logging for WorkerPool operations."""
    manager_id: str
    datacenter: str
    worker_count: int
    healthy_worker_count: int
    total_cores: int
    available_cores: int
    level: LogLevel = LogLevel.ERROR
