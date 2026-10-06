"""``DispatcherTrace`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class DispatcherTrace(Entry, kw_only=True):
    """Trace-level logging for WorkflowDispatcher operations."""
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    pending_count: int
    dispatched_count: int
    level: LogLevel = LogLevel.TRACE
