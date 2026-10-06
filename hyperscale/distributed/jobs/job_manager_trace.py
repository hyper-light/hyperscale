"""``JobManagerTrace`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class JobManagerTrace(Entry, kw_only=True):
    """Trace-level logging for JobManager operations."""
    manager_id: str
    datacenter: str
    job_id: str = ""
    workflow_id: str = ""
    sub_workflow_token: str = ""
    level: LogLevel = LogLevel.TRACE
