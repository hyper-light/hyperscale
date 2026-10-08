"""``JobManagerCritical`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class JobManagerCritical(Entry, kw_only=True):
    """Critical-level logging for JobManager operations."""
    manager_id: str
    datacenter: str
    job_id: str = ""
    workflow_id: str = ""
    sub_workflow_token: str = ""
    level: LogLevel = LogLevel.CRITICAL
