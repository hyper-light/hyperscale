"""``JobManagerDebug`` -- pickled under the namespace
``hyperscale.distributed.jobs.logging_models`` (see that module)."""

from hyperscale.logging.models import Entry, LogLevel


class JobManagerDebug(Entry, kw_only=True):
    """Debug-level logging for JobManager operations."""
    manager_id: str
    datacenter: str
    job_id: str = ""
    workflow_id: str = ""
    sub_workflow_token: str = ""
    level: LogLevel = LogLevel.DEBUG
