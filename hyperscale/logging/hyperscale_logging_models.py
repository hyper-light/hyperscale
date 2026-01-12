from .models import Entry, LogLevel


class TestTrace(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.TRACE


class TestDebug(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.DEBUG


class TestFatal(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.FATAL


class TestError(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.ERROR


class TestInfo(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.INFO


class RemoteManagerInfo(Entry, kw_only=True):
    host: str
    port: int
    with_ssl: bool
    level: LogLevel = LogLevel.INFO


class GraphDebug(Entry, kw_only=True):
    graph: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.DEBUG


class WorkflowTrace(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.TRACE


class WorkflowDebug(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.DEBUG


class WorkflowInfo(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.INFO


class WorkflowError(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.ERROR


class WorkflowFatal(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.FATAL


class RunTrace(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.TRACE


class RunDebug(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.DEBUG


class RunInfo(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.INFO


class RunError(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.ERROR


class RunFatal(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.FATAL


class ServerTrace(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.TRACE


class ServerDebug(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.DEBUG


class ServerInfo(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.INFO


class ServerWarning(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.WARN


class ServerError(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.ERROR


class ServerFatal(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.FATAL


class StatusUpdate(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    completed_count: int
    failed_count: int
    avg_cpu: float
    avg_mem_mb: float
    level: LogLevel = LogLevel.TRACE  # TRACE level since this fires every 100ms


class SilentDropStats(Entry, kw_only=True):
    """Periodic summary of silently dropped messages for security monitoring."""

    node_id: str
    node_host: str
    node_port: int
    protocol: str  # "tcp" or "udp"
    rate_limited_count: int
    message_too_large_count: int
    decompression_too_large_count: int
    decryption_failed_count: int
    malformed_message_count: int
    load_shed_count: int = (
        0  # AD-32: Messages dropped due to priority-based load shedding
    )
    total_dropped: int
    interval_seconds: float
    level: LogLevel = LogLevel.WARN


class IdempotencyInfo(Entry, kw_only=True):
    component: str
    idempotency_key: str | None = None
    job_id: str | None = None
    level: LogLevel = LogLevel.INFO


class IdempotencyWarning(Entry, kw_only=True):
    component: str
    idempotency_key: str | None = None
    job_id: str | None = None
    level: LogLevel = LogLevel.WARN


class IdempotencyError(Entry, kw_only=True):
    component: str
    idempotency_key: str | None = None
    job_id: str | None = None
    level: LogLevel = LogLevel.ERROR


class WALDebug(Entry, kw_only=True):
    path: str
    level: LogLevel = LogLevel.DEBUG


class WALInfo(Entry, kw_only=True):
    path: str
    level: LogLevel = LogLevel.INFO


class WALWarning(Entry, kw_only=True):
    path: str
    error_type: str | None = None
    level: LogLevel = LogLevel.WARN


class WALError(Entry, kw_only=True):
    path: str
    error_type: str
    level: LogLevel = LogLevel.ERROR


class WorkerStarted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    manager_host: str | None = None
    manager_port: int | None = None
    level: LogLevel = LogLevel.INFO


class WorkerStopping(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    reason: str | None = None
    level: LogLevel = LogLevel.INFO


class WorkerJobReceived(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    source_manager_host: str
    source_manager_port: int
    level: LogLevel = LogLevel.INFO


class WorkerJobStarted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    level: LogLevel = LogLevel.INFO


class WorkerJobCompleted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    duration_ms: float
    level: LogLevel = LogLevel.INFO


class WorkerJobFailed(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    error_type: str
    duration_ms: float
    level: LogLevel = LogLevel.ERROR


class WorkerActionStarted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    action_name: str
    level: LogLevel = LogLevel.TRACE


class WorkerActionCompleted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    action_name: str
    duration_ms: float
    level: LogLevel = LogLevel.TRACE


class WorkerActionFailed(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    action_name: str
    error_type: str
    duration_ms: float
    level: LogLevel = LogLevel.WARN


class WorkerHealthcheckReceived(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    source_host: str
    source_port: int
    level: LogLevel = LogLevel.TRACE


class WorkerExtensionRequested(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    requested_seconds: float
    level: LogLevel = LogLevel.DEBUG
