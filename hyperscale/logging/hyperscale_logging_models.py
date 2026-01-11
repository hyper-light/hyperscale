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
    node_id: str | int
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.TRACE

class RunDebug(Entry, kw_only=True):
    node_id: str | int
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.DEBUG

class RunInfo(Entry, kw_only=True):
    node_id: str | int
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.INFO

class RunError(Entry, kw_only=True):
    node_id: str | int
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.ERROR

class RunFatal(Entry, kw_only=True):
    node_id: str | int
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.FATAL

class ServerTrace(Entry, kw_only=True):
    node_id: str | int
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.TRACE

class ServerDebug(Entry, kw_only=True):
    node_id: int
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.DEBUG

class ServerInfo(Entry, kw_only=True):
    node_id: int
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.INFO

class ServerWarning(Entry, kw_only=True):
    node_id: int
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.WARN

class ServerError(Entry, kw_only=True):
    node_id: int
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.ERROR

class ServerFatal(Entry, kw_only=True):
    node_id: int
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.FATAL

class StatusUpdate(Entry, kw_only=True):
    node_id: int
    node_host: str
    node_port: int
    completed_count: int
    failed_count: int
    avg_cpu: float
    avg_mem_mb: float
    level: LogLevel = LogLevel.TRACE  # TRACE level since this fires every 100ms


class SilentDropStats(Entry, kw_only=True):
    """Periodic summary of silently dropped messages for security monitoring."""
    node_id: int
    node_host: str
    node_port: int
    protocol: str  # "tcp" or "udp"
    rate_limited_count: int
    message_too_large_count: int
    decompression_too_large_count: int
    decryption_failed_count: int
    malformed_message_count: int
    load_shed_count: int = 0  # AD-32: Messages dropped due to priority-based load shedding
    total_dropped: int
    interval_seconds: float
    level: LogLevel = LogLevel.WARN