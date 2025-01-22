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