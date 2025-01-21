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

class TestInfo(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.INFO

