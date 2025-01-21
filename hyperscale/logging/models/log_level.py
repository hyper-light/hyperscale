from __future__ import annotations
from enum import Enum
from typing import Literal

LogLevelName = Literal[
    'trace',
    'debug',
    'info',
    'warn',
    'error',
    'critical',
    'fatal'
]

class LogLevel(Enum):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    FATAL = 'FATAL'

    @classmethod
    def to_level(cls, level_name: LogLevelName) -> LogLevel:
        levels_map = {
            LogLevel.TRACE.value: LogLevel.TRACE,
            LogLevel.DEBUG.value: LogLevel.DEBUG,
            LogLevel.INFO.value: LogLevel.INFO,
            LogLevel.WARN.value: LogLevel.WARN,
            LogLevel.ERROR.value: LogLevel.ERROR,
            LogLevel.CRITICAL.value: LogLevel.CRITICAL,
            LogLevel.FATAL.value: LogLevel.FATAL
        }

        return levels_map.get(level_name.upper())
