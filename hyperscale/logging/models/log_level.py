from __future__ import annotations
from enum import Enum
from typing import Literal

LogLevelName = Literal[
    'debug',
    'info',
    'warn',
    'error',
    'critical',
]

class LogLevel(Enum):
    NOTSET = "NOTSET"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    def to_level(self, level_name: LogLevelName) -> LogLevel:
        levels_map = {
            LogLevel.NOTSET.value: LogLevel.NOTSET,
            LogLevel.DEBUG.value: LogLevel.DEBUG,
            LogLevel.INFO.value: LogLevel.INFO,
            LogLevel.WARN.value: LogLevel.WARN,
            LogLevel.ERROR.value: LogLevel.ERROR,
            LogLevel.CRITICAL.value: LogLevel.CRITICAL,
        }

        return levels_map.get(level_name.upper())

