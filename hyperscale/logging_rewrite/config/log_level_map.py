from typing import Dict

from hyperscale.logging_rewrite.models import LogLevel


class LogLevelMap:
    def __init__(self) -> None:
        self._levels: Dict[LogLevel, int] = {
            LogLevel.NOTSET: 0,
            LogLevel.DEBUG: 1,
            LogLevel.INFO: 2,
            LogLevel.WARN: 3,
            LogLevel.ERROR: 4,
            LogLevel.CRITICAL: 5,
        }

    def __getitem__(self, level: LogLevel):
        return self._levels[level]
