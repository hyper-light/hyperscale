import contextvars
from typing import List

from mkfst.logging.models import LogLevel

from .log_level_map import LogLevelMap

_global_log_level = contextvars.ContextVar("_global_log_level", default=LogLevel.INFO)
_global_enabled_loggers = contextvars.ContextVar("_global_enabled_loggers", default=[])
_global_level_map = contextvars.ContextVar("_global_level_map", default=LogLevelMap())


class LoggingConfig:
    def __init__(self) -> None:
        self._log_level: contextvars.ContextVar[LogLevel] = _global_log_level
        self._enabled_loggers: contextvars.ContextVar[List[str]] = (
            _global_enabled_loggers
        )
        self._level_map = _global_level_map.get()

    def set_log_level(self, level: LogLevel):
        self._log_level.set(level)

    def enabled(self, logger_name: str, log_level: LogLevel) -> bool:
        enabled_loggers = self._enabled_loggers.get()
        current_log_level = self._log_level.get()
        return (len(enabled_loggers) < 1 or logger_name in enabled_loggers) and (
            self._level_map[log_level] >= self._level_map[current_log_level]
        )

    @property
    def level(self):
        return self._log_level.get()
