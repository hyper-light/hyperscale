import contextvars
from typing import List, Literal

from hyperscale.logging.models import LogLevel, LogLevelName
from .log_level_map import LogLevelMap
from .stream_type import StreamType


LogOutput = Literal['stdout', 'stderr']

_global_log_level = contextvars.ContextVar("_global_log_level", default=LogLevel.INFO)
_global_disabled_loggers = contextvars.ContextVar("_global_disabled_loggers", default=[])
_global_level_map = contextvars.ContextVar("_global_level_map", default=LogLevelMap())
_global_log_output_type = contextvars.ContextVar("_global_log_level_type", default=StreamType.STDOUT)
_global_logging_directory = contextvars.ContextVar("_global_logging_directory")


class LoggingConfig:
    def __init__(self) -> None:
        self._log_level: contextvars.ContextVar[LogLevel] = _global_log_level
        self._log_output_type: contextvars.ContextVar[StreamType] = _global_log_output_type
        self._log_directory: contextvars.ContextVar[str | None] = _global_logging_directory

        self._disabled_loggers: contextvars.ContextVar[List[str]] = (
            _global_disabled_loggers
        )
        self._level_map = _global_level_map.get()

    def update(
        self, 
        log_directory: str | None = None,
        log_level: LogLevelName | None = None,
        log_output: LogOutput | None = None,
    ):
        if log_directory:
            self._log_directory.set(log_directory)

        if log_level:
            self._log_level.set(
                LogLevel.to_level(log_level)
            )

        if log_output:
            self._log_output_type.set(
                StreamType.STDOUT if log_output == 'stdout' else StreamType.STDERR
            )

    def enabled(self, logger_name: str, log_level: LogLevel) -> bool:
        disabled_loggers = self._disabled_loggers.get()
        current_log_level = self._log_level.get()
        return logger_name not in disabled_loggers and (
            self._level_map[log_level] >= self._level_map[current_log_level]
        )

    @property
    def level(self):
        return self._log_level.get()

    @property
    def output(self):
        return self._log_output_type.get()
    
    @property
    def directory(self):
        return self._log_directory.get()
