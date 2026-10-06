"""
Shared protocols for SWIM module components.

Provides centralized protocol definitions to ensure consistency
across all modules and avoid circular import issues.

Phase 6c note: ``TaskRunnerProtocol`` was moved to
``hyperscale.distributed.runtime.runner.Runner`` so every consumer
under ``hyperscale/distributed/`` imports the task-runner seam from
one canonical location, consistent with the Phase 5 ``Clock`` /
``Random`` / ``Transport`` seams. This module re-exports it under
the historical name for backward compatibility; new callers should
import ``Runner`` from ``hyperscale.distributed.runtime`` directly.
"""

from typing import Protocol, runtime_checkable

from hyperscale.distributed.runtime import Runner as TaskRunnerProtocol
from hyperscale.logging.models import Entry


__all__ = ["LoggerProtocol", "TaskRunnerProtocol"]


@runtime_checkable
class LoggerProtocol(Protocol):
    """
    Protocol for structured async logging.

    All SWIM components that need logging should accept a LoggerProtocol
    instance via their set_logger() method. This enables structured
    logging with ServerDebug/ServerInfo/ServerError models.

    The logger is async to support non-blocking I/O in the logging
    backend (file writes, network sends, etc.).
    """

    async def log(self, entry: Entry) -> None:
        """
        Log a structured entry.

        Args:
            entry: A structured log entry, typically a msgspec model
                   like ServerDebug, ServerInfo, or ServerError.
        """
        ...

