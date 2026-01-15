"""
Shared protocols for SWIM module components.

Provides centralized protocol definitions to ensure consistency
across all modules and avoid circular import issues.
"""

from typing import Protocol, Any, runtime_checkable


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
    
    async def log(self, entry: Any) -> None:
        """
        Log a structured entry.
        
        Args:
            entry: A structured log entry, typically a msgspec model
                   like ServerDebug, ServerInfo, or ServerError.
        """
        ...


@runtime_checkable
class TaskRunnerProtocol(Protocol):
    """
    Protocol for managed async task execution.
    
    Provides lifecycle management for background tasks, including
    timeout handling, cleanup tracking, and cancellation support.
    """
    
    def run(
        self,
        func: Any,
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Schedule a function for execution."""
        ...
    
    async def cancel(self, token: str) -> bool:
        """Cancel a task by its token."""
        ...

