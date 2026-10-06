"""``ErrorContext`` -- pickled under the namespace
``hyperscale.distributed.swim.core.error_handler`` (see that module)."""

import asyncio

from .errors import ErrorCategory
from .error_handler_impl import ErrorHandler


class ErrorContext:
    """
    Async context manager for consistent error handling.

    Example:
        async with ErrorContext(handler, "probe_round") as ctx:
            await probe_node(target)
            ctx.record_success(ErrorCategory.NETWORK)
    """

    def __init__(
        self,
        handler: ErrorHandler,
        operation: str,
        reraise: bool = False,
    ):
        self.handler = handler
        self.operation = operation
        self.reraise = reraise

    async def __aenter__(self) -> "ErrorContext":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_val is not None:
            # System-level exceptions must NEVER be suppressed
            if isinstance(exc_val, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                return False  # Always propagate

            # CancelledError is not an error - it's a normal signal for task cancellation
            # Log at debug level for visibility but don't treat as error or update metrics
            if isinstance(exc_val, asyncio.CancelledError):
                await self.handler._log_internal(
                    f"Operation '{self.operation}' cancelled (normal during shutdown)"
                )
                return False  # Don't suppress, let it propagate

            await self.handler.handle_exception(exc_val, self.operation)
            return not self.reraise  # Suppress exception unless reraise=True
        return False

    def record_success(self, category: ErrorCategory) -> None:
        """Record successful operation for circuit breaker."""
        self.handler.record_success(category)
