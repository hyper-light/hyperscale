"""``UnexpectedError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .internal_error import InternalError


class UnexpectedError(InternalError):
    """An unexpected exception occurred."""
    
    def __init__(self, cause: BaseException, operation: str = "unknown"):
        super().__init__(
            message=f"Unexpected error during {operation}: {cause}",
            severity=ErrorSeverity.DEGRADED,
            cause=cause,
            operation=operation,
        )
