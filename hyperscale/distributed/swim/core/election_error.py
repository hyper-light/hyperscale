"""``ElectionError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error import SwimError


class ElectionError(SwimError):
    """
    Leader election errors.
    
    These are generally handled gracefully by the election protocol
    but should be tracked for debugging.
    """
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.DEGRADED,
        cause: BaseException | None = None,
        **context: object,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.ELECTION,
            severity=severity,
            context=context,
            cause=cause,
        )
