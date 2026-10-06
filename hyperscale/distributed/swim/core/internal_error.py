"""``InternalError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error import SwimError


class InternalError(SwimError):
    """
    Internal errors indicating bugs or unexpected conditions.
    
    These should be investigated and fixed. They may indicate:
    - Logic errors
    - Assertion failures
    - Unexpected exceptions from dependencies
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
            category=ErrorCategory.INTERNAL,
            severity=severity,
            context=context,
            cause=cause,
        )
