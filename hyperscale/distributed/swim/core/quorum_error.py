"""``QuorumError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error import SwimError


class QuorumError(SwimError):
    """
    Base class for quorum-related errors.
    
    These errors occur when distributed consensus cannot be achieved,
    such as when too many managers are down or unreachable.
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
            category=ErrorCategory.PROTOCOL,
            severity=severity,
            context=context,
            cause=cause,
        )
