"""``NetworkError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error import SwimError


class NetworkError(SwimError):
    """
    Network-related failures.
    
    These are typically transient and should trigger retry with backoff.
    Examples: timeouts, connection refused, DNS failures.
    """
    
    def __init__(
        self, 
        message: str, 
        severity: ErrorSeverity = ErrorSeverity.TRANSIENT,
        cause: BaseException | None = None,
        **context: object,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.NETWORK,
            severity=severity,
            context=context,
            cause=cause,
        )
