"""``ProtocolError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error import SwimError


class ProtocolError(SwimError):
    """
    Protocol violations or unexpected messages.
    
    These may indicate:
    - Malformed messages
    - Version mismatch between nodes
    - Unexpected state transitions
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
