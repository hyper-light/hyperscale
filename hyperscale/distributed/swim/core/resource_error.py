"""``ResourceError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error import SwimError


class ResourceError(SwimError):
    """
    Resource exhaustion errors.
    
    These indicate the node is under stress and may need to:
    - Shed load
    - Increase LHM score
    - Step down from leadership
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
            category=ErrorCategory.RESOURCE,
            severity=severity,
            context=context,
            cause=cause,
        )
