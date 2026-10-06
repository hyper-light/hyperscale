"""``AssertionError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .internal_error import InternalError


class AssertionError(InternalError):
    """An internal assertion failed."""
    
    def __init__(self, condition: str, **context: object):
        super().__init__(
            message=f"Assertion failed: {condition}",
            severity=ErrorSeverity.FATAL,
            condition=condition,
            **context,
        )
