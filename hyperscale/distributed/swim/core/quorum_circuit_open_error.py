"""``QuorumCircuitOpenError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .quorum_error import QuorumError


class QuorumCircuitOpenError(QuorumError):
    """
    Quorum circuit breaker is open due to repeated failures.
    
    Too many recent quorum operations have failed. The circuit breaker
    has opened to prevent cascading failures. Operations should fail
    fast with this error until the circuit closes.
    """
    
    def __init__(
        self,
        recent_failures: int,
        window_seconds: float,
        retry_after_seconds: float,
    ):
        super().__init__(
            message=f"Quorum circuit breaker OPEN: {recent_failures} failures in {window_seconds}s window. Retry after {retry_after_seconds:.1f}s",
            severity=ErrorSeverity.DEGRADED,
            recent_failures=recent_failures,
            window_seconds=window_seconds,
            retry_after_seconds=retry_after_seconds,
        )
