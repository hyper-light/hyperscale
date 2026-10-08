"""``QuorumTimeoutError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .quorum_error import QuorumError


class QuorumTimeoutError(QuorumError):
    """
    Quorum confirmation timed out.
    
    Managers are available but didn't respond in time. This could be
    due to network issues or high load.
    """
    
    def __init__(
        self,
        confirmations_received: int,
        required_quorum: int,
        timeout: float,
    ):
        super().__init__(
            message=f"Quorum timeout: got {confirmations_received} confirmations, need {required_quorum} (timeout={timeout}s)",
            severity=ErrorSeverity.TRANSIENT,
            confirmations_received=confirmations_received,
            required_quorum=required_quorum,
            timeout=timeout,
        )
