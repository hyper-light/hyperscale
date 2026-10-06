"""``QuorumUnavailableError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .quorum_error import QuorumError


class QuorumUnavailableError(QuorumError):
    """
    Quorum cannot be achieved due to insufficient active managers.
    
    This is a structural issue - there simply aren't enough managers
    available to form a quorum. Operations requiring quorum should
    fail fast with this error.
    """
    
    def __init__(
        self,
        active_managers: int,
        required_quorum: int,
        reason: str = "insufficient active managers",
    ):
        super().__init__(
            message=f"Quorum unavailable: {reason} ({active_managers} active, need {required_quorum})",
            severity=ErrorSeverity.DEGRADED,
            active_managers=active_managers,
            required_quorum=required_quorum,
            reason=reason,
        )
