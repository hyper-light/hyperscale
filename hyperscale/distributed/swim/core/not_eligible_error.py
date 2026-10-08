"""``NotEligibleError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .election_error import ElectionError
from .error_severity import ErrorSeverity


class NotEligibleError(ElectionError):
    """Node is not eligible to become leader."""
    
    def __init__(self, reason: str, lhm_score: int, max_lhm: int):
        super().__init__(
            message=f"Not eligible for leadership: {reason}",
            severity=ErrorSeverity.TRANSIENT,
            reason=reason,
            lhm_score=lhm_score,
            max_lhm=max_lhm,
        )
