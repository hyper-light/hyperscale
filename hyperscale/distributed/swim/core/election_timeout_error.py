"""``ElectionTimeoutError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .election_error import ElectionError
from .error_severity import ErrorSeverity


class ElectionTimeoutError(ElectionError):
    """Election did not complete within timeout."""
    
    def __init__(self, term: int, votes_received: int, votes_needed: int):
        super().__init__(
            message=f"Election timeout in term {term}: got {votes_received}/{votes_needed} votes",
            severity=ErrorSeverity.TRANSIENT,
            term=term,
            votes_received=votes_received,
            votes_needed=votes_needed,
        )
