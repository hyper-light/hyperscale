"""``SplitBrainError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .election_error import ElectionError
from .error_severity import ErrorSeverity


class SplitBrainError(ElectionError):
    """Multiple leaders detected — possibly in different terms.

    The detection path compares two heartbeats: the local node's
    current term and the remote claimant's term. ``other_term`` is
    optional so callers that only know a single term (legacy
    detector paths, tests) keep working with positional args.
    """

    def __init__(
        self,
        self_addr: tuple[str, int],
        other_leader: tuple[str, int],
        term: int,
        other_term: int | None = None,
    ):
        if other_term is not None and other_term != term:
            message = (
                f"Split brain detected: {self_addr} (term {term}) and "
                f"{other_leader} (term {other_term}) both claim leadership"
            )
        else:
            message = (
                f"Split brain detected: both {self_addr} and "
                f"{other_leader} are leaders in term {term}"
            )
        super().__init__(
            message=message,
            severity=ErrorSeverity.DEGRADED,
            self_addr=self_addr,
            other_leader=other_leader,
            term=term,
            other_term=other_term,
        )
