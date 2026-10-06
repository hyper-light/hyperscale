"""``RetryDecision`` -- pickled under the namespace
``hyperscale.distributed.swim.retry`` (see that module)."""

from enum import Enum, auto


class RetryDecision(Enum):
    """Decision for whether to retry an operation."""
    RETRY = auto()       # Retry after delay
    ABORT = auto()       # Don't retry, give up
    IMMEDIATE = auto()   # Retry immediately (no delay)
