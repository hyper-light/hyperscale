from enum import Enum, auto


class IdempotencyStatus(Enum):
    """Status of an idempotency entry."""

    PENDING = auto()
    COMMITTED = auto()
    REJECTED = auto()
