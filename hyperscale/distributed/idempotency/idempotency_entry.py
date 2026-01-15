from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Generic, TypeVar

from .idempotency_key import IdempotencyKey
from .idempotency_status import IdempotencyStatus

T = TypeVar("T")


@dataclass(slots=True)
class IdempotencyEntry(Generic[T]):
    """Tracks the state and outcome of an idempotent request."""

    idempotency_key: IdempotencyKey
    status: IdempotencyStatus
    job_id: str | None
    result: T | None
    created_at: float
    committed_at: float | None
    source_gate_id: str | None

    def is_terminal(self) -> bool:
        """Check if entry is in a terminal state."""
        return self.status in (IdempotencyStatus.COMMITTED, IdempotencyStatus.REJECTED)

    def age_seconds(self) -> float:
        """Get age of entry in seconds."""
        return time.time() - self.created_at
