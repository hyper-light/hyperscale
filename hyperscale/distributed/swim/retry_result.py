"""``RetryResult`` -- pickled under the namespace
``hyperscale.distributed.swim.retry`` (see that module)."""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

RetryValueT = TypeVar("RetryValueT")


@dataclass(slots=True)
class RetryResult(Generic[RetryValueT]):
    """Result of a retry operation."""
    
    success: bool
    """Whether the operation eventually succeeded."""
    
    value: RetryValueT | None = None
    """Return value if successful."""
    
    attempts: int = 0
    """Number of attempts made."""
    
    total_time: float = 0.0
    """Total time spent including delays."""
    
    last_error: Exception | None = None
    """Last error encountered (if failed)."""
    
    errors: list[Exception] = field(default_factory=list)
    """All errors encountered."""
