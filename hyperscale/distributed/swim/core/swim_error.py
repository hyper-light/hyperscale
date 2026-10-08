"""``SwimError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
import traceback

from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .swim_error_record import SwimErrorRecord

if TYPE_CHECKING:
    from .network_error import NetworkError


@dataclass
class SwimError(Exception):
    """
    Base exception for SWIM protocol errors.
    
    All SWIM errors carry:
    - message: Human-readable description
    - category: What kind of error
    - severity: How serious
    - context: Additional debugging info
    - cause: Original exception if wrapping
    
    Example:
        raise NetworkError(
            "Probe timeout",
            target=("10.0.0.5", 8671),
            timeout=2.0,
        )
    """
    
    message: str
    category: ErrorCategory
    severity: ErrorSeverity
    context: dict[str, object] = field(default_factory=dict)
    cause: BaseException | None = None
    
    def __post_init__(self):
        # Capture stack trace at creation time
        self._traceback = traceback.format_stack()[:-1]
    
    def __str__(self) -> str:
        ctx = f" {self.context}" if self.context else ""
        cause = self._cause_suffix()
        return f"[{self.category.name}/{self.severity.name}] {self.message}{ctx}{cause}"
    
    def _cause_suffix(self) -> str:
        """The " (caused by ...)" suffix of ``__str__``, or "" when there is no cause."""
        if not self.cause:
            return ""
        # Include type name for better debugging, especially when str(cause) is empty
        cause_str = str(self.cause)
        cause_type = type(self.cause).__name__
        if cause_str:
            return f" (caused by {cause_type}: {cause_str})"
        return f" (caused by {cause_type})"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"category={self.category}, "
            f"severity={self.severity}, "
            f"context={self.context})"
        )
    
    def with_context(self, **kwargs: object) -> 'SwimError':
        """Add additional context to the error."""
        self.context.update(kwargs)
        return self
    
    def get_traceback(self) -> str:
        """Get the stack trace from when this error was created."""
        return ''.join(self._traceback)
    
    def to_dict(self) -> SwimErrorRecord:
        """Convert to dictionary for structured logging."""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'category': self.category.name,
            'severity': self.severity.name,
            'context': self.context,
            'cause': str(self.cause) if self.cause else None,
        }
