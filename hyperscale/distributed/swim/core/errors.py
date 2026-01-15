"""
SWIM Protocol Error Hierarchy

Categorized exceptions for robust error handling in distributed
failure detection. Errors are classified by:
- Category: What kind of error (network, protocol, resource, internal)
- Severity: How serious (transient, degraded, fatal)

This enables appropriate recovery actions and LHM adjustments.
"""

from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Any
import traceback


class ErrorSeverity(Enum):
    """How serious is this error?"""
    
    TRANSIENT = auto()
    """Network blip, retry likely to succeed. No LHM impact."""
    
    DEGRADED = auto()
    """Partial failure, can continue with reduced functionality. Minor LHM impact."""
    
    FATAL = auto()
    """Cannot continue, must restart/escalate. Major LHM impact."""


class ErrorCategory(Enum):
    """What kind of error is this?"""
    
    NETWORK = auto()
    """Timeouts, connection refused, DNS failures, unreachable hosts."""
    
    PROTOCOL = auto()
    """Malformed messages, unexpected state, version mismatch."""
    
    RESOURCE = auto()
    """Memory pressure, file descriptors, CPU saturation."""
    
    INTERNAL = auto()
    """Bugs, assertion failures, unexpected exceptions."""
    
    ELECTION = auto()
    """Leader election specific errors."""


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
    context: dict[str, Any] = field(default_factory=dict)
    cause: BaseException | None = None
    
    def __post_init__(self):
        # Capture stack trace at creation time
        self._traceback = traceback.format_stack()[:-1]
    
    def __str__(self) -> str:
        ctx = f" {self.context}" if self.context else ""
        cause = ""
        if self.cause:
            # Include type name for better debugging, especially when str(cause) is empty
            cause_str = str(self.cause)
            cause_type = type(self.cause).__name__
            if cause_str:
                cause = f" (caused by {cause_type}: {cause_str})"
            else:
                cause = f" (caused by {cause_type})"
        return f"[{self.category.name}/{self.severity.name}] {self.message}{ctx}{cause}"
    
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"category={self.category}, "
            f"severity={self.severity}, "
            f"context={self.context})"
        )
    
    def with_context(self, **kwargs: Any) -> 'SwimError':
        """Add additional context to the error."""
        self.context.update(kwargs)
        return self
    
    def get_traceback(self) -> str:
        """Get the stack trace from when this error was created."""
        return ''.join(self._traceback)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for structured logging."""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'category': self.category.name,
            'severity': self.severity.name,
            'context': self.context,
            'cause': str(self.cause) if self.cause else None,
        }


# =============================================================================
# Network Errors - Transient by default, retry likely to help
# =============================================================================

class NetworkError(SwimError):
    """
    Network-related failures.
    
    These are typically transient and should trigger retry with backoff.
    Examples: timeouts, connection refused, DNS failures.
    """
    
    def __init__(
        self, 
        message: str, 
        severity: ErrorSeverity = ErrorSeverity.TRANSIENT,
        cause: BaseException | None = None,
        **context: Any,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.NETWORK,
            severity=severity,
            context=context,
            cause=cause,
        )


class ProbeTimeoutError(NetworkError):
    """Direct probe to a node timed out."""
    
    def __init__(
        self,
        target: tuple[str, int],
        timeout: float,
        cause: BaseException | None = None,
    ):
        super().__init__(
            message=f"Probe to {target[0]}:{target[1]} timed out after {timeout:.2f}s",
            target=target,
            timeout=timeout,
            cause=cause,
        )


class IndirectProbeTimeoutError(NetworkError):
    """Indirect probe via proxy nodes all timed out."""
    
    def __init__(
        self,
        target: tuple[str, int],
        proxies: list[tuple[str, int]],
        timeout: float,
    ):
        super().__init__(
            message=f"Indirect probe to {target[0]}:{target[1]} via {len(proxies)} proxies timed out",
            severity=ErrorSeverity.DEGRADED,  # More serious than direct timeout
            target=target,
            proxies=proxies,
            timeout=timeout,
        )


class ConnectionRefusedError(NetworkError):
    """Target node refused connection."""
    
    def __init__(self, target: tuple[str, int], cause: BaseException | None = None):
        super().__init__(
            message=f"Connection refused by {target[0]}:{target[1]}",
            target=target,
            cause=cause,
        )


# =============================================================================
# Protocol Errors - Message/state issues, may indicate version mismatch
# =============================================================================

class ProtocolError(SwimError):
    """
    Protocol violations or unexpected messages.
    
    These may indicate:
    - Malformed messages
    - Version mismatch between nodes
    - Unexpected state transitions
    """
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.DEGRADED,
        cause: BaseException | None = None,
        **context: Any,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.PROTOCOL,
            severity=severity,
            context=context,
            cause=cause,
        )


class MalformedMessageError(ProtocolError):
    """Received message could not be parsed."""
    
    def __init__(
        self,
        raw_data: bytes,
        reason: str,
        source: tuple[str, int] | None = None,
        cause: BaseException | None = None,
    ):
        # Truncate raw data for logging
        preview = raw_data[:100].hex() if len(raw_data) > 100 else raw_data.hex()
        super().__init__(
            message=f"Malformed message: {reason}",
            raw_preview=preview,
            raw_length=len(raw_data),
            source=source,
            cause=cause,
        )


class UnexpectedMessageError(ProtocolError):
    """Received message type not expected in current state."""
    
    def __init__(
        self,
        msg_type: bytes,
        expected: list[bytes] | None = None,
        source: tuple[str, int] | None = None,
    ):
        super().__init__(
            message=f"Unexpected message type: {msg_type!r}",
            severity=ErrorSeverity.TRANSIENT,  # Might just be timing
            msg_type=msg_type,
            expected=expected,
            source=source,
        )


class StaleMessageError(ProtocolError):
    """Message has old incarnation number."""
    
    def __init__(
        self,
        node: tuple[str, int],
        received_incarnation: int,
        current_incarnation: int,
    ):
        super().__init__(
            message=f"Stale message from {node[0]}:{node[1]}: incarnation {received_incarnation} < {current_incarnation}",
            severity=ErrorSeverity.TRANSIENT,  # Normal in async systems
            node=node,
            received_incarnation=received_incarnation,
            current_incarnation=current_incarnation,
        )


# =============================================================================
# Resource Errors - System resource issues
# =============================================================================

class ResourceError(SwimError):
    """
    Resource exhaustion errors.
    
    These indicate the node is under stress and may need to:
    - Shed load
    - Increase LHM score
    - Step down from leadership
    """
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.DEGRADED,
        cause: BaseException | None = None,
        **context: Any,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.RESOURCE,
            severity=severity,
            context=context,
            cause=cause,
        )


class QueueFullError(ResourceError):
    """Message queue is full, cannot accept more work."""
    
    def __init__(self, queue_name: str, queue_size: int):
        super().__init__(
            message=f"Queue '{queue_name}' is full ({queue_size} items)",
            queue_name=queue_name,
            queue_size=queue_size,
        )


class TaskOverloadError(ResourceError):
    """Too many concurrent tasks running."""
    
    def __init__(self, task_count: int, max_tasks: int):
        super().__init__(
            message=f"Task overload: {task_count}/{max_tasks} tasks",
            task_count=task_count,
            max_tasks=max_tasks,
        )


# =============================================================================
# Election Errors - Leader election specific
# =============================================================================

class ElectionError(SwimError):
    """
    Leader election errors.
    
    These are generally handled gracefully by the election protocol
    but should be tracked for debugging.
    """
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.DEGRADED,
        cause: BaseException | None = None,
        **context: Any,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.ELECTION,
            severity=severity,
            context=context,
            cause=cause,
        )


class SplitBrainError(ElectionError):
    """Multiple leaders detected in the same term."""
    
    def __init__(
        self,
        self_addr: tuple[str, int],
        other_leader: tuple[str, int],
        term: int,
    ):
        super().__init__(
            message=f"Split brain detected: both {self_addr} and {other_leader} are leaders in term {term}",
            severity=ErrorSeverity.DEGRADED,
            self_addr=self_addr,
            other_leader=other_leader,
            term=term,
        )


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


# =============================================================================
# Quorum Errors - Distributed consensus failures
# =============================================================================

class QuorumError(SwimError):
    """
    Base class for quorum-related errors.
    
    These errors occur when distributed consensus cannot be achieved,
    such as when too many managers are down or unreachable.
    """
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.DEGRADED,
        cause: BaseException | None = None,
        **context: Any,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.PROTOCOL,
            severity=severity,
            context=context,
            cause=cause,
        )


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


class QuorumCircuitOpenError(QuorumError):
    """
    Quorum circuit breaker is open due to repeated failures.
    
    Too many recent quorum operations have failed. The circuit breaker
    has opened to prevent cascading failures. Operations should fail
    fast with this error until the circuit closes.
    """
    
    def __init__(
        self,
        recent_failures: int,
        window_seconds: float,
        retry_after_seconds: float,
    ):
        super().__init__(
            message=f"Quorum circuit breaker OPEN: {recent_failures} failures in {window_seconds}s window. Retry after {retry_after_seconds:.1f}s",
            severity=ErrorSeverity.DEGRADED,
            recent_failures=recent_failures,
            window_seconds=window_seconds,
            retry_after_seconds=retry_after_seconds,
        )


# =============================================================================
# Internal Errors - Bugs and unexpected conditions
# =============================================================================

class InternalError(SwimError):
    """
    Internal errors indicating bugs or unexpected conditions.
    
    These should be investigated and fixed. They may indicate:
    - Logic errors
    - Assertion failures
    - Unexpected exceptions from dependencies
    """
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.DEGRADED,
        cause: BaseException | None = None,
        **context: Any,
    ):
        super().__init__(
            message=message,
            category=ErrorCategory.INTERNAL,
            severity=severity,
            context=context,
            cause=cause,
        )


class AssertionError(InternalError):
    """An internal assertion failed."""
    
    def __init__(self, condition: str, **context: Any):
        super().__init__(
            message=f"Assertion failed: {condition}",
            severity=ErrorSeverity.FATAL,
            condition=condition,
            **context,
        )


class UnexpectedError(InternalError):
    """An unexpected exception occurred."""
    
    def __init__(self, cause: BaseException, operation: str = "unknown"):
        super().__init__(
            message=f"Unexpected error during {operation}: {cause}",
            severity=ErrorSeverity.DEGRADED,
            cause=cause,
            operation=operation,
        )

