"""
Centralized Error Handler for SWIM Protocol

Provides:
- Circuit breaker pattern for cascading failure prevention
- Error rate tracking with sliding window
- LHM integration for health-aware error handling
- Recovery action registration
- Structured logging integration
"""

from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Protocol
from collections import deque
from enum import Enum, auto
import asyncio
import time
from hyperscale.logging.hyperscale_logging_models import ServerError

from .errors import (
    SwimError,
    ErrorCategory,
    ErrorSeverity,
    NetworkError,
    ProtocolError,
    ResourceError,
    ElectionError,
    InternalError,
    UnexpectedError,
)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()      # Normal operation
    OPEN = auto()        # Failing, rejecting requests
    HALF_OPEN = auto()   # Testing if recovery succeeded


class LoggerProtocol(Protocol):
    """Protocol for structured logging."""
    
    def log(self, message: Any) -> None:
        """Log a message."""
        ...


@dataclass
class ErrorStats:
    """
    Track error rates for circuit breaker decisions.
    
    Uses a sliding window to calculate recent error rate and
    determine if the circuit should open.
    """
    
    window_seconds: float = 60.0
    """Time window for error rate calculation."""
    
    max_errors: int = 10
    """Circuit opens after this many errors in window."""
    
    half_open_after: float = 30.0
    """Seconds to wait before attempting recovery."""
    
    _timestamps: deque[float] = field(default_factory=deque)
    _circuit_state: CircuitState = CircuitState.CLOSED
    _circuit_opened_at: float | None = None
    
    def record_error(self) -> None:
        """Record an error occurrence."""
        now = time.monotonic()
        self._timestamps.append(now)
        self._prune_old_entries(now)
        
        # Check if we should open the circuit
        if self._circuit_state == CircuitState.CLOSED:
            if len(self._timestamps) >= self.max_errors:
                self._circuit_state = CircuitState.OPEN
                self._circuit_opened_at = now
    
    def record_success(self) -> None:
        """Record a successful operation (for half-open state)."""
        if self._circuit_state == CircuitState.HALF_OPEN:
            self._circuit_state = CircuitState.CLOSED
            self._circuit_opened_at = None
    
    def _prune_old_entries(self, now: float) -> None:
        """Remove entries outside the window."""
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
    
    @property
    def error_count(self) -> int:
        """Number of errors in current window."""
        self._prune_old_entries(time.monotonic())
        return len(self._timestamps)
    
    @property
    def error_rate(self) -> float:
        """Errors per second in the window."""
        count = self.error_count
        if count == 0:
            return 0.0
        return count / self.window_seconds
    
    @property
    def circuit_state(self) -> CircuitState:
        """Get current circuit state, transitioning to half-open if appropriate."""
        if self._circuit_state == CircuitState.OPEN and self._circuit_opened_at:
            elapsed = time.monotonic() - self._circuit_opened_at
            if elapsed >= self.half_open_after:
                self._circuit_state = CircuitState.HALF_OPEN
        return self._circuit_state
    
    @property
    def is_circuit_open(self) -> bool:
        """Check if circuit is open (rejecting requests)."""
        return self.circuit_state == CircuitState.OPEN
    
    def reset(self) -> None:
        """Reset error stats and close circuit."""
        self._timestamps.clear()
        self._circuit_state = CircuitState.CLOSED
        self._circuit_opened_at = None


@dataclass
class ErrorHandler:
    """
    Centralized error handling with recovery actions.
    
    Features:
    - Categorized error tracking with circuit breakers per category
    - LHM integration (errors affect local health score)
    - Recovery action registration for automatic healing
    - Structured logging with context
    
    Example:
        handler = ErrorHandler(
            logger=server._udp_logger,
            increment_lhm=server.increase_failure_detector,
            node_id=server.node_id.short,
        )
        
        # Register recovery actions
        handler.register_recovery(
            ErrorCategory.NETWORK,
            self._reset_connections,
        )
        
        # Handle errors
        try:
            await probe_node(target)
        except asyncio.TimeoutError as e:
            await handler.handle(
                ProbeTimeoutError(target, timeout)
            )
    """
    
    logger: LoggerProtocol | None = None
    """Logger for structured error logging."""
    
    increment_lhm: Callable[[str], Awaitable[None]] | None = None
    """Callback to increment Local Health Multiplier."""
    
    node_id: str = "unknown"
    """Node identifier for log context."""
    
    # Circuit breaker settings per category
    circuit_settings: dict[ErrorCategory, dict[str, Any]] = field(
        default_factory=lambda: {
            ErrorCategory.NETWORK: {'max_errors': 15, 'window_seconds': 60.0},
            ErrorCategory.PROTOCOL: {'max_errors': 10, 'window_seconds': 60.0},
            ErrorCategory.RESOURCE: {'max_errors': 5, 'window_seconds': 30.0},
            ErrorCategory.ELECTION: {'max_errors': 5, 'window_seconds': 30.0},
            ErrorCategory.INTERNAL: {'max_errors': 3, 'window_seconds': 60.0},
        }
    )
    
    # Track errors by category
    _stats: dict[ErrorCategory, ErrorStats] = field(default_factory=dict)
    
    # Recovery actions by category
    _recovery_actions: dict[ErrorCategory, Callable[[], Awaitable[None]]] = field(
        default_factory=dict
    )
    
    # Callbacks for fatal errors
    _fatal_callback: Callable[[SwimError], Awaitable[None]] | None = None
    
    def __post_init__(self):
        # Initialize stats for each category
        for category, settings in self.circuit_settings.items():
            self._stats[category] = ErrorStats(**settings)
    
    def register_recovery(
        self,
        category: ErrorCategory,
        action: Callable[[], Awaitable[None]],
    ) -> None:
        """
        Register a recovery action for a category.
        
        The action is called when the circuit breaker opens for that category.
        """
        self._recovery_actions[category] = action
    
    def set_fatal_callback(
        self,
        callback: Callable[[SwimError], Awaitable[None]],
    ) -> None:
        """Set callback for fatal errors (e.g., graceful shutdown)."""
        self._fatal_callback = callback
    
    async def handle(self, error: SwimError) -> None:
        """
        Handle an error with appropriate response.
        
        Steps:
        1. Log with structured context
        2. Update error stats for circuit breaker
        3. Affect LHM based on error type/severity
        4. Trigger recovery if circuit opens
        5. Escalate fatal errors
        """
        # 1. Log with structured context
        self._log_error(error)
        
        # 2. Update error stats
        stats = self._get_stats(error.category)
        stats.record_error()
        
        # 3. Affect LHM based on error
        await self._update_lhm(error)
        
        # 4. Check circuit breaker and trigger recovery
        if stats.is_circuit_open:
            self._log_circuit_open(error.category, stats)
            await self._trigger_recovery(error.category)
        
        # 5. Fatal errors need escalation
        if error.severity == ErrorSeverity.FATAL:
            await self._handle_fatal(error)
    
    async def handle_exception(
        self,
        exception: BaseException,
        operation: str = "unknown",
    ) -> None:
        """
        Wrap and handle a raw exception.
        
        Converts standard exceptions to SwimError types.
        """
        # Convert known exceptions to SwimError types
        if isinstance(exception, SwimError):
            await self.handle(exception)
        elif isinstance(exception, asyncio.TimeoutError):
            await self.handle(
                NetworkError(
                    f"Timeout during {operation}",
                    cause=exception,
                    operation=operation,
                )
            )
        elif isinstance(exception, ConnectionError):
            await self.handle(
                NetworkError(
                    f"Connection error during {operation}",
                    cause=exception,
                    operation=operation,
                )
            )
        elif isinstance(exception, ValueError):
            await self.handle(
                ProtocolError(
                    f"Value error during {operation}: {exception}",
                    cause=exception,
                    operation=operation,
                )
            )
        elif isinstance(exception, MemoryError):
            await self.handle(
                ResourceError(
                    f"Memory error during {operation}",
                    severity=ErrorSeverity.FATAL,
                    cause=exception,
                )
            )
        else:
            await self.handle(
                UnexpectedError(exception, operation)
            )
    
    def record_success(self, category: ErrorCategory) -> None:
        """Record a successful operation (helps circuit breaker recover)."""
        stats = self._get_stats(category)
        stats.record_success()
    
    def is_circuit_open(self, category: ErrorCategory) -> bool:
        """Check if circuit is open for a category."""
        return self._get_stats(category).is_circuit_open
    
    def get_circuit_state(self, category: ErrorCategory) -> CircuitState:
        """Get circuit state for a category."""
        return self._get_stats(category).circuit_state
    
    def get_error_rate(self, category: ErrorCategory) -> float:
        """Get current error rate for a category."""
        return self._get_stats(category).error_rate
    
    def get_stats_summary(self) -> dict[str, dict[str, Any]]:
        """Get summary of all error stats for debugging."""
        return {
            cat.name: {
                'error_count': stats.error_count,
                'error_rate': stats.error_rate,
                'circuit_state': stats.circuit_state.name,
            }
            for cat, stats in self._stats.items()
        }
    
    def reset_category(self, category: ErrorCategory) -> None:
        """Reset error stats for a category."""
        self._get_stats(category).reset()
    
    def reset_all(self) -> None:
        """Reset all error stats."""
        for stats in self._stats.values():
            stats.reset()
    
    def _get_stats(self, category: ErrorCategory) -> ErrorStats:
        """Get or create stats for a category."""
        if category not in self._stats:
            settings = self.circuit_settings.get(category, {})
            self._stats[category] = ErrorStats(**settings)
        return self._stats[category]
    
    def _log_error(self, error: SwimError) -> None:
        """Log error with structured context."""
        if self.logger:
            try:
                # Try to use structured logging
                self.logger.log(
                    ServerError(
                        message=str(error),
                        node_id=self.node_id,
                        error_type=error.__class__.__name__,
                        error_category=error.category.name,
                        error_severity=error.severity.name,
                        error_context=error.context,
                    )
                )
            except (ImportError, AttributeError):
                # Fallback to simple logging
                try:
                    self.logger.log(str(error))
                except Exception:
                    pass
    
    def _log_circuit_open(self, category: ErrorCategory, stats: ErrorStats) -> None:
        """Log circuit breaker opening."""
        message = (
            f"Circuit breaker OPEN for {category.name}: "
            f"{stats.error_count} errors, rate={stats.error_rate:.2f}/s"
        )
        if self.logger:
            try:
                from hyperscale.logging.hyperscale_logging_models import ServerError
                self.logger.log(
                    ServerError(
                        message=message,
                        node_id=self.node_id,
                        error_type="CircuitBreakerOpen",
                        error_category=category.name,
                    )
                )
            except (ImportError, AttributeError):
                try:
                    self.logger.log(message)
                except Exception:
                    pass
    
    async def _update_lhm(self, error: SwimError) -> None:
        """Update Local Health Multiplier based on error."""
        if not self.increment_lhm:
            return
        
        # Map error types to LHM event types
        event_type: str | None = None
        
        if error.category == ErrorCategory.NETWORK:
            if error.severity == ErrorSeverity.TRANSIENT:
                event_type = 'probe_timeout'
            else:
                event_type = 'network_error'
        
        elif error.category == ErrorCategory.RESOURCE:
            event_type = 'resource_pressure'
        
        elif error.category == ErrorCategory.ELECTION:
            if 'split_brain' in error.message.lower():
                event_type = 'refutation'
        
        elif error.severity == ErrorSeverity.FATAL:
            event_type = 'fatal_error'
        
        if event_type:
            try:
                await self.increment_lhm(event_type)
            except Exception:
                pass  # Don't let LHM updates cause more errors
    
    async def _trigger_recovery(self, category: ErrorCategory) -> None:
        """Trigger recovery action for a category."""
        if category in self._recovery_actions:
            try:
                await self._recovery_actions[category]()
            except Exception as e:
                # Log recovery failure but don't propagate
                if self.logger:
                    try:
                        self.logger.log(
                            f"Recovery action failed for {category.name}: {e}"
                        )
                    except Exception:
                        pass
    
    async def _handle_fatal(self, error: SwimError) -> None:
        """Handle fatal error - escalate to callback or raise."""
        if self._fatal_callback:
            try:
                await self._fatal_callback(error)
            except Exception:
                pass
        else:
            # Re-raise fatal errors if no handler
            raise error


# =============================================================================
# Context manager for error handling
# =============================================================================

class ErrorContext:
    """
    Async context manager for consistent error handling.
    
    Example:
        async with ErrorContext(handler, "probe_round") as ctx:
            await probe_node(target)
            ctx.record_success(ErrorCategory.NETWORK)
    """
    
    def __init__(
        self,
        handler: ErrorHandler,
        operation: str,
        reraise: bool = False,
    ):
        self.handler = handler
        self.operation = operation
        self.reraise = reraise
    
    async def __aenter__(self) -> 'ErrorContext':
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_val is not None:
            await self.handler.handle_exception(exc_val, self.operation)
            return not self.reraise  # Suppress exception unless reraise=True
        return False
    
    def record_success(self, category: ErrorCategory) -> None:
        """Record successful operation for circuit breaker."""
        self.handler.record_success(category)

