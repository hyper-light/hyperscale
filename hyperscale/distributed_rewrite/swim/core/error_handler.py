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
from typing import Callable, Awaitable, Any
from collections import deque
from enum import Enum, auto
import asyncio
import time
import traceback
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


from .protocols import LoggerProtocol


@dataclass(slots=True)
class ErrorStats:
    """
    Track error rates for circuit breaker decisions.
    
    Uses a sliding window to calculate recent error rate and
    determine if the circuit should open.
    
    Memory safety:
    - Timestamps deque is bounded to prevent unbounded growth
    - Prunes old entries on each operation
    """
    
    window_seconds: float = 60.0
    """Time window for error rate calculation."""
    
    max_errors: int = 10
    """Circuit opens after this many errors in window."""
    
    half_open_after: float = 30.0
    """Seconds to wait before attempting recovery."""
    
    max_timestamps: int = 1000
    """Maximum timestamps to store (prevents memory growth under sustained errors)."""
    
    _timestamps: deque[float] = field(default_factory=deque)
    _circuit_state: CircuitState = CircuitState.CLOSED
    _circuit_opened_at: float | None = None
    
    def __post_init__(self):
        """Initialize bounded deque."""
        # Create bounded deque if not already bounded
        if not hasattr(self._timestamps, 'maxlen') or self._timestamps.maxlen != self.max_timestamps:
            self._timestamps = deque(self._timestamps, maxlen=self.max_timestamps)
    
    def record_error(self) -> None:
        """Record an error occurrence."""
        now = time.monotonic()
        self._timestamps.append(now)  # Deque maxlen handles overflow automatically
        self._prune_old_entries(now)
        
        # Check if we should open the circuit
        if self._circuit_state == CircuitState.CLOSED:
            if len(self._timestamps) >= self.max_errors:
                self._circuit_state = CircuitState.OPEN
                self._circuit_opened_at = now
    
    def record_success(self) -> None:
        """
        Record a successful operation.

        In HALF_OPEN state: Closes the circuit and clears error history.
        In OPEN state: No effect (must wait for half_open_after timeout first).
        In CLOSED state: Prunes old timestamps, helping prevent false opens.

        IMPORTANT: When closing from HALF_OPEN, we clear the timestamps deque.
        Without this, the circuit would immediately re-open on the next error
        because old errors would still be counted in the window.
        """
        if self._circuit_state == CircuitState.HALF_OPEN:
            self._circuit_state = CircuitState.CLOSED
            self._circuit_opened_at = None
            # CRITICAL: Clear error history to allow real recovery
            # Without this, circuit immediately re-opens on next error
            self._timestamps.clear()
        elif self._circuit_state == CircuitState.CLOSED:
            # Prune old entries to keep window current
            self._prune_old_entries(time.monotonic())
    
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


@dataclass(slots=True)
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

    # Shutdown flag - when True, suppress non-fatal errors
    _shutting_down: bool = False

    # Track last error per category for debugging (includes traceback)
    _last_errors: dict[ErrorCategory, tuple[SwimError, str]] = field(default_factory=dict)

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

    def start_shutdown(self) -> None:
        """Signal that shutdown is in progress - suppress non-fatal errors."""
        self._shutting_down = True

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
        # During shutdown, only handle fatal errors - suppress routine probe failures etc.
        if self._shutting_down and error.severity != ErrorSeverity.FATAL:
            return

        # Capture traceback for debugging - get the last line of the traceback
        tb_line = ""
        if error.cause:
            tb_lines = traceback.format_exception(type(error.cause), error.cause, error.cause.__traceback__)
            if tb_lines:
                # Get the last non-empty line (usually the actual error)
                tb_line = "".join(tb_lines[-3:]).strip() if len(tb_lines) >= 3 else "".join(tb_lines).strip()

        # Store last error with traceback for circuit breaker logging
        self._last_errors[error.category] = (error, tb_line)

        # 1. Log with structured context
        await self._log_error(error)

        # 2. Update error stats - but only for non-TRANSIENT errors
        # TRANSIENT errors (like stale messages) are expected in async distributed
        # systems and should NOT trip the circuit breaker. They indicate normal
        # protocol operation (e.g., incarnation changes during refutation).
        stats = self._get_stats(error.category)
        if error.severity != ErrorSeverity.TRANSIENT:
            stats.record_error()

        # 3. Affect LHM based on error
        await self._update_lhm(error)

        # 4. Check circuit breaker and trigger recovery
        if stats.is_circuit_open:
            await self._log_circuit_open(error.category, stats)
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
        System-level exceptions (KeyboardInterrupt, SystemExit, GeneratorExit)
        are re-raised immediately without processing.
        """
        # System-level exceptions must be re-raised immediately
        # These signal process termination and should never be suppressed
        if isinstance(exception, (KeyboardInterrupt, SystemExit, GeneratorExit)):
            raise exception

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
        elif isinstance(exception, OSError):
            # OSError is the base class for many network errors:
            # ConnectionRefusedError, BrokenPipeError, etc.
            # Treat as TRANSIENT since network conditions can change
            await self.handle(
                NetworkError(
                    f"OS/socket error during {operation}: {exception}",
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
            # Snapshot to avoid dict mutation during iteration
            for cat, stats in list(self._stats.items())
        }
    
    def reset_category(self, category: ErrorCategory) -> None:
        """Reset error stats for a category."""
        self._get_stats(category).reset()
    
    def reset_all(self) -> None:
        """Reset all error stats."""
        # Snapshot to avoid dict mutation during iteration
        for stats in list(self._stats.values()):
            stats.reset()
    
    def _get_stats(self, category: ErrorCategory) -> ErrorStats:
        """Get or create stats for a category."""
        if category not in self._stats:
            settings = self.circuit_settings.get(category, {})
            self._stats[category] = ErrorStats(**settings)
        return self._stats[category]
    
    async def _log_internal(self, message: str) -> None:
        """Log an internal error handler issue using ServerDebug."""
        if self.logger:
            try:
                from hyperscale.logging.hyperscale_logging_models import ServerDebug
                await self.logger.log(ServerDebug(
                    message=f"[ErrorHandler] {message}",
                    node_id=self.node_id,
                    node_host="",  # Not available at handler level
                    node_port=0,
                ))
            except Exception:
                pass  # Best effort - don't fail on logging errors
    
    async def _log_error(self, error: SwimError) -> None:
        """Log error with structured context, using appropriate level based on severity."""
        if self.logger:
            try:
                # Build structured message with error details
                message = (
                    f"[{error.__class__.__name__}] {error} "
                    f"(category={error.category.name}, severity={error.severity.name}"
                )
                if error.context:
                    message += f", context={error.context}"
                message += ")"
                
                # Select log model based on severity
                # TRANSIENT = expected/normal, DEGRADED = warning, FATAL = error
                from hyperscale.logging.hyperscale_logging_models import (
                    ServerDebug, ServerWarning, ServerError, ServerFatal
                )
                
                log_kwargs = {
                    "message": message,
                    "node_id": self.node_id,
                    "node_host": "",  # Not available at handler level
                    "node_port": 0,
                }
                
                if error.severity == ErrorSeverity.TRANSIENT:
                    log_model = ServerDebug(**log_kwargs)
                elif error.severity == ErrorSeverity.DEGRADED:
                    log_model = ServerWarning(**log_kwargs)
                else:  # FATAL
                    log_model = ServerError(**log_kwargs)
                
                await self.logger.log(log_model)
            except (ImportError, AttributeError, TypeError):
                # Fallback to simple logging - if this also fails, silently ignore
                # since logging errors shouldn't crash the application
                try:
                    await self.logger.log(str(error))
                except Exception:
                    pass  # Logging is best-effort
    
    async def _log_circuit_open(self, category: ErrorCategory, stats: ErrorStats) -> None:
        """Log circuit breaker opening with last error details."""
        message = (
            f"[CircuitBreakerOpen] Circuit breaker OPEN for {category.name}: "
            f"{stats.error_count} errors, rate={stats.error_rate:.2f}/s"
        )

        # Include last error details if available
        last_error_info = self._last_errors.get(category)
        if last_error_info:
            error, tb_line = last_error_info
            message += f" | Last error: {error}"
            if tb_line:
                message += f" | Traceback: {tb_line}"

        if self.logger:
            try:
                from hyperscale.logging.hyperscale_logging_models import ServerError
                await self.logger.log(
                    ServerError(
                        message=message,
                        node_id=self.node_id,
                        node_host="",  # Not available at handler level
                        node_port=0,
                    )
                )
            except (ImportError, AttributeError, TypeError):
                # Fallback to simple logging - if this also fails, silently ignore
                try:
                    await self.logger.log(message)
                except Exception:
                    pass  # Logging is best-effort
    
    async def _update_lhm(self, error: SwimError) -> None:
        """
        Update Local Health Multiplier based on error.

        IMPORTANT: This is intentionally conservative to avoid double-counting.
        Most LHM updates happen via direct calls to increase_failure_detector()
        at the point of the event (e.g., probe timeout, refutation needed).

        The error handler only updates LHM for:
        - FATAL errors (always serious)
        - RESOURCE errors (indicate local node is struggling)

        We explicitly DO NOT update LHM here for:
        - NETWORK errors: Already handled by direct calls in probe logic
        - PROTOCOL errors: Usually indicate remote issues, not local health
        - ELECTION errors: Handled by election logic directly
        - TRANSIENT errors: Expected behavior, not health issues
        """
        if not self.increment_lhm:
            return

        # Only update LHM for errors that clearly indicate LOCAL node issues
        event_type: str | None = None

        if error.severity == ErrorSeverity.FATAL:
            # Fatal errors always affect health significantly
            event_type = 'event_loop_critical'

        elif error.category == ErrorCategory.RESOURCE:
            # Resource exhaustion is a clear signal of local problems
            event_type = 'event_loop_lag'

        # Note: We intentionally skip NETWORK, PROTOCOL, ELECTION, and TRANSIENT
        # errors here. They are either:
        # 1. Already handled by direct increase_failure_detector() calls
        # 2. Indicate remote node issues rather than local health problems

        if event_type:
            try:
                await self.increment_lhm(event_type)
            except Exception as e:
                # Log but don't let LHM updates cause more errors
                await self._log_internal(f"LHM update failed for {event_type}: {type(e).__name__}: {e}")
    
    async def _trigger_recovery(self, category: ErrorCategory) -> None:
        """Trigger recovery action for a category."""
        if category in self._recovery_actions:
            try:
                await self._recovery_actions[category]()
            except Exception as e:
                # Log recovery failure but don't propagate
                await self._log_internal(f"Recovery action failed for {category.name}: {type(e).__name__}: {e}")
    
    async def _handle_fatal(self, error: SwimError) -> None:
        """Handle fatal error - escalate to callback or raise."""
        if self._fatal_callback:
            try:
                await self._fatal_callback(error)
            except Exception as e:
                # Log fatal callback failure - this is serious
                await self._log_internal(f"FATAL: Fatal callback failed: {type(e).__name__}: {e} (original error: {error})")
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
            # System-level exceptions must NEVER be suppressed
            if isinstance(exc_val, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                return False  # Always propagate

            # CancelledError is not an error - it's a normal signal for task cancellation
            # Log at debug level for visibility but don't treat as error or update metrics
            if isinstance(exc_val, asyncio.CancelledError):
                await self.handler._log_internal(
                    f"Operation '{self.operation}' cancelled (normal during shutdown)"
                )
                return False  # Don't suppress, let it propagate

            await self.handler.handle_exception(exc_val, self.operation)
            return not self.reraise  # Suppress exception unless reraise=True
        return False

    def record_success(self, category: ErrorCategory) -> None:
        """Record successful operation for circuit breaker."""
        self.handler.record_success(category)

