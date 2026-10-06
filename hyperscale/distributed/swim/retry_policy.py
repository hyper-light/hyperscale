"""``RetryPolicy`` -- pickled under the namespace
``hyperscale.distributed.swim.retry`` (see that module)."""

import asyncio
from dataclasses import dataclass, field
from hyperscale.distributed.runtime import Random
from hyperscale.distributed.swim.core import SwimError, ErrorCategory, ErrorSeverity

from .retry_shared import _DEFAULT_RANDOM
from .retry_decision import RetryDecision


@dataclass(slots=True)
class RetryPolicy:
    """
    Configuration for retry behavior.
    
    Example:
        # Aggressive retry for probes
        probe_policy = RetryPolicy(
            max_attempts=3,
            base_delay=0.1,
            max_delay=2.0,
            jitter=0.2,
        )
        
        # Conservative retry for elections
        election_policy = RetryPolicy(
            max_attempts=2,
            base_delay=1.0,
            max_delay=5.0,
            budget_seconds=10.0,
        )
    """
    
    max_attempts: int = 3
    """Maximum number of attempts (including first try)."""
    
    base_delay: float = 0.1
    """Initial delay in seconds."""
    
    max_delay: float = 5.0
    """Maximum delay in seconds (caps exponential growth)."""
    
    exponential_base: float = 2.0
    """Base for exponential backoff (delay = base_delay * base^attempt)."""
    
    jitter: float = 0.1
    """Jitter factor (0-1). Delay varies by ±jitter*delay."""
    
    budget_seconds: float | None = None
    """Total time budget for all retries. None = unlimited."""
    
    retryable_categories: set[ErrorCategory] = field(
        default_factory=lambda: {
            ErrorCategory.NETWORK,
            ErrorCategory.RESOURCE,
        }
    )
    """Error categories that should be retried."""
    
    retryable_severities: set[ErrorSeverity] = field(
        default_factory=lambda: {
            ErrorSeverity.TRANSIENT,
            ErrorSeverity.DEGRADED,
        }
    )
    """Error severities that should be retried."""
    
    def should_retry(self, error: SwimError | Exception) -> RetryDecision:
        """Determine if an error should trigger a retry."""
        if isinstance(error, SwimError):
            if error.category not in self.retryable_categories:
                return RetryDecision.ABORT
            if error.severity not in self.retryable_severities:
                return RetryDecision.ABORT
            return RetryDecision.RETRY
        
        # Standard exceptions
        if isinstance(error, (asyncio.TimeoutError, ConnectionError, OSError)):
            return RetryDecision.RETRY
        if isinstance(error, (ValueError, TypeError, AttributeError)):
            return RetryDecision.ABORT  # Likely a bug, don't retry
        
        return RetryDecision.RETRY  # Default to retry for unknown
    
    def get_delay(
        self,
        attempt: int,
        *,
        random_source: Random | None = None,
    ) -> float:
        """
        Calculate delay for a given attempt number.

        Uses exponential backoff with jitter.

        ``random_source`` defaults to the module-level ``RealRandom``
        so existing callers see identical behavior; Phase 6 SIM mode
        threads a ``SeededRandom`` through to make backoff jitter
        deterministic.
        """
        rng = random_source if random_source is not None else _DEFAULT_RANDOM

        # Exponential backoff
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay,
        )

        # Add jitter to prevent thundering herd
        if self.jitter > 0:
            jitter_range = delay * self.jitter
            delay += rng.uniform(-jitter_range, jitter_range)

        return max(0, delay)
