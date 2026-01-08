"""
Reliability infrastructure for distributed operations.

This module provides cross-cutting reliability components:
- Retry with jitter (AD-21)
- Overload detection (AD-18)
- Load shedding (AD-22)
- Backpressure (AD-23)
- Rate limiting (AD-24)
"""

from hyperscale.distributed_rewrite.reliability.retry import (
    JitterStrategy as JitterStrategy,
    RetryConfig as RetryConfig,
    RetryExecutor as RetryExecutor,
)
from hyperscale.distributed_rewrite.reliability.overload import (
    OverloadState as OverloadState,
    OverloadConfig as OverloadConfig,
    HybridOverloadDetector as HybridOverloadDetector,
)
from hyperscale.distributed_rewrite.reliability.load_shedding import (
    LoadShedder as LoadShedder,
    LoadShedderConfig as LoadShedderConfig,
    RequestPriority as RequestPriority,
)
from hyperscale.distributed_rewrite.reliability.backpressure import (
    BackpressureLevel as BackpressureLevel,
    BackpressureSignal as BackpressureSignal,
    StatsBuffer as StatsBuffer,
    StatsBufferConfig as StatsBufferConfig,
    StatsEntry as StatsEntry,
)
from hyperscale.distributed_rewrite.reliability.robust_queue import (
    RobustMessageQueue as RobustMessageQueue,
    RobustQueueConfig as RobustQueueConfig,
    QueuePutResult as QueuePutResult,
    QueueState as QueueState,
    QueueMetrics as QueueMetrics,
    QueueFullError as QueueFullError,
)
from hyperscale.distributed_rewrite.reliability.rate_limiting import (
    # Core rate limiting
    SlidingWindowCounter as SlidingWindowCounter,
    AdaptiveRateLimitConfig as AdaptiveRateLimitConfig,
    AdaptiveRateLimiter as AdaptiveRateLimiter,
    ServerRateLimiter as ServerRateLimiter,
    RateLimitConfig as RateLimitConfig,
    RateLimitResult as RateLimitResult,
    # Legacy (kept for backward compatibility)
    TokenBucket as TokenBucket,
    CooperativeRateLimiter as CooperativeRateLimiter,
    # Retry-after helpers
    is_rate_limit_response as is_rate_limit_response,
    handle_rate_limit_response as handle_rate_limit_response,
    # Retry-after with automatic retry
    RateLimitRetryConfig as RateLimitRetryConfig,
    RateLimitRetryResult as RateLimitRetryResult,
    execute_with_rate_limit_retry as execute_with_rate_limit_retry,
)
