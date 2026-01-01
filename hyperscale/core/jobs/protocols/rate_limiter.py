"""
Rate limiting for protocol message handling.

Provides per-source rate limiting using a token bucket algorithm
to prevent DoS attacks and resource exhaustion.
"""

import time
from collections import OrderedDict
from typing import Dict, Tuple, Optional


# Default configuration
DEFAULT_REQUESTS_PER_SECOND = 1000  # Max requests per second per source
DEFAULT_BURST_SIZE = 100  # Maximum burst allowed
DEFAULT_MAX_SOURCES = 10000  # Maximum number of sources to track


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    pass


class TokenBucket:
    """
    Token bucket rate limiter for a single source.
    
    Tokens are added at a fixed rate up to a maximum (burst size).
    Each request consumes one token. If no tokens available, request is rejected.
    """
    
    __slots__ = ('tokens', 'last_update', 'rate', 'burst')
    
    def __init__(self, rate: float, burst: int) -> None:
        self.tokens = float(burst)  # Start with full bucket
        self.last_update = time.monotonic()
        self.rate = rate
        self.burst = burst
    
    def consume(self, now: float) -> bool:
        """
        Try to consume a token.
        
        Returns True if token was available, False if rate limited.
        """
        # Add tokens based on elapsed time
        elapsed = now - self.last_update
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_update = now
        
        # Try to consume
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class RateLimiter:
    """
    Per-source rate limiter using token buckets.
    
    Tracks rate limits for multiple sources (identified by address).
    Uses LRU eviction to bound memory usage.
    """
    
    __slots__ = (
        '_buckets',
        '_rate',
        '_burst',
        '_max_sources',
        '_stats_allowed',
        '_stats_rejected',
        '_stats_evicted',
    )
    
    def __init__(
        self,
        requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND,
        burst_size: int = DEFAULT_BURST_SIZE,
        max_sources: int = DEFAULT_MAX_SOURCES,
    ) -> None:
        """
        Initialize rate limiter.
        
        Args:
            requests_per_second: Rate at which tokens are replenished
            burst_size: Maximum tokens (allows bursts up to this size)
            max_sources: Maximum number of sources to track (LRU eviction)
        """
        self._buckets: OrderedDict[Tuple[str, int], TokenBucket] = OrderedDict()
        self._rate = requests_per_second
        self._burst = burst_size
        self._max_sources = max_sources
        
        # Statistics
        self._stats_allowed = 0
        self._stats_rejected = 0
        self._stats_evicted = 0
    
    def check(self, addr: Tuple[str, int], raise_on_limit: bool = False) -> bool:
        """
        Check if request from address is allowed.
        
        Args:
            addr: Source address tuple (host, port)
            raise_on_limit: If True, raise RateLimitExceeded instead of returning False
            
        Returns:
            True if request is allowed, False if rate limited
            
        Raises:
            RateLimitExceeded: If raise_on_limit is True and rate is exceeded
        """
        now = time.monotonic()
        
        # Get or create bucket for this source
        bucket = self._buckets.get(addr)
        if bucket is None:
            bucket = TokenBucket(self._rate, self._burst)
            self._buckets[addr] = bucket
            
            # Evict oldest if over limit
            while len(self._buckets) > self._max_sources:
                self._buckets.popitem(last=False)
                self._stats_evicted += 1
        else:
            # Move to end (most recently used)
            self._buckets.move_to_end(addr)
        
        # Check rate limit
        if bucket.consume(now):
            self._stats_allowed += 1
            return True
        else:
            self._stats_rejected += 1
            if raise_on_limit:
                raise RateLimitExceeded(f"Rate limit exceeded for {addr[0]}:{addr[1]}")
            return False
    
    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        return {
            'allowed': self._stats_allowed,
            'rejected': self._stats_rejected,
            'evicted_sources': self._stats_evicted,
            'tracked_sources': len(self._buckets),
            'rate_per_second': self._rate,
            'burst_size': self._burst,
        }
    
    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._stats_allowed = 0
        self._stats_rejected = 0
        self._stats_evicted = 0
    
    def clear(self) -> None:
        """Clear all tracked sources."""
        self._buckets.clear()
        self.reset_stats()
    
    def remove_source(self, addr: Tuple[str, int]) -> None:
        """Remove a specific source from tracking."""
        self._buckets.pop(addr, None)
    
    def __len__(self) -> int:
        """Return number of tracked sources."""
        return len(self._buckets)
    
    def __getstate__(self):
        """Support pickling for multiprocessing."""
        return {
            'rate': self._rate,
            'burst': self._burst,
            'max_sources': self._max_sources,
        }
    
    def __setstate__(self, state):
        """Restore from pickle."""
        self._rate = state['rate']
        self._burst = state['burst']
        self._max_sources = state['max_sources']
        self._buckets = OrderedDict()
        self._stats_allowed = 0
        self._stats_rejected = 0
        self._stats_evicted = 0

