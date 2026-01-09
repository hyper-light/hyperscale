"""
Negative cache for DNS resolution failures.

Prevents repeated lookups for known-failed hostnames.
"""

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class NegativeEntry:
    """A cached negative result for DNS lookup."""

    hostname: str
    """The hostname that failed resolution."""

    error_message: str
    """Description of the failure."""

    cached_at: float
    """Timestamp when this entry was cached."""

    failure_count: int = 1
    """Number of consecutive failures for this hostname."""


@dataclass
class NegativeCache:
    """
    Cache for DNS resolution failures.

    Stores negative results to avoid hammering DNS servers for
    hostnames that are known to fail. Uses exponential backoff
    for retry timing based on failure count.

    Thread-safe for asyncio (single-threaded async access).
    """

    base_ttl_seconds: float = 30.0
    """Base TTL for negative entries (before backoff)."""

    max_ttl_seconds: float = 300.0
    """Maximum TTL after exponential backoff (5 minutes)."""

    max_failure_count: int = 10
    """Maximum tracked failure count (caps backoff)."""

    _entries: dict[str, NegativeEntry] = field(default_factory=dict)
    """Map of hostname to negative entry."""

    def get(self, hostname: str) -> NegativeEntry | None:
        """
        Get a negative cache entry if it exists and hasn't expired.

        Args:
            hostname: The hostname to look up

        Returns:
            NegativeEntry if cached and not expired, None otherwise
        """
        entry = self._entries.get(hostname)
        if entry is None:
            return None

        ttl = self._compute_ttl(entry.failure_count)
        if time.monotonic() - entry.cached_at > ttl:
            # Entry expired, remove it
            del self._entries[hostname]
            return None

        return entry

    def is_cached(self, hostname: str) -> bool:
        """
        Check if a hostname has a valid negative cache entry.

        Args:
            hostname: The hostname to check

        Returns:
            True if hostname is negatively cached and not expired
        """
        return self.get(hostname) is not None

    def put(self, hostname: str, error_message: str) -> NegativeEntry:
        """
        Add or update a negative cache entry.

        If the hostname already has an entry, increments the failure
        count (extending the TTL via exponential backoff).

        Args:
            hostname: The hostname that failed resolution
            error_message: Description of the failure

        Returns:
            The created or updated NegativeEntry
        """
        existing = self._entries.get(hostname)
        if existing is not None:
            # Increment failure count (capped at max)
            failure_count = min(existing.failure_count + 1, self.max_failure_count)
        else:
            failure_count = 1

        entry = NegativeEntry(
            hostname=hostname,
            error_message=error_message,
            cached_at=time.monotonic(),
            failure_count=failure_count,
        )
        self._entries[hostname] = entry
        return entry

    def remove(self, hostname: str) -> bool:
        """
        Remove a negative cache entry.

        Call this when a hostname successfully resolves to clear
        the negative entry and reset the failure count.

        Args:
            hostname: The hostname to remove from cache

        Returns:
            True if an entry was removed, False if not found
        """
        if hostname in self._entries:
            del self._entries[hostname]
            return True
        return False

    def clear(self) -> int:
        """
        Clear all entries from the cache.

        Returns:
            Number of entries removed
        """
        count = len(self._entries)
        self._entries.clear()
        return count

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries from the cache.

        Call this periodically to free memory.

        Returns:
            Number of entries removed
        """
        now = time.monotonic()
        to_remove = []

        for hostname, entry in self._entries.items():
            ttl = self._compute_ttl(entry.failure_count)
            if now - entry.cached_at > ttl:
                to_remove.append(hostname)

        for hostname in to_remove:
            del self._entries[hostname]

        return len(to_remove)

    def _compute_ttl(self, failure_count: int) -> float:
        """
        Compute TTL with exponential backoff.

        TTL = base_ttl * 2^(failure_count - 1), capped at max_ttl.

        Args:
            failure_count: Number of consecutive failures

        Returns:
            TTL in seconds
        """
        # Exponential backoff: 30s, 60s, 120s, 240s, 300s (capped)
        ttl = self.base_ttl_seconds * (2 ** (failure_count - 1))
        return min(ttl, self.max_ttl_seconds)

    def get_remaining_ttl(self, hostname: str) -> float | None:
        """
        Get the remaining TTL for a cached entry.

        Args:
            hostname: The hostname to check

        Returns:
            Remaining TTL in seconds, or None if not cached
        """
        entry = self._entries.get(hostname)
        if entry is None:
            return None

        ttl = self._compute_ttl(entry.failure_count)
        elapsed = time.monotonic() - entry.cached_at
        remaining = ttl - elapsed

        if remaining <= 0:
            # Expired, remove it
            del self._entries[hostname]
            return None

        return remaining

    @property
    def size(self) -> int:
        """Return the number of entries in the cache."""
        return len(self._entries)
