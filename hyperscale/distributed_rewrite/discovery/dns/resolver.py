"""
Async DNS resolver with caching for peer discovery.

Provides DNS-based service discovery with positive and negative caching,
supporting both A and SRV records.
"""

import asyncio
import socket
import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed_rewrite.discovery.dns.negative_cache import NegativeCache


class DNSError(Exception):
    """Raised when DNS resolution fails."""

    def __init__(self, hostname: str, message: str):
        self.hostname = hostname
        super().__init__(f"DNS resolution failed for '{hostname}': {message}")


@dataclass(slots=True)
class DNSResult:
    """Result of a DNS lookup."""

    hostname: str
    """The hostname that was resolved."""

    addresses: list[str]
    """Resolved IP addresses."""

    port: int | None = None
    """Port from SRV record (if applicable)."""

    ttl_seconds: float = 60.0
    """Time-to-live for this result."""

    resolved_at: float = field(default_factory=time.monotonic)
    """Timestamp when this result was resolved."""

    @property
    def is_expired(self) -> bool:
        """Check if this result has expired."""
        return time.monotonic() - self.resolved_at > self.ttl_seconds


@dataclass
class AsyncDNSResolver:
    """
    Async DNS resolver with positive and negative caching.

    Features:
    - Async resolution using getaddrinfo
    - Positive caching with configurable TTL
    - Negative caching with exponential backoff
    - Concurrent resolution limits
    - Support for SRV record patterns (hostname:port)

    Usage:
        resolver = AsyncDNSResolver()
        result = await resolver.resolve("manager.hyperscale.local")
        for addr in result.addresses:
            print(f"Found: {addr}")
    """

    default_ttl_seconds: float = 60.0
    """Default TTL for positive cache entries."""

    max_concurrent_resolutions: int = 10
    """Maximum concurrent DNS resolutions."""

    resolution_timeout_seconds: float = 5.0
    """Timeout for individual DNS resolution."""

    negative_cache: NegativeCache = field(default_factory=NegativeCache)
    """Cache for failed resolutions."""

    _positive_cache: dict[str, DNSResult] = field(default_factory=dict)
    """Cache for successful resolutions."""

    _resolution_semaphore: asyncio.Semaphore | None = field(default=None, repr=False)
    """Semaphore to limit concurrent resolutions."""

    _pending_resolutions: dict[str, asyncio.Future[DNSResult]] = field(
        default_factory=dict, repr=False
    )
    """Map of hostname to pending resolution future (deduplication)."""

    _on_resolution: Callable[[DNSResult], None] | None = field(default=None, repr=False)
    """Optional callback when resolution completes."""

    _on_error: Callable[[str, str], None] | None = field(default=None, repr=False)
    """Optional callback when resolution fails (hostname, error)."""

    def __post_init__(self) -> None:
        """Initialize the semaphore."""
        self._resolution_semaphore = asyncio.Semaphore(self.max_concurrent_resolutions)

    async def resolve(
        self,
        hostname: str,
        port: int | None = None,
        force_refresh: bool = False,
    ) -> DNSResult:
        """
        Resolve a hostname to IP addresses.

        Args:
            hostname: The hostname to resolve
            port: Optional port (for SRV-style lookups)
            force_refresh: If True, bypass cache and force fresh lookup

        Returns:
            DNSResult with resolved addresses

        Raises:
            DNSError: If resolution fails and hostname is not in positive cache
        """
        cache_key = f"{hostname}:{port}" if port else hostname

        # Check positive cache first (unless force refresh)
        if not force_refresh:
            cached = self._positive_cache.get(cache_key)
            if cached is not None and not cached.is_expired:
                return cached

        # Check negative cache
        negative_entry = self.negative_cache.get(hostname)
        if negative_entry is not None and not force_refresh:
            raise DNSError(hostname, f"Cached failure: {negative_entry.error_message}")

        # Check for pending resolution (deduplication)
        pending = self._pending_resolutions.get(cache_key)
        if pending is not None:
            return await pending

        # Start new resolution
        loop = asyncio.get_running_loop()
        future: asyncio.Future[DNSResult] = loop.create_future()
        self._pending_resolutions[cache_key] = future

        try:
            result = await self._do_resolve(hostname, port)

            # Cache successful result
            self._positive_cache[cache_key] = result

            # Clear any negative cache entry on success
            self.negative_cache.remove(hostname)

            # Notify callback
            if self._on_resolution is not None:
                self._on_resolution(result)

            future.set_result(result)
            return result

        except Exception as exc:
            error_message = str(exc)

            # Add to negative cache
            self.negative_cache.put(hostname, error_message)

            # Notify error callback
            if self._on_error is not None:
                self._on_error(hostname, error_message)

            # Check if we have a stale cached result we can return
            stale = self._positive_cache.get(cache_key)
            if stale is not None:
                # Return stale result with warning
                future.set_result(stale)
                return stale

            dns_error = DNSError(hostname, error_message)
            future.set_exception(dns_error)
            raise dns_error from exc

        finally:
            self._pending_resolutions.pop(cache_key, None)

    async def _do_resolve(self, hostname: str, port: int | None) -> DNSResult:
        """
        Perform actual DNS resolution.

        Args:
            hostname: The hostname to resolve
            port: Optional port for the lookup

        Returns:
            DNSResult with resolved addresses
        """
        if self._resolution_semaphore is None:
            self._resolution_semaphore = asyncio.Semaphore(
                self.max_concurrent_resolutions
            )

        async with self._resolution_semaphore:
            try:
                # Use asyncio's getaddrinfo for async resolution
                results = await asyncio.wait_for(
                    asyncio.get_running_loop().getaddrinfo(
                        hostname,
                        port or 0,
                        family=socket.AF_UNSPEC,  # Both IPv4 and IPv6
                        type=socket.SOCK_STREAM,
                    ),
                    timeout=self.resolution_timeout_seconds,
                )

                if not results:
                    raise DNSError(hostname, "No addresses returned")

                # Extract unique addresses
                addresses: list[str] = []
                seen: set[str] = set()

                for family, type_, proto, canonname, sockaddr in results:
                    # sockaddr is (host, port) for IPv4, (host, port, flow, scope) for IPv6
                    addr = sockaddr[0]
                    if addr not in seen:
                        seen.add(addr)
                        addresses.append(addr)

                return DNSResult(
                    hostname=hostname,
                    addresses=addresses,
                    port=port,
                    ttl_seconds=self.default_ttl_seconds,
                )

            except asyncio.TimeoutError:
                raise DNSError(
                    hostname, f"Resolution timeout ({self.resolution_timeout_seconds}s)"
                )
            except socket.gaierror as exc:
                raise DNSError(hostname, f"getaddrinfo failed: {exc}")

    async def resolve_many(
        self,
        hostnames: list[str],
        port: int | None = None,
    ) -> dict[str, DNSResult | DNSError]:
        """
        Resolve multiple hostnames concurrently.

        Args:
            hostnames: List of hostnames to resolve
            port: Optional port for all lookups

        Returns:
            Dict mapping hostname to DNSResult or DNSError
        """
        results: dict[str, DNSResult | DNSError] = {}

        async def resolve_one(host: str) -> None:
            try:
                results[host] = await self.resolve(host, port)
            except DNSError as exc:
                results[host] = exc

        await asyncio.gather(*[resolve_one(h) for h in hostnames])
        return results

    def get_cached(self, hostname: str, port: int | None = None) -> DNSResult | None:
        """
        Get a cached result without triggering resolution.

        Args:
            hostname: The hostname to look up
            port: Optional port

        Returns:
            Cached DNSResult if available and not expired, None otherwise
        """
        cache_key = f"{hostname}:{port}" if port else hostname
        cached = self._positive_cache.get(cache_key)
        if cached is not None and not cached.is_expired:
            return cached
        return None

    def invalidate(self, hostname: str, port: int | None = None) -> bool:
        """
        Invalidate a cached entry.

        Args:
            hostname: The hostname to invalidate
            port: Optional port

        Returns:
            True if an entry was invalidated
        """
        cache_key = f"{hostname}:{port}" if port else hostname
        if cache_key in self._positive_cache:
            del self._positive_cache[cache_key]
            return True
        return False

    def clear_cache(self) -> tuple[int, int]:
        """
        Clear all cached entries (positive and negative).

        Returns:
            Tuple of (positive entries cleared, negative entries cleared)
        """
        positive_count = len(self._positive_cache)
        negative_count = self.negative_cache.clear()
        self._positive_cache.clear()
        return (positive_count, negative_count)

    def cleanup_expired(self) -> tuple[int, int]:
        """
        Remove expired entries from both caches.

        Returns:
            Tuple of (positive entries removed, negative entries removed)
        """
        now = time.monotonic()

        # Cleanup positive cache
        positive_expired = [
            key
            for key, result in self._positive_cache.items()
            if now - result.resolved_at > result.ttl_seconds
        ]
        for key in positive_expired:
            del self._positive_cache[key]

        # Cleanup negative cache
        negative_removed = self.negative_cache.cleanup_expired()

        return (len(positive_expired), negative_removed)

    @property
    def cache_stats(self) -> dict[str, int]:
        """Get cache statistics."""
        return {
            "positive_entries": len(self._positive_cache),
            "negative_entries": self.negative_cache.size,
            "pending_resolutions": len(self._pending_resolutions),
        }

    def set_callbacks(
        self,
        on_resolution: Callable[[DNSResult], None] | None = None,
        on_error: Callable[[str, str], None] | None = None,
    ) -> None:
        """
        Set optional callbacks for resolution events.

        Args:
            on_resolution: Called when resolution succeeds
            on_error: Called when resolution fails (hostname, error_message)
        """
        self._on_resolution = on_resolution
        self._on_error = on_error
