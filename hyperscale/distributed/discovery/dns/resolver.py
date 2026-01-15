"""
Async DNS resolver with caching for peer discovery.

Provides DNS-based service discovery with positive and negative caching,
supporting both A and SRV records. Includes security validation against
DNS cache poisoning, hijacking, and spoofing attacks.
"""

import asyncio
import socket
import time
from dataclasses import dataclass, field
from typing import Callable

import aiodns

from hyperscale.distributed.discovery.dns.negative_cache import NegativeCache
from hyperscale.distributed.discovery.dns.security import (
    DNSSecurityValidator,
    DNSSecurityEvent,
    DNSSecurityViolation,
)


class DNSError(Exception):
    """Raised when DNS resolution fails."""

    def __init__(self, hostname: str, message: str):
        self.hostname = hostname
        super().__init__(f"DNS resolution failed for '{hostname}': {message}")


@dataclass(slots=True)
class SRVRecord:
    """Represents a DNS SRV record."""

    priority: int
    """Priority of the target host (lower values are preferred)."""

    weight: int
    """Weight for hosts with the same priority (for load balancing)."""

    port: int
    """Port number of the service."""

    target: str
    """Target hostname."""


@dataclass(slots=True)
class DNSResult:
    """Result of a DNS lookup."""

    hostname: str
    """The hostname that was resolved."""

    addresses: list[str]
    """Resolved IP addresses."""

    port: int | None = None
    """Port from SRV record (if applicable)."""

    srv_records: list[SRVRecord] = field(default_factory=list)
    """SRV records if this was an SRV query."""

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
    - Async resolution using getaddrinfo for A/AAAA records
    - Real DNS SRV record resolution using aiodns
    - Positive caching with configurable TTL
    - Negative caching with exponential backoff
    - Concurrent resolution limits
    - Support for SRV record patterns (_service._proto.domain)

    Usage:
        resolver = AsyncDNSResolver()

        # A/AAAA record resolution
        result = await resolver.resolve("manager.hyperscale.local")
        for addr in result.addresses:
            print(f"Found: {addr}")

        # SRV record resolution
        result = await resolver.resolve("_hyperscale-manager._tcp.cluster.local")
        for srv in result.srv_records:
            print(f"Found: {srv.target}:{srv.port} (priority={srv.priority})")
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

    _on_security_event: Callable[[DNSSecurityEvent], None] | None = field(default=None, repr=False)
    """Optional callback when security violation is detected."""

    security_validator: DNSSecurityValidator | None = field(default=None)
    """Optional security validator for IP range and anomaly checking.

    When set, resolved IPs are validated against allowed CIDR ranges
    and checked for suspicious patterns (rapid changes, rebinding).
    IPs that fail validation are filtered from results.
    """

    reject_on_security_violation: bool = True
    """If True, reject IPs that fail security validation.

    If False, violations are logged but IPs are still returned.
    """

    _aiodns_resolver: aiodns.DNSResolver | None = field(default=None, repr=False)
    """Internal aiodns resolver for SRV queries."""

    def __post_init__(self) -> None:
        """Initialize internal state. Async components are lazily created when first needed."""
        # Note: Both asyncio.Semaphore and aiodns.DNSResolver may require a
        # running event loop. They are lazily initialized in their respective
        # async methods (_do_resolve, _do_resolve_srv, resolve_srv) instead.
        pass

    @staticmethod
    def _is_srv_pattern(hostname: str) -> bool:
        """
        Check if a hostname follows the SRV record pattern.

        SRV patterns start with '_' and contain either '._tcp.' or '._udp.'
        Examples:
            - _hyperscale-manager._tcp.cluster.local
            - _http._tcp.example.com
            - _service._udp.domain.local

        Args:
            hostname: The hostname to check

        Returns:
            True if hostname matches SRV pattern
        """
        return hostname.startswith("_") and ("._tcp." in hostname or "._udp." in hostname)

    async def resolve_srv(self, service_name: str) -> list[SRVRecord]:
        """
        Resolve a DNS SRV record.

        SRV records provide service discovery by returning a list of
        (priority, weight, port, target) tuples. This allows clients
        to discover multiple instances of a service and choose based
        on priority and weight.

        Args:
            service_name: The SRV record name to query
                         Format: _service._proto.domain
                         Example: _hyperscale-manager._tcp.cluster.local

        Returns:
            List of SRVRecord objects, sorted by priority (ascending) then weight (descending)

        Raises:
            DNSError: If SRV query fails or returns no records
        """
        if self._aiodns_resolver is None:
            self._aiodns_resolver = aiodns.DNSResolver()

        try:
            # Query SRV records using aiodns
            srv_results = await asyncio.wait_for(
                self._aiodns_resolver.query(service_name, "SRV"),
                timeout=self.resolution_timeout_seconds,
            )

            if not srv_results:
                raise DNSError(service_name, "No SRV records returned")

            # Convert to our SRVRecord dataclass
            records: list[SRVRecord] = []
            for srv in srv_results:
                # aiodns returns objects with priority, weight, port, host attributes
                record = SRVRecord(
                    priority=srv.priority,
                    weight=srv.weight,
                    port=srv.port,
                    target=srv.host.rstrip("."),  # Remove trailing dot from FQDN
                )
                records.append(record)

            # Sort by priority (ascending), then weight (descending)
            # Lower priority values are preferred
            # Higher weight values are preferred for same priority
            records.sort(key=lambda r: (r.priority, -r.weight))

            return records

        except asyncio.TimeoutError:
            raise DNSError(
                service_name,
                f"SRV resolution timeout ({self.resolution_timeout_seconds}s)",
            )
        except aiodns.error.DNSError as exc:
            raise DNSError(service_name, f"SRV query failed: {exc}")
        except Exception as exc:
            raise DNSError(service_name, f"Unexpected error during SRV query: {exc}")

    async def resolve(
        self,
        hostname: str,
        port: int | None = None,
        force_refresh: bool = False,
    ) -> DNSResult:
        """
        Resolve a hostname to IP addresses.

        Supports both standard A/AAAA records and SRV records.
        SRV patterns are detected automatically (starting with '_' and containing '._tcp.' or '._udp.').

        Args:
            hostname: The hostname or SRV pattern to resolve
                     A/AAAA: "manager.hyperscale.local"
                     SRV: "_hyperscale-manager._tcp.cluster.local"
            port: Optional port (ignored for SRV lookups which provide their own ports)
            force_refresh: If True, bypass cache and force fresh lookup

        Returns:
            DNSResult with resolved addresses and optional SRV records

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
            # Detect SRV pattern and route accordingly
            if self._is_srv_pattern(hostname):
                result = await self._do_resolve_srv(hostname)
            else:
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

                # Apply security validation if configured
                if self.security_validator and self.security_validator.is_enabled:
                    validated_addresses = self._validate_addresses(hostname, addresses)
                    if not validated_addresses and self.reject_on_security_violation:
                        raise DNSError(
                            hostname,
                            f"All resolved IPs failed security validation: {addresses}"
                        )
                    addresses = validated_addresses if validated_addresses else addresses

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

    async def _do_resolve_srv(self, service_name: str) -> DNSResult:
        """
        Perform SRV record resolution and resolve target hostnames to IPs.

        This method:
        1. Queries SRV records for the service name
        2. Resolves each SRV target hostname to IP addresses
        3. Returns a DNSResult with all addresses and SRV records

        Args:
            service_name: The SRV service name to resolve

        Returns:
            DNSResult with addresses from all SRV targets and the SRV records
        """
        if self._resolution_semaphore is None:
            self._resolution_semaphore = asyncio.Semaphore(
                self.max_concurrent_resolutions
            )

        async with self._resolution_semaphore:
            # First, get the SRV records
            srv_records = await self.resolve_srv(service_name)

            if not srv_records:
                raise DNSError(service_name, "No SRV records found")

            # Now resolve each target to IP addresses
            all_addresses: list[str] = []
            seen_addresses: set[str] = set()

            for srv_record in srv_records:
                try:
                    # Resolve the target hostname to IPs
                    # Note: We resolve recursively but avoid adding to cache under service_name
                    target_result = await self._do_resolve(srv_record.target, srv_record.port)

                    # Collect unique addresses
                    for addr in target_result.addresses:
                        if addr not in seen_addresses:
                            seen_addresses.add(addr)
                            all_addresses.append(addr)

                except DNSError:
                    # If one target fails, continue with others
                    # This provides resilience if some targets are down
                    continue

            if not all_addresses:
                raise DNSError(
                    service_name,
                    "All SRV target hostnames failed to resolve to IP addresses"
                )

            # Apply security validation if configured
            if self.security_validator and self.security_validator.is_enabled:
                validated_addresses = self._validate_addresses(service_name, all_addresses)
                if not validated_addresses and self.reject_on_security_violation:
                    raise DNSError(
                        service_name,
                        f"All resolved IPs failed security validation: {all_addresses}"
                    )
                all_addresses = validated_addresses if validated_addresses else all_addresses

            # Return result with both addresses and SRV records
            # The port from the first (highest priority) SRV record is used
            return DNSResult(
                hostname=service_name,
                addresses=all_addresses,
                port=srv_records[0].port if srv_records else None,
                srv_records=srv_records,
                ttl_seconds=self.default_ttl_seconds,
            )

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
        on_security_event: Callable[[DNSSecurityEvent], None] | None = None,
    ) -> None:
        """
        Set optional callbacks for resolution events.

        Args:
            on_resolution: Called when resolution succeeds
            on_error: Called when resolution fails (hostname, error_message)
            on_security_event: Called when security violation detected
        """
        self._on_resolution = on_resolution
        self._on_error = on_error
        self._on_security_event = on_security_event

    def _validate_addresses(
        self,
        hostname: str,
        addresses: list[str],
    ) -> list[str]:
        """
        Validate resolved addresses against security policy.

        Args:
            hostname: The hostname being resolved
            addresses: List of resolved IP addresses

        Returns:
            List of addresses that pass validation
        """
        if not self.security_validator:
            return addresses

        valid_addresses: list[str] = []

        for addr in addresses:
            event = self.security_validator.validate(hostname, addr)

            if event is None:
                # No violation, address is valid
                valid_addresses.append(addr)
            else:
                # Security violation detected
                if self._on_security_event:
                    self._on_security_event(event)

                # Only block on certain violation types
                # IP changes are informational, not blocking
                if event.violation_type in (
                    DNSSecurityViolation.IP_OUT_OF_RANGE,
                    DNSSecurityViolation.PRIVATE_IP_FOR_PUBLIC_HOST,
                    DNSSecurityViolation.RAPID_IP_ROTATION,
                ):
                    # Skip this address
                    continue
                else:
                    # Allow informational violations through
                    valid_addresses.append(addr)

        return valid_addresses

    def get_security_events(
        self,
        limit: int = 100,
        violation_type: DNSSecurityViolation | None = None,
    ) -> list[DNSSecurityEvent]:
        """
        Get recent DNS security events.

        Args:
            limit: Maximum events to return
            violation_type: Filter by type (None = all)

        Returns:
            List of security events
        """
        if not self.security_validator:
            return []
        return self.security_validator.get_recent_events(limit, violation_type)

    @property
    def security_stats(self) -> dict[str, int]:
        """Get security validation statistics."""
        if not self.security_validator:
            return {"enabled": False}
        return {"enabled": True, **self.security_validator.stats}
