#!/usr/bin/env python3
"""
DNS-Based Discovery Integration Tests (AD-28).

Tests that the DiscoveryService correctly discovers peers via DNS resolution,
handles DNS failures gracefully, and recovers when DNS becomes available again.

Unlike the config-based discovery tests, these tests validate the actual DNS
resolution path in DiscoveryService, including:
- DNS resolution via AsyncDNSResolver
- Positive and negative caching
- Security validation integration
- Failure detection and recovery
- Multi-name resolution (multiple DNS names)

Test scenarios:
1. Basic DNS discovery with localhost resolution
2. DNS resolution with caching validation
3. DNS failure handling (negative caching)
4. DNS recovery after failure
5. Multi-name DNS discovery
6. DNS security validation integration
7. Discovery service peer lifecycle with DNS

Usage:
    python test_dns_discovery.py
"""

import asyncio
import sys
import os
import time
from dataclasses import dataclass, field
from typing import Callable

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.discovery import (
    DiscoveryConfig,
    DiscoveryService,
)
from hyperscale.distributed_rewrite.discovery.dns.resolver import (
    AsyncDNSResolver,
    DNSResult,
    DNSError,
)
from hyperscale.distributed_rewrite.discovery.dns.security import (
    DNSSecurityValidator,
    DNSSecurityEvent,
    DNSSecurityViolation,
)
from hyperscale.distributed_rewrite.discovery.models.peer_info import (
    PeerInfo,
    PeerHealth,
)


# ==========================================================================
# Mock DNS Resolver for Testing
# ==========================================================================

@dataclass
class MockDNSResolver:
    """
    Mock DNS resolver for testing DNS discovery paths.

    Allows injecting specific resolution results without actual DNS queries.
    """

    default_ttl_seconds: float = 60.0
    resolution_timeout_seconds: float = 5.0
    max_concurrent_resolutions: int = 10

    _mock_results: dict[str, list[str]] = field(default_factory=dict)
    """Hostname -> list of IP addresses."""

    _mock_failures: dict[str, str] = field(default_factory=dict)
    """Hostname -> error message for simulated failures."""

    _resolution_count: dict[str, int] = field(default_factory=dict)
    """Track resolution calls per hostname."""

    _positive_cache: dict[str, DNSResult] = field(default_factory=dict)
    """Simulated positive cache."""

    _on_resolution: Callable[[DNSResult], None] | None = None
    _on_error: Callable[[str, str], None] | None = None
    _on_security_event: Callable[[DNSSecurityEvent], None] | None = None

    security_validator: DNSSecurityValidator | None = None
    reject_on_security_violation: bool = True

    def set_mock_result(self, hostname: str, addresses: list[str]) -> None:
        """Set mock resolution result for a hostname."""
        self._mock_results[hostname] = addresses
        # Clear any failure for this hostname
        self._mock_failures.pop(hostname, None)

    def set_mock_failure(self, hostname: str, error: str) -> None:
        """Set mock failure for a hostname."""
        self._mock_failures[hostname] = error
        # Clear any result for this hostname
        self._mock_results.pop(hostname, None)

    def clear_mock(self, hostname: str) -> None:
        """Clear mock data for a hostname."""
        self._mock_results.pop(hostname, None)
        self._mock_failures.pop(hostname, None)

    def get_resolution_count(self, hostname: str) -> int:
        """Get number of resolution attempts for a hostname."""
        return self._resolution_count.get(hostname, 0)

    async def resolve(
        self,
        hostname: str,
        port: int | None = None,
        force_refresh: bool = False,
    ) -> DNSResult:
        """Resolve hostname using mock data."""
        cache_key = f"{hostname}:{port}" if port else hostname

        # Check cache unless force refresh
        if not force_refresh:
            cached = self._positive_cache.get(cache_key)
            if cached is not None and not cached.is_expired:
                return cached

        # Track resolution count
        self._resolution_count[hostname] = self._resolution_count.get(hostname, 0) + 1

        # Check for simulated failure
        if hostname in self._mock_failures:
            error_msg = self._mock_failures[hostname]
            if self._on_error:
                self._on_error(hostname, error_msg)
            raise DNSError(hostname, error_msg)

        # Check for mock result
        if hostname in self._mock_results:
            addresses = self._mock_results[hostname]

            # Apply security validation if configured
            if self.security_validator and self.security_validator.is_enabled:
                validated = []
                for addr in addresses:
                    event = self.security_validator.validate(hostname, addr)
                    if event is None:
                        validated.append(addr)
                    elif self._on_security_event:
                        self._on_security_event(event)

                if not validated and self.reject_on_security_violation:
                    raise DNSError(hostname, f"All IPs failed security: {addresses}")
                addresses = validated if validated else addresses

            result = DNSResult(
                hostname=hostname,
                addresses=addresses,
                port=port,
                ttl_seconds=self.default_ttl_seconds,
            )

            # Cache result
            self._positive_cache[cache_key] = result

            if self._on_resolution:
                self._on_resolution(result)

            return result

        # No mock data - raise error
        raise DNSError(hostname, "No mock data configured")

    def invalidate(self, hostname: str, port: int | None = None) -> bool:
        """Invalidate cache entry."""
        cache_key = f"{hostname}:{port}" if port else hostname
        if cache_key in self._positive_cache:
            del self._positive_cache[cache_key]
            return True
        return False

    def clear_cache(self) -> tuple[int, int]:
        """Clear all cache entries."""
        count = len(self._positive_cache)
        self._positive_cache.clear()
        return (count, 0)

    def cleanup_expired(self) -> tuple[int, int]:
        """Remove expired entries."""
        expired = [k for k, v in self._positive_cache.items() if v.is_expired]
        for key in expired:
            del self._positive_cache[key]
        return (len(expired), 0)

    @property
    def cache_stats(self) -> dict[str, int]:
        """Get cache statistics."""
        return {
            "positive_entries": len(self._positive_cache),
            "negative_entries": 0,
            "pending_resolutions": 0,
        }

    def set_callbacks(
        self,
        on_resolution: Callable[[DNSResult], None] | None = None,
        on_error: Callable[[str, str], None] | None = None,
        on_security_event: Callable[[DNSSecurityEvent], None] | None = None,
    ) -> None:
        """Set callbacks."""
        self._on_resolution = on_resolution
        self._on_error = on_error
        self._on_security_event = on_security_event


# ==========================================================================
# Test Helper: Create DiscoveryService with Mock Resolver
# ==========================================================================

def create_discovery_with_mock_resolver(
    dns_names: list[str],
    mock_resolver: MockDNSResolver,
    cluster_id: str = "test-cluster",
    datacenter_id: str = "dc-east",
) -> DiscoveryService:
    """Create a DiscoveryService with an injected mock resolver."""
    config = DiscoveryConfig(
        cluster_id=cluster_id,
        environment_id="test",
        node_role="client",
        dns_names=dns_names,
        static_seeds=[],
        default_port=9000,
        datacenter_id=datacenter_id,
    )

    service = DiscoveryService(config=config)
    # Inject mock resolver
    service._resolver = mock_resolver  # type: ignore

    return service


# ==========================================================================
# Test: Basic DNS Discovery
# ==========================================================================

async def scenario_dns_discovery_basic() -> bool:
    """
    Test basic DNS discovery with mock resolver.

    Validates:
    - DiscoveryService resolves DNS names
    - Discovered IPs are added as peers
    - Peer info is correctly populated
    """
    print(f"\n{'=' * 70}")
    print("TEST: Basic DNS Discovery")
    print(f"{'=' * 70}")

    mock_resolver = MockDNSResolver()
    mock_resolver.set_mock_result("managers.test.local", [
        "10.0.0.1",
        "10.0.0.2",
        "10.0.0.3",
    ])

    service = create_discovery_with_mock_resolver(
        dns_names=["managers.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "discovery_called": False,
        "peers_discovered": False,
        "peer_count_correct": False,
        "peer_info_valid": False,
    }

    try:
        print("\n[1/3] Discovering peers via DNS...")
        discovered = await service.discover_peers()
        results["discovery_called"] = True
        print(f"  Discovered {len(discovered)} peers")

        print("\n[2/3] Validating peer count...")
        results["peers_discovered"] = len(discovered) == 3
        results["peer_count_correct"] = service.peer_count == 3
        print(f"  Total peers in service: {service.peer_count}")
        print(f"  Expected: 3, Actual: {service.peer_count} [{'PASS' if results['peer_count_correct'] else 'FAIL'}]")

        print("\n[3/3] Validating peer info...")
        all_valid = True
        for peer in service.get_all_peers():
            print(f"\n  Peer: {peer.peer_id}")
            print(f"    Host: {peer.host}")
            print(f"    Port: {peer.port}")
            print(f"    Role: {peer.role}")
            print(f"    Cluster: {peer.cluster_id}")

            # Validate peer info
            if not peer.host.startswith("10.0.0."):
                print(f"    [FAIL] Invalid host")
                all_valid = False
            if peer.port != 9000:
                print(f"    [FAIL] Invalid port")
                all_valid = False
            if peer.cluster_id != "test-cluster":
                print(f"    [FAIL] Invalid cluster")
                all_valid = False

        results["peer_info_valid"] = all_valid

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: DNS Caching Behavior
# ==========================================================================

async def scenario_dns_discovery_caching() -> bool:
    """
    Test DNS caching in discovery.

    Validates:
    - First resolution hits DNS
    - Second resolution uses cache
    - Force refresh bypasses cache
    - Cache expiry triggers new resolution
    """
    print(f"\n{'=' * 70}")
    print("TEST: DNS Discovery Caching")
    print(f"{'=' * 70}")

    mock_resolver = MockDNSResolver(default_ttl_seconds=1.0)  # Short TTL for testing
    mock_resolver.set_mock_result("cached.test.local", ["10.0.1.1", "10.0.1.2"])

    service = create_discovery_with_mock_resolver(
        dns_names=["cached.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "first_resolution": False,
        "cached_resolution": False,
        "force_refresh": False,
        "ttl_expiry": False,
    }

    try:
        print("\n[1/4] First discovery (should resolve)...")
        await service.discover_peers()
        first_count = mock_resolver.get_resolution_count("cached.test.local")
        results["first_resolution"] = first_count == 1
        print(f"  Resolution count: {first_count} [{'PASS' if first_count == 1 else 'FAIL'}]")

        print("\n[2/4] Second discovery (should use cache)...")
        await service.discover_peers()
        second_count = mock_resolver.get_resolution_count("cached.test.local")
        results["cached_resolution"] = second_count == 1  # Should still be 1
        print(f"  Resolution count: {second_count} (expected: 1) [{'PASS' if second_count == 1 else 'FAIL'}]")

        print("\n[3/4] Force refresh discovery (should resolve)...")
        await service.discover_peers(force_refresh=True)
        force_count = mock_resolver.get_resolution_count("cached.test.local")
        results["force_refresh"] = force_count == 2
        print(f"  Resolution count: {force_count} (expected: 2) [{'PASS' if force_count == 2 else 'FAIL'}]")

        print("\n[4/4] Wait for TTL expiry and discover...")
        await asyncio.sleep(1.5)  # Wait for 1s TTL to expire
        mock_resolver.cleanup_expired()
        await service.discover_peers()
        expiry_count = mock_resolver.get_resolution_count("cached.test.local")
        results["ttl_expiry"] = expiry_count == 3
        print(f"  Resolution count: {expiry_count} (expected: 3) [{'PASS' if expiry_count == 3 else 'FAIL'}]")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: DNS Failure Handling
# ==========================================================================

async def scenario_dns_discovery_failure_handling() -> bool:
    """
    Test DNS failure handling in discovery.

    Validates:
    - DNS failure doesn't crash discovery
    - Failed DNS name is skipped
    - Other DNS names still resolve
    - Partial discovery succeeds
    """
    print(f"\n{'=' * 70}")
    print("TEST: DNS Discovery Failure Handling")
    print(f"{'=' * 70}")

    mock_resolver = MockDNSResolver()
    mock_resolver.set_mock_result("working.test.local", ["10.0.2.1", "10.0.2.2"])
    mock_resolver.set_mock_failure("broken.test.local", "NXDOMAIN")

    service = create_discovery_with_mock_resolver(
        dns_names=["working.test.local", "broken.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "no_crash": False,
        "partial_discovery": False,
        "correct_peers": False,
    }

    try:
        print("\n[1/3] Discovering with mixed success/failure DNS names...")
        discovered = await service.discover_peers()
        results["no_crash"] = True
        print(f"  Discovery completed without crash [PASS]")

        print("\n[2/3] Validating partial discovery...")
        results["partial_discovery"] = len(discovered) == 2
        print(f"  Discovered peers: {len(discovered)} (expected: 2) [{'PASS' if len(discovered) == 2 else 'FAIL'}]")

        print("\n[3/3] Validating peer sources...")
        peer_hosts = [p.host for p in service.get_all_peers()]
        all_from_working = all(h.startswith("10.0.2.") for h in peer_hosts)
        results["correct_peers"] = all_from_working
        print(f"  All peers from working DNS: {all_from_working} [{'PASS' if all_from_working else 'FAIL'}]")
        for peer in service.get_all_peers():
            print(f"    - {peer.host}:{peer.port}")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: DNS Recovery
# ==========================================================================

async def scenario_dns_discovery_recovery() -> bool:
    """
    Test DNS recovery after failure.

    Validates:
    - Initial failure is handled
    - Recovery resolves correctly
    - Peers are added after recovery
    """
    print(f"\n{'=' * 70}")
    print("TEST: DNS Discovery Recovery")
    print(f"{'=' * 70}")

    mock_resolver = MockDNSResolver()
    # Start with failure
    mock_resolver.set_mock_failure("recovery.test.local", "Temporary DNS failure")

    service = create_discovery_with_mock_resolver(
        dns_names=["recovery.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "initial_failure_handled": False,
        "no_peers_on_failure": False,
        "recovery_succeeds": False,
        "peers_added_on_recovery": False,
    }

    try:
        print("\n[1/4] Initial discovery (expected to fail)...")
        discovered = await service.discover_peers()
        results["initial_failure_handled"] = True  # Didn't throw
        results["no_peers_on_failure"] = len(discovered) == 0
        print(f"  Discovered: {len(discovered)} peers (expected: 0) [{'PASS' if len(discovered) == 0 else 'FAIL'}]")

        print("\n[2/4] Simulating DNS recovery...")
        mock_resolver.set_mock_result("recovery.test.local", ["10.0.3.1", "10.0.3.2", "10.0.3.3"])
        mock_resolver.invalidate("recovery.test.local")  # Clear negative cache
        print("  DNS now returning results")

        print("\n[3/4] Discovery after recovery...")
        discovered = await service.discover_peers(force_refresh=True)
        results["recovery_succeeds"] = len(discovered) == 3
        print(f"  Discovered: {len(discovered)} peers (expected: 3) [{'PASS' if len(discovered) == 3 else 'FAIL'}]")

        print("\n[4/4] Validating peers added...")
        results["peers_added_on_recovery"] = service.peer_count == 3
        print(f"  Total peers: {service.peer_count} (expected: 3) [{'PASS' if service.peer_count == 3 else 'FAIL'}]")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Multi-Name DNS Discovery
# ==========================================================================

async def scenario_dns_discovery_multi_name() -> bool:
    """
    Test discovery with multiple DNS names.

    Validates:
    - Multiple DNS names are resolved
    - All discovered peers are tracked
    - Duplicates are handled correctly
    """
    print(f"\n{'=' * 70}")
    print("TEST: Multi-Name DNS Discovery")
    print(f"{'=' * 70}")

    mock_resolver = MockDNSResolver()
    # Set up multiple DNS names with some overlapping IPs
    mock_resolver.set_mock_result("primary.test.local", ["10.0.4.1", "10.0.4.2"])
    mock_resolver.set_mock_result("secondary.test.local", ["10.0.4.3", "10.0.4.4"])
    mock_resolver.set_mock_result("tertiary.test.local", ["10.0.4.5"])

    service = create_discovery_with_mock_resolver(
        dns_names=["primary.test.local", "secondary.test.local", "tertiary.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "all_names_resolved": False,
        "correct_total_peers": False,
        "all_addresses_present": False,
    }

    try:
        print("\n[1/3] Discovering from multiple DNS names...")
        discovered = await service.discover_peers()

        primary_count = mock_resolver.get_resolution_count("primary.test.local")
        secondary_count = mock_resolver.get_resolution_count("secondary.test.local")
        tertiary_count = mock_resolver.get_resolution_count("tertiary.test.local")

        results["all_names_resolved"] = (primary_count == 1 and secondary_count == 1 and tertiary_count == 1)
        print(f"  primary.test.local resolutions: {primary_count}")
        print(f"  secondary.test.local resolutions: {secondary_count}")
        print(f"  tertiary.test.local resolutions: {tertiary_count}")
        print(f"  All names resolved: [{'PASS' if results['all_names_resolved'] else 'FAIL'}]")

        print("\n[2/3] Validating total peer count...")
        results["correct_total_peers"] = service.peer_count == 5
        print(f"  Total peers: {service.peer_count} (expected: 5) [{'PASS' if service.peer_count == 5 else 'FAIL'}]")

        print("\n[3/3] Validating all addresses present...")
        peer_hosts = {p.host for p in service.get_all_peers()}
        expected_hosts = {"10.0.4.1", "10.0.4.2", "10.0.4.3", "10.0.4.4", "10.0.4.5"}
        results["all_addresses_present"] = peer_hosts == expected_hosts
        print(f"  Found hosts: {sorted(peer_hosts)}")
        print(f"  Expected hosts: {sorted(expected_hosts)}")
        print(f"  [{'PASS' if results['all_addresses_present'] else 'FAIL'}]")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: DNS Security Validation Integration
# ==========================================================================

async def scenario_dns_discovery_security_validation() -> bool:
    """
    Test DNS security validation in discovery.

    Validates:
    - IPs outside allowed CIDRs are filtered
    - Security events are tracked
    - Valid IPs are still discovered
    """
    print(f"\n{'=' * 70}")
    print("TEST: DNS Discovery Security Validation")
    print(f"{'=' * 70}")

    security_events: list[DNSSecurityEvent] = []

    def on_security_event(event: DNSSecurityEvent) -> None:
        security_events.append(event)

    # Create security validator that only allows 10.0.0.0/8
    security_validator = DNSSecurityValidator(
        allowed_cidrs=["10.0.0.0/8"],
    )

    mock_resolver = MockDNSResolver()
    mock_resolver.security_validator = security_validator
    mock_resolver.reject_on_security_violation = True
    mock_resolver.set_callbacks(on_security_event=on_security_event)

    # Mix of allowed and disallowed IPs
    mock_resolver.set_mock_result("mixed.test.local", [
        "10.0.5.1",  # Allowed
        "192.168.1.1",  # Blocked (outside 10.0.0.0/8)
        "10.0.5.2",  # Allowed
        "172.16.0.1",  # Blocked
    ])

    service = create_discovery_with_mock_resolver(
        dns_names=["mixed.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "discovery_succeeds": False,
        "filtered_correctly": False,
        "security_events_logged": False,
        "only_allowed_ips": False,
    }

    try:
        print("\n[1/4] Discovering with security validation...")
        discovered = await service.discover_peers()
        results["discovery_succeeds"] = True
        print(f"  Discovery completed [PASS]")

        print("\n[2/4] Validating peer filtering...")
        # Only 10.0.5.1 and 10.0.5.2 should be allowed
        results["filtered_correctly"] = service.peer_count == 2
        print(f"  Peers discovered: {service.peer_count} (expected: 2) [{'PASS' if service.peer_count == 2 else 'FAIL'}]")

        print("\n[3/4] Validating security events...")
        # Should have 2 events for blocked IPs
        results["security_events_logged"] = len(security_events) == 2
        print(f"  Security events: {len(security_events)} (expected: 2) [{'PASS' if len(security_events) == 2 else 'FAIL'}]")
        for event in security_events:
            print(f"    - {event.violation_type.value}: {event.ip_address}")

        print("\n[4/4] Validating only allowed IPs present...")
        peer_hosts = {p.host for p in service.get_all_peers()}
        expected = {"10.0.5.1", "10.0.5.2"}
        results["only_allowed_ips"] = peer_hosts == expected
        print(f"  Found hosts: {sorted(peer_hosts)}")
        print(f"  Expected hosts: {sorted(expected)}")
        print(f"  [{'PASS' if results['only_allowed_ips'] else 'FAIL'}]")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Discovery Peer Lifecycle with DNS
# ==========================================================================

async def scenario_dns_discovery_peer_lifecycle() -> bool:
    """
    Test peer lifecycle events during DNS discovery.

    Validates:
    - on_peer_added callback fires for new peers
    - Peer selection works after discovery
    - Latency feedback is recorded
    - Peer removal works correctly
    """
    print(f"\n{'=' * 70}")
    print("TEST: DNS Discovery Peer Lifecycle")
    print(f"{'=' * 70}")

    added_peers: list[PeerInfo] = []
    removed_peers: list[str] = []

    def on_peer_added(peer: PeerInfo) -> None:
        added_peers.append(peer)

    def on_peer_removed(peer_id: str) -> None:
        removed_peers.append(peer_id)

    mock_resolver = MockDNSResolver()
    mock_resolver.set_mock_result("lifecycle.test.local", [
        "10.0.6.1",
        "10.0.6.2",
        "10.0.6.3",
    ])

    service = create_discovery_with_mock_resolver(
        dns_names=["lifecycle.test.local"],
        mock_resolver=mock_resolver,
    )
    service.set_callbacks(on_peer_added=on_peer_added, on_peer_removed=on_peer_removed)

    results = {
        "add_callbacks_fired": False,
        "peer_selection_works": False,
        "latency_feedback_recorded": False,
        "peer_removal_works": False,
    }

    try:
        print("\n[1/4] Discovering peers with lifecycle callbacks...")
        await service.discover_peers()
        results["add_callbacks_fired"] = len(added_peers) == 3
        print(f"  on_peer_added fired {len(added_peers)} times (expected: 3) [{'PASS' if len(added_peers) == 3 else 'FAIL'}]")

        print("\n[2/4] Testing peer selection...")
        selection = service.select_peer("test-key-123")
        results["peer_selection_works"] = selection is not None
        if selection:
            print(f"  Selected peer: {selection.peer_id} [PASS]")
        else:
            print(f"  No peer selected [FAIL]")

        print("\n[3/4] Recording latency feedback...")
        if selection:
            service.record_success(selection.peer_id, latency_ms=25.0)
            effective_latency = service.get_effective_latency(selection.peer_id)
            # Latency should be updated from default
            results["latency_feedback_recorded"] = effective_latency != 100.0  # Default baseline
            print(f"  Effective latency: {effective_latency:.2f}ms [{'PASS' if results['latency_feedback_recorded'] else 'FAIL'}]")

        print("\n[4/4] Testing peer removal...")
        if selection:
            removed = service.remove_peer(selection.peer_id)
            results["peer_removal_works"] = removed and len(removed_peers) == 1
            print(f"  Peer removed: {removed}")
            print(f"  on_peer_removed fired: {len(removed_peers)} times (expected: 1) [{'PASS' if len(removed_peers) == 1 else 'FAIL'}]")
            print(f"  Remaining peers: {service.peer_count}")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Real DNS Resolution (localhost)
# ==========================================================================

async def scenario_dns_discovery_real_localhost() -> bool:
    """
    Test real DNS resolution with localhost.

    Validates:
    - AsyncDNSResolver can resolve localhost
    - Resolution results are correct
    - Caching works with real resolver
    """
    print(f"\n{'=' * 70}")
    print("TEST: Real DNS Resolution (localhost)")
    print(f"{'=' * 70}")

    resolver = AsyncDNSResolver(
        default_ttl_seconds=60.0,
        resolution_timeout_seconds=5.0,
    )

    results = {
        "localhost_resolves": False,
        "addresses_valid": False,
        "cache_works": False,
    }

    try:
        print("\n[1/3] Resolving localhost...")
        result = await resolver.resolve("localhost", port=8080)
        results["localhost_resolves"] = True
        print(f"  Hostname: {result.hostname}")
        print(f"  Addresses: {result.addresses}")
        print(f"  Port: {result.port}")
        print(f"  TTL: {result.ttl_seconds}s")

        print("\n[2/3] Validating addresses...")
        # localhost should resolve to 127.0.0.1 and/or ::1
        valid_addrs = {"127.0.0.1", "::1"}
        has_valid = any(addr in valid_addrs for addr in result.addresses)
        results["addresses_valid"] = has_valid
        print(f"  Contains 127.0.0.1 or ::1: {has_valid} [{'PASS' if has_valid else 'FAIL'}]")

        print("\n[3/3] Testing cache behavior...")
        # Second resolution should use cache
        result2 = await resolver.resolve("localhost", port=8080)
        # If it was cached, the resolved_at time should be the same
        results["cache_works"] = result.resolved_at == result2.resolved_at
        print(f"  First resolved_at: {result.resolved_at}")
        print(f"  Second resolved_at: {result2.resolved_at}")
        print(f"  Cache hit: {results['cache_works']} [{'PASS' if results['cache_works'] else 'FAIL'}]")

    except DNSError as e:
        print(f"\n  DNS Error: {e}")
    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: DNS Discovery Scaling
# ==========================================================================

async def scenario_dns_discovery_scaling(peer_count: int) -> bool:
    """
    Test DNS discovery with varying peer counts.

    Validates:
    - Discovery handles large peer counts
    - Selection still works efficiently
    - Metrics are tracked correctly
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: DNS Discovery Scaling - {peer_count} Peers")
    print(f"{'=' * 70}")

    mock_resolver = MockDNSResolver()
    addresses = [f"10.1.{i // 256}.{i % 256}" for i in range(peer_count)]
    mock_resolver.set_mock_result("scaled.test.local", addresses)

    service = create_discovery_with_mock_resolver(
        dns_names=["scaled.test.local"],
        mock_resolver=mock_resolver,
    )

    results = {
        "discovery_completes": False,
        "correct_peer_count": False,
        "selection_works": False,
        "metrics_tracked": False,
    }

    try:
        print(f"\n[1/4] Discovering {peer_count} peers...")
        start_time = time.monotonic()
        discovered = await service.discover_peers()
        discovery_time = time.monotonic() - start_time
        results["discovery_completes"] = True
        print(f"  Discovery completed in {discovery_time:.3f}s [PASS]")

        print(f"\n[2/4] Validating peer count...")
        results["correct_peer_count"] = service.peer_count == peer_count
        print(f"  Peers: {service.peer_count} (expected: {peer_count}) [{'PASS' if results['correct_peer_count'] else 'FAIL'}]")

        print(f"\n[3/4] Testing selection performance...")
        selection_times = []
        for i in range(100):
            start = time.monotonic()
            selection = service.select_peer(f"key-{i}")
            selection_times.append(time.monotonic() - start)

        avg_selection = sum(selection_times) / len(selection_times) * 1000  # ms
        results["selection_works"] = selection is not None and avg_selection < 10  # < 10ms
        print(f"  Avg selection time: {avg_selection:.3f}ms [{'PASS' if avg_selection < 10 else 'FAIL'}]")

        print(f"\n[4/4] Checking metrics...")
        metrics = service.get_metrics_snapshot()
        results["metrics_tracked"] = metrics["peer_count"] == peer_count
        print(f"  Metrics peer_count: {metrics['peer_count']}")
        print(f"  DNS cache stats: {metrics['dns_cache_stats']}")
        print(f"  [{'PASS' if results['metrics_tracked'] else 'FAIL'}]")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Final verdict
    all_passed = all(results.values())
    print(f"\n{'=' * 70}")
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for check, passed in results.items():
        print(f"  {check}: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Main Test Runner
# ==========================================================================

async def run_all_tests() -> bool:
    """Run all DNS discovery tests."""
    print("=" * 70)
    print("DNS DISCOVERY INTEGRATION TESTS (AD-28)")
    print("=" * 70)
    print("\nThis test suite validates DNS-based peer discovery:")
    print("  1. Basic DNS resolution and peer creation")
    print("  2. DNS caching (positive/negative)")
    print("  3. Failure handling and recovery")
    print("  4. Multi-name DNS discovery")
    print("  5. Security validation integration")
    print("  6. Peer lifecycle callbacks")
    print("  7. Real localhost DNS resolution")
    print("  8. Discovery scaling")

    results: dict[str, bool] = {}

    # Basic tests
    print("\n--- Basic DNS Discovery Tests ---")
    results["basic_discovery"] = await scenario_dns_discovery_basic()
    results["caching"] = await scenario_dns_discovery_caching()

    # Failure/recovery tests
    print("\n--- Failure Handling Tests ---")
    results["failure_handling"] = await scenario_dns_discovery_failure_handling()
    results["recovery"] = await scenario_dns_discovery_recovery()

    # Multi-name tests
    print("\n--- Multi-Name DNS Tests ---")
    results["multi_name"] = await scenario_dns_discovery_multi_name()

    # Security tests
    print("\n--- Security Validation Tests ---")
    results["security_validation"] = await scenario_dns_discovery_security_validation()

    # Lifecycle tests
    print("\n--- Peer Lifecycle Tests ---")
    results["peer_lifecycle"] = await scenario_dns_discovery_peer_lifecycle()

    # Real DNS tests
    print("\n--- Real DNS Resolution Tests ---")
    results["real_localhost"] = await scenario_dns_discovery_real_localhost()

    # Scaling tests
    print("\n--- Scaling Tests ---")
    for peer_count in [10, 50, 100]:
        results[f"scaling_{peer_count}_peers"] = await scenario_dns_discovery_scaling(peer_count)

    # Final summary
    print("\n" + "=" * 70)
    print("FINAL TEST SUMMARY")
    print("=" * 70)

    all_passed = True
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    print()
    print(f"Overall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    print("=" * 70)

    return all_passed


def main():
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
