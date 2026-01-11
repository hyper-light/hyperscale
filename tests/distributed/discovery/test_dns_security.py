#!/usr/bin/env python3
"""
DNS Security Integration Tests (AD-28 Phase 2).

Tests the DNS security features that protect against:
- DNS Cache Poisoning: IP range validation
- DNS Hijacking: Anomaly detection
- DNS Spoofing: IP change tracking
- DNS Rebinding: Private IP blocking for public hosts

Test scenarios:
1. IP range validation (CIDR filtering)
2. Rapid IP rotation detection
3. DNS rebinding protection
4. Security event logging and callbacks
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.discovery.dns.security import (
    DNSSecurityValidator,
    DNSSecurityEvent,
    DNSSecurityViolation,
)
from hyperscale.distributed.discovery.dns.resolver import (
    AsyncDNSResolver,
    DNSResult,
    DNSError,
)


# ==========================================================================
# Test: IP Range Validation
# ==========================================================================

def scenario_ip_range_validation_allows_in_range():
    """Test that IPs within allowed CIDR ranges pass validation."""
    print(f"\n{'=' * 70}")
    print("TEST: IP Range Validation - Allows In-Range IPs")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
    )

    test_cases = [
        ("manager.local", "10.0.1.5", True),
        ("manager.local", "10.255.255.255", True),
        ("worker.local", "172.16.0.1", True),
        ("worker.local", "172.31.255.255", True),
        ("gate.local", "192.168.1.1", True),
        ("gate.local", "192.168.255.254", True),
    ]

    results = {"passed": 0, "failed": 0}

    for hostname, ip, should_pass in test_cases:
        event = validator.validate(hostname, ip)
        passed = (event is None) == should_pass

        status = "PASS" if passed else "FAIL"
        print(f"  {hostname} -> {ip}: {status}")

        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1
            print(f"    Expected: {'valid' if should_pass else 'violation'}")
            print(f"    Got: {event}")

    print(f"\n{'=' * 70}")
    all_passed = results["failed"] == 0
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    print(f"  Passed: {results['passed']}, Failed: {results['failed']}")
    print(f"{'=' * 70}")

    return all_passed


def scenario_ip_range_validation_rejects_out_of_range():
    """Test that IPs outside allowed CIDR ranges are rejected."""
    print(f"\n{'=' * 70}")
    print("TEST: IP Range Validation - Rejects Out-of-Range IPs")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=["10.0.0.0/8"]  # Only allow 10.x.x.x
    )

    test_cases = [
        ("manager.local", "192.168.1.1", False),  # Should be rejected
        ("manager.local", "172.16.0.1", False),   # Should be rejected
        ("manager.local", "8.8.8.8", False),      # Should be rejected
        ("manager.local", "1.2.3.4", False),      # Should be rejected
    ]

    results = {"passed": 0, "failed": 0}

    for hostname, ip, should_pass in test_cases:
        event = validator.validate(hostname, ip)
        is_valid = event is None
        passed = is_valid == should_pass

        status = "PASS" if passed else "FAIL"
        violation_type = event.violation_type.value if event else "none"
        print(f"  {hostname} -> {ip}: {status} (violation: {violation_type})")

        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1

        # Verify correct violation type
        if not should_pass and event:
            if event.violation_type != DNSSecurityViolation.IP_OUT_OF_RANGE:
                print(f"    Wrong violation type: {event.violation_type}")
                results["failed"] += 1
                results["passed"] -= 1

    print(f"\n{'=' * 70}")
    all_passed = results["failed"] == 0
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    print(f"  Passed: {results['passed']}, Failed: {results['failed']}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Rapid IP Rotation Detection
# ==========================================================================

def scenario_rapid_ip_rotation_detection():
    """Test detection of rapid IP rotation (fast-flux attack indicator)."""
    print(f"\n{'=' * 70}")
    print("TEST: Rapid IP Rotation Detection")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=[],  # Disable CIDR check
        detect_ip_changes=True,
        max_ip_changes_per_window=3,  # Low threshold for testing
        ip_change_window_seconds=60.0,
    )

    hostname = "suspicious.local"

    print(f"\n  Testing rapid rotation for '{hostname}'...")
    print(f"  Max changes allowed: {validator.max_ip_changes_per_window}")

    # Simulate rapid IP changes
    ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"]
    rotation_detected = False

    for i, ip in enumerate(ips):
        event = validator.validate(hostname, ip)
        if event and event.violation_type == DNSSecurityViolation.RAPID_IP_ROTATION:
            print(f"  Change {i + 1}: {ip} -> RAPID ROTATION DETECTED")
            rotation_detected = True
            break
        else:
            print(f"  Change {i + 1}: {ip} -> ok")

    print(f"\n{'=' * 70}")
    passed = rotation_detected
    print(f"TEST RESULT: {'PASSED' if passed else 'FAILED'}")
    print(f"  Rapid rotation detected: {rotation_detected}")
    print(f"{'=' * 70}")

    return passed


# ==========================================================================
# Test: DNS Rebinding Protection
# ==========================================================================

def scenario_dns_rebinding_protection():
    """Test blocking of private IPs for public hostnames."""
    print(f"\n{'=' * 70}")
    print("TEST: DNS Rebinding Protection")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=[],  # Disable CIDR check
        block_private_for_public=True,
        detect_ip_changes=False,
    )

    test_cases = [
        # Internal hostnames - should allow private IPs
        ("manager.local", "10.0.0.1", True),
        ("service.internal", "172.16.0.1", True),
        ("app.svc.cluster.local", "192.168.1.1", True),

        # Public hostnames - should block private IPs
        ("api.example.com", "10.0.0.1", False),
        ("service.example.org", "192.168.1.1", False),
        ("app.malicious.com", "127.0.0.1", False),

        # Public hostnames with public IPs - should allow
        ("api.example.com", "8.8.8.8", True),
        ("service.example.org", "1.1.1.1", True),
    ]

    results = {"passed": 0, "failed": 0}

    for hostname, ip, should_pass in test_cases:
        event = validator.validate(hostname, ip)
        is_valid = event is None
        passed = is_valid == should_pass

        status = "PASS" if passed else "FAIL"
        print(f"  {hostname} -> {ip}: {status}")

        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1
            expected = "allowed" if should_pass else "blocked"
            actual = "allowed" if is_valid else "blocked"
            print(f"    Expected: {expected}, Got: {actual}")

    print(f"\n{'=' * 70}")
    all_passed = results["failed"] == 0
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    print(f"  Passed: {results['passed']}, Failed: {results['failed']}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Security Event Logging
# ==========================================================================

def scenario_security_event_logging():
    """Test that security events are properly logged and retrievable."""
    print(f"\n{'=' * 70}")
    print("TEST: Security Event Logging")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=["10.0.0.0/8"],
        detect_ip_changes=True,
        max_ip_changes_per_window=2,
    )

    # Generate some violations
    print("\n  Generating security violations...")

    # Out of range
    validator.validate("host1.local", "192.168.1.1")
    validator.validate("host2.local", "172.16.0.1")

    # Rapid rotation
    validator.validate("host3.local", "10.0.0.1")
    validator.validate("host3.local", "10.0.0.2")
    validator.validate("host3.local", "10.0.0.3")
    validator.validate("host3.local", "10.0.0.4")

    # Get events
    all_events = validator.get_recent_events(limit=100)
    out_of_range_events = validator.get_recent_events(
        limit=100,
        violation_type=DNSSecurityViolation.IP_OUT_OF_RANGE
    )
    rotation_events = validator.get_recent_events(
        limit=100,
        violation_type=DNSSecurityViolation.RAPID_IP_ROTATION
    )

    print(f"\n  Total events: {len(all_events)}")
    print(f"  Out-of-range events: {len(out_of_range_events)}")
    print(f"  Rapid rotation events: {len(rotation_events)}")

    # Print event details
    print("\n  Event details:")
    for event in all_events[-5:]:
        print(f"    - {event.violation_type.value}: {event.hostname} -> {event.resolved_ip}")

    # Check stats
    stats = validator.stats
    print(f"\n  Stats: {stats}")

    # Verify
    results = {
        "has_events": len(all_events) > 0,
        "has_out_of_range": len(out_of_range_events) >= 2,
        "has_rotation": len(rotation_events) >= 1,
        "stats_correct": stats["total_events"] == len(all_events),
    }

    print(f"\n{'=' * 70}")
    all_passed = all(results.values())
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for key, value in results.items():
        print(f"  {key}: {'PASS' if value else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Resolver Integration
# ==========================================================================

async def scenario_resolver_security_integration():
    """Test that security validator integrates with DNS resolver."""
    print(f"\n{'=' * 70}")
    print("TEST: Resolver Security Integration")
    print(f"{'=' * 70}")

    security_events: list[DNSSecurityEvent] = []

    def on_security_event(event: DNSSecurityEvent) -> None:
        security_events.append(event)
        print(f"    Security event: {event.violation_type.value} for {event.hostname}")

    validator = DNSSecurityValidator(
        allowed_cidrs=["127.0.0.0/8"],  # Only allow localhost
    )

    resolver = AsyncDNSResolver(
        security_validator=validator,
        reject_on_security_violation=True,
    )
    resolver.set_callbacks(on_security_event=on_security_event)

    print("\n  Testing localhost resolution (should pass)...")
    try:
        result = await resolver.resolve("localhost")
        localhost_passed = any("127" in addr for addr in result.addresses)
        print(f"    Result: {result.addresses}")
        print(f"    Contains localhost: {localhost_passed}")
    except DNSError as exc:
        print(f"    Error: {exc}")
        localhost_passed = False

    # Note: Testing rejection requires a hostname that resolves to non-local IP
    # For unit testing, we'd mock the DNS response
    # Here we just verify the resolver has security enabled
    print("\n  Verifying security is enabled...")
    stats = resolver.security_stats
    print(f"    Security stats: {stats}")
    security_enabled = stats.get("enabled", False)

    print(f"\n{'=' * 70}")
    all_passed = localhost_passed and security_enabled
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    print(f"  Localhost resolution: {'PASS' if localhost_passed else 'FAIL'}")
    print(f"  Security enabled: {'PASS' if security_enabled else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: Batch Validation
# ==========================================================================

def scenario_batch_ip_validation():
    """Test batch validation and filtering of IPs."""
    print(f"\n{'=' * 70}")
    print("TEST: Batch IP Validation")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=["10.0.0.0/8", "192.168.0.0/16"]
    )

    hostname = "service.local"
    mixed_ips = [
        "10.0.0.1",      # Valid
        "192.168.1.1",   # Valid
        "172.16.0.1",    # Invalid (not in allowed CIDRs)
        "10.0.0.2",      # Valid
        "8.8.8.8",       # Invalid
        "192.168.2.1",   # Valid
    ]

    print(f"\n  Input IPs: {mixed_ips}")

    # Test batch validation
    events = validator.validate_batch(hostname, mixed_ips)
    print(f"  Violations: {len(events)}")
    for event in events:
        print(f"    - {event.resolved_ip}: {event.violation_type.value}")

    # Test filtering
    valid_ips = validator.filter_valid_ips(hostname, mixed_ips)
    print(f"\n  Valid IPs: {valid_ips}")

    # Verify
    expected_valid = ["10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.2.1"]
    expected_violations = 2  # 172.16.0.1 and 8.8.8.8

    results = {
        "correct_valid_count": len(valid_ips) == len(expected_valid),
        "correct_violation_count": len(events) == expected_violations,
        "valid_ips_match": set(valid_ips) == set(expected_valid),
    }

    print(f"\n{'=' * 70}")
    all_passed = all(results.values())
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    for key, value in results.items():
        print(f"  {key}: {'PASS' if value else 'FAIL'}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Test: IPv6 Support
# ==========================================================================

def scenario_ipv6_validation():
    """Test that IPv6 addresses are properly validated."""
    print(f"\n{'=' * 70}")
    print("TEST: IPv6 Validation")
    print(f"{'=' * 70}")

    validator = DNSSecurityValidator(
        allowed_cidrs=[
            "10.0.0.0/8",           # IPv4 private
            "2001:db8::/32",        # IPv6 documentation range
            "fd00::/8",             # IPv6 unique local
        ]
    )

    test_cases = [
        ("host.local", "10.0.0.1", True),              # IPv4 in range
        ("host.local", "2001:db8::1", True),           # IPv6 in range
        ("host.local", "fd00::1", True),               # IPv6 unique local in range
        ("host.local", "2607:f8b0:4004:800::200e", False),  # Google DNS IPv6
        ("host.local", "::1", False),                  # IPv6 loopback not in allowed
    ]

    results = {"passed": 0, "failed": 0}

    for hostname, ip, should_pass in test_cases:
        event = validator.validate(hostname, ip)
        is_valid = event is None
        passed = is_valid == should_pass

        status = "PASS" if passed else "FAIL"
        print(f"  {hostname} -> {ip}: {status}")

        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1

    print(f"\n{'=' * 70}")
    all_passed = results["failed"] == 0
    print(f"TEST RESULT: {'PASSED' if all_passed else 'FAILED'}")
    print(f"  Passed: {results['passed']}, Failed: {results['failed']}")
    print(f"{'=' * 70}")

    return all_passed


# ==========================================================================
# Main Test Runner
# ==========================================================================

async def run_all_tests():
    """Run all DNS security tests."""
    results = {}

    print("\n" + "=" * 70)
    print("DNS SECURITY INTEGRATION TESTS")
    print("=" * 70)
    print("\nThis test suite validates DNS security features:")
    print("  1. IP range validation (CIDR filtering)")
    print("  2. Rapid IP rotation detection (fast-flux)")
    print("  3. DNS rebinding protection")
    print("  4. Security event logging")
    print("  5. Resolver integration")
    print("  6. Batch validation")
    print("  7. IPv6 support")

    # Synchronous tests
    print("\n--- IP Range Validation Tests ---")
    results["ip_range_allows"] = scenario_ip_range_validation_allows_in_range()
    results["ip_range_rejects"] = scenario_ip_range_validation_rejects_out_of_range()

    print("\n--- Anomaly Detection Tests ---")
    results["rapid_rotation"] = scenario_rapid_ip_rotation_detection()

    print("\n--- Rebinding Protection Tests ---")
    results["rebinding_protection"] = scenario_dns_rebinding_protection()

    print("\n--- Event Logging Tests ---")
    results["event_logging"] = scenario_security_event_logging()

    print("\n--- Batch Validation Tests ---")
    results["batch_validation"] = scenario_batch_ip_validation()

    print("\n--- IPv6 Support Tests ---")
    results["ipv6_validation"] = scenario_ipv6_validation()

    # Async tests
    print("\n--- Resolver Integration Tests ---")
    results["resolver_integration"] = await scenario_resolver_security_integration()

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

    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    print("=" * 70)

    return all_passed


def main():
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
