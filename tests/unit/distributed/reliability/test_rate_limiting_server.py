#!/usr/bin/env python3
"""
Rate Limiting Server Integration Test.

Tests that:
1. ServerRateLimiter provides per-client rate limiting
2. Rate limit responses include proper Retry-After information
3. Client cleanup prevents memory leaks

This tests the rate limiting infrastructure defined in AD-24.
"""

import asyncio
import sys
import time


from hyperscale.distributed.reliability import (
    RateLimitResult,
    AdaptiveRateLimitConfig,
    ServerRateLimiter,
)


async def run_test():
    """Run the rate limiting integration test."""

    try:
        # ==============================================================
        # TEST 1: ServerRateLimiter per-client buckets
        # ==============================================================
        print("[1/4] Testing ServerRateLimiter per-client buckets...")
        print("-" * 50)

        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (5, 0.5), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Client 1 makes requests
        for i in range(5):
            result = await limiter.check_rate_limit("client-1", "test_op")
            assert result.allowed is True, f"Request {i+1} should be allowed"
        print("  ✓ Client-1: 5 requests allowed (bucket exhausted)")

        # Client 1's next request should be rate limited
        result = await limiter.check_rate_limit("client-1", "test_op")
        assert result.allowed is False, "6th request should be rate limited"
        assert result.retry_after_seconds > 0, "Should have retry_after time"
        print(f"  ✓ Client-1: 6th request rate limited (retry_after={result.retry_after_seconds:.3f}s)")

        # Client 2 should have separate bucket
        for i in range(5):
            result = await limiter.check_rate_limit("client-2", "test_op")
            assert result.allowed is True, f"Client-2 request {i+1} should be allowed"
        print("  ✓ Client-2: Has separate bucket, 5 requests allowed")

        # Check metrics
        metrics = limiter.get_metrics()
        assert metrics["total_requests"] == 11, f"Should have 11 total requests, got {metrics['total_requests']}"
        assert metrics["rate_limited_requests"] == 1, f"Should have 1 rate limited, got {metrics['rate_limited_requests']}"
        assert metrics["active_clients"] == 2, f"Should have 2 clients, got {metrics['active_clients']}"
        print(f"  ✓ Metrics: {metrics['total_requests']} total, {metrics['rate_limited_requests']} limited, {metrics['active_clients']} clients")

        print()

        # ==============================================================
        # TEST 2: ServerRateLimiter client stats and reset
        # ==============================================================
        print("[2/4] Testing ServerRateLimiter client stats and reset...")
        print("-" * 50)

        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"op_a": (10, 1.0), "op_b": (20, 2.0), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Use different operations
        await limiter.check_rate_limit("client-1", "op_a")
        await limiter.check_rate_limit("client-1", "op_a")
        await limiter.check_rate_limit("client-1", "op_b")

        stats = limiter.get_client_stats("client-1")
        assert "op_a" in stats, "Should have op_a stats"
        assert "op_b" in stats, "Should have op_b stats"
        assert stats["op_a"] == 8.0, f"op_a should have 8 tokens, got {stats['op_a']}"
        assert stats["op_b"] == 19.0, f"op_b should have 19 tokens, got {stats['op_b']}"
        print(f"  ✓ Client stats: op_a={stats['op_a']}, op_b={stats['op_b']}")

        # Reset client
        limiter.reset_client("client-1")
        stats = limiter.get_client_stats("client-1")
        assert stats["op_a"] == 10.0, f"op_a should be reset to 10, got {stats['op_a']}"
        assert stats["op_b"] == 20.0, f"op_b should be reset to 20, got {stats['op_b']}"
        print(f"  ✓ After reset: op_a={stats['op_a']}, op_b={stats['op_b']}")

        print()

        # ==============================================================
        # TEST 3: ServerRateLimiter inactive client cleanup
        # ==============================================================
        print("[3/4] Testing ServerRateLimiter inactive client cleanup...")
        print("-" * 50)

        limiter = ServerRateLimiter(
            adaptive_config=AdaptiveRateLimitConfig(inactive_cleanup_seconds=0.1),  # Very short for testing
        )

        # Create some clients
        for i in range(5):
            await limiter.check_rate_limit(f"client-{i}", "test_op")

        assert limiter.get_metrics()["active_clients"] == 5, "Should have 5 clients"
        print("  ✓ Created 5 clients")

        # Cleanup immediately - should find no inactive clients
        cleaned = await limiter.cleanup_inactive_clients()
        assert cleaned == 0, f"Should clean 0 clients (all active), got {cleaned}"
        print("  ✓ No clients cleaned immediately")

        # Wait for inactivity threshold
        await asyncio.sleep(0.15)

        # Now cleanup should find inactive clients
        cleaned = await limiter.cleanup_inactive_clients()
        assert cleaned == 5, f"Should clean 5 inactive clients, got {cleaned}"
        assert limiter.get_metrics()["active_clients"] == 0, "Should have 0 clients after cleanup"
        print(f"  ✓ Cleaned {cleaned} inactive clients after timeout")

        print()

        # ==============================================================
        # TEST 4: ServerRateLimiter async with wait
        # ==============================================================
        print("[4/4] Testing ServerRateLimiter async check with wait...")
        print("-" * 50)

        config = AdaptiveRateLimitConfig(default_max_requests=100, default_window_size=10.0, operation_limits={"test_op": (2, 0.2), "default": (100, 10.0)})
        limiter = ServerRateLimiter(adaptive_config=config)

        # Exhaust bucket
        await limiter.check_rate_limit("client-1", "test_op")
        await limiter.check_rate_limit("client-1", "test_op")

        # Check without wait
        result = await limiter.check_rate_limit_async("client-1", "test_op", max_wait=0.0)
        assert result.allowed is False, "Should be rate limited without wait"
        print("  ✓ Rate limited without wait")

        # Check with wait
        start = time.monotonic()
        result = await limiter.check_rate_limit_async("client-1", "test_op", max_wait=0.5)
        elapsed = time.monotonic() - start
        assert result.allowed is True, "Should succeed within max_wait"
        # The sliding window weights the previous window's 2 requests by the
        # share of it still overlapping; a third is admitted once that weight
        # falls to 1 -- at least half a window after the window rolls.
        window_seconds = config.operation_limits["test_op"][1]
        assert elapsed >= window_seconds / 2, f"Admitted before the window could decay, took {elapsed:.3f}s"
        print(f"  ✓ Succeeded after waiting {elapsed:.3f}s")

        print()

        # ==============================================================
        # Final Results
        # ==============================================================
        print("=" * 70)
        print("TEST RESULT: ✓ ALL TESTS PASSED")
        print()
        print("  Rate limiting infrastructure verified:")
        print("  - ServerRateLimiter per-client buckets")
        print("  - ServerRateLimiter client stats and reset")
        print("  - ServerRateLimiter inactive client cleanup")
        print("  - ServerRateLimiter async check with wait")
        print("=" * 70)

        return True

    except AssertionError as e:
        print(f"\n✗ Test assertion failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 70)
    print("RATE LIMITING SERVER INTEGRATION TEST")
    print("=" * 70)
    print("Testing rate limiting infrastructure (AD-24)")
    print()

    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
