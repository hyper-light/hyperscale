#!/usr/bin/env python3
"""
Health Probes Server Integration Test.

Tests that:
1. LivenessProbe correctly tracks node responsiveness
2. ReadinessProbe correctly tracks if node can accept work
3. StartupProbe delays other probes until initialization complete
4. CompositeProbe aggregates multiple probes correctly
5. Probe state transitions based on threshold configuration
6. Periodic probe execution and automatic health updates

This tests the probe infrastructure defined in AD-19.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.health import (
    HealthProbe,
    LivenessProbe,
    ReadinessProbe,
    StartupProbe,
    CompositeProbe,
    ProbeConfig,
    ProbeResult,
    ProbeResponse,
    ProbeState,
)


async def run_test():
    """Run the health probes integration test."""

    all_passed = True

    try:
        # ==============================================================
        # TEST 1: Basic HealthProbe functionality
        # ==============================================================
        print("[1/8] Testing basic HealthProbe functionality...")
        print("-" * 50)

        check_counter = 0
        check_success = True

        async def basic_check() -> tuple[bool, str]:
            nonlocal check_counter
            check_counter += 1
            if check_success:
                return True, f"Check {check_counter} passed"
            return False, f"Check {check_counter} failed"

        probe = HealthProbe(
            name="basic_test",
            check=basic_check,
            config=ProbeConfig(
                timeout_seconds=1.0,
                failure_threshold=2,
                success_threshold=1,
            ),
        )

        # Verify initial state
        assert probe.is_healthy() is True, "Probe should start healthy"
        assert probe.name == "basic_test", "Probe name should match"
        print("  ✓ Initial state is healthy")

        # Run successful check
        response = await probe.check()
        assert response.result == ProbeResult.SUCCESS, f"Expected SUCCESS, got {response.result}"
        assert probe.is_healthy() is True, "Should remain healthy after success"
        print(f"  ✓ Successful check: {response.message}")

        # Run multiple failures to trigger unhealthy state
        check_success = False
        await probe.check()
        assert probe.is_healthy() is True, "Should still be healthy after 1 failure (threshold=2)"
        print("  ✓ Still healthy after 1 failure (threshold=2)")

        await probe.check()
        assert probe.is_healthy() is False, "Should be unhealthy after 2 consecutive failures"
        print("  ✓ Unhealthy after 2 consecutive failures")

        # Recover with success
        check_success = True
        await probe.check()
        assert probe.is_healthy() is True, "Should recover after 1 success (success_threshold=1)"
        print("  ✓ Recovered after successful check")

        print()

        # ==============================================================
        # TEST 2: Probe timeout handling
        # ==============================================================
        print("[2/8] Testing probe timeout handling...")
        print("-" * 50)

        async def slow_check() -> tuple[bool, str]:
            await asyncio.sleep(2.0)  # Longer than timeout
            return True, "Should not reach here"

        timeout_probe = HealthProbe(
            name="timeout_test",
            check=slow_check,
            config=ProbeConfig(
                timeout_seconds=0.1,
                failure_threshold=1,
                success_threshold=1,
            ),
        )

        response = await timeout_probe.check()
        assert response.result == ProbeResult.TIMEOUT, f"Expected TIMEOUT, got {response.result}"
        assert timeout_probe.is_healthy() is False, "Should be unhealthy after timeout"
        assert "timed out" in response.message.lower(), f"Message should mention timeout: {response.message}"
        print(f"  ✓ Timeout detected: {response.message}")
        print(f"  ✓ Latency recorded: {response.latency_ms:.2f}ms")

        print()

        # ==============================================================
        # TEST 3: Probe error handling
        # ==============================================================
        print("[3/8] Testing probe error handling...")
        print("-" * 50)

        async def error_check() -> tuple[bool, str]:
            raise ValueError("Simulated error")

        error_probe = HealthProbe(
            name="error_test",
            check=error_check,
            config=ProbeConfig(
                timeout_seconds=1.0,
                failure_threshold=1,
                success_threshold=1,
            ),
        )

        response = await error_probe.check()
        assert response.result == ProbeResult.ERROR, f"Expected ERROR, got {response.result}"
        assert error_probe.is_healthy() is False, "Should be unhealthy after error"
        assert "Simulated error" in response.message, f"Message should contain error: {response.message}"
        print(f"  ✓ Error captured: {response.message}")

        print()

        # ==============================================================
        # TEST 4: LivenessProbe with defaults
        # ==============================================================
        print("[4/8] Testing LivenessProbe...")
        print("-" * 50)

        # Default liveness probe should always pass
        liveness = LivenessProbe(name="process")
        response = await liveness.check()
        assert response.result == ProbeResult.SUCCESS, f"Default liveness should pass, got {response.result}"
        assert liveness.is_healthy() is True, "Liveness probe should be healthy"
        print(f"  ✓ Default liveness check passed: {response.message}")

        # Custom liveness check
        process_running = True

        async def custom_liveness_check() -> tuple[bool, str]:
            if process_running:
                return True, "Process responding"
            return False, "Process not responding"

        custom_liveness = LivenessProbe(
            name="custom_process",
            check=custom_liveness_check,
        )

        response = await custom_liveness.check()
        assert response.result == ProbeResult.SUCCESS, "Custom liveness should pass when process running"
        print(f"  ✓ Custom liveness check passed: {response.message}")

        process_running = False
        # Need 3 failures for default config
        await custom_liveness.check()
        await custom_liveness.check()
        await custom_liveness.check()
        assert custom_liveness.is_healthy() is False, "Should be unhealthy when process not running"
        print("  ✓ Custom liveness detects process failure after threshold")

        print()

        # ==============================================================
        # TEST 5: ReadinessProbe with dependency checks
        # ==============================================================
        print("[5/8] Testing ReadinessProbe with dependencies...")
        print("-" * 50)

        database_connected = True
        queue_depth = 100

        async def readiness_check() -> tuple[bool, str]:
            if not database_connected:
                return False, "Database not connected"
            if queue_depth > 1000:
                return False, f"Queue too deep: {queue_depth}"
            return True, f"Ready (queue: {queue_depth})"

        readiness = ReadinessProbe(
            name="service",
            check=readiness_check,
            config=ProbeConfig(
                timeout_seconds=2.0,
                failure_threshold=2,
                success_threshold=1,
            ),
        )

        response = await readiness.check()
        assert response.result == ProbeResult.SUCCESS, "Readiness should pass with all dependencies up"
        print(f"  ✓ Service ready: {response.message}")

        # Simulate database disconnect
        database_connected = False
        await readiness.check()
        await readiness.check()  # Need 2 failures
        assert readiness.is_healthy() is False, "Should be not ready when database down"
        print("  ✓ Service not ready when database disconnected")

        # Reconnect database
        database_connected = True
        await readiness.check()
        assert readiness.is_healthy() is True, "Should recover when database reconnects"
        print("  ✓ Service ready again after database reconnects")

        # Simulate high queue depth
        queue_depth = 1500
        await readiness.check()
        await readiness.check()
        assert readiness.is_healthy() is False, "Should be not ready when queue too deep"
        print("  ✓ Service not ready when queue too deep")

        print()

        # ==============================================================
        # TEST 6: StartupProbe behavior
        # ==============================================================
        print("[6/8] Testing StartupProbe for slow initialization...")
        print("-" * 50)

        init_step = 0
        init_total = 5

        async def startup_check() -> tuple[bool, str]:
            if init_step >= init_total:
                return True, "Startup complete"
            return False, f"Initializing... step {init_step}/{init_total}"

        startup = StartupProbe(
            name="init",
            check=startup_check,
            config=ProbeConfig(
                timeout_seconds=5.0,
                period_seconds=0.1,
                failure_threshold=10,  # Allow many failures during startup
                success_threshold=1,
            ),
        )

        # Startup initially fails but probe stays healthy (high threshold)
        for _ in range(5):
            response = await startup.check()
            assert response.result == ProbeResult.FAILURE, f"Should fail during init, step {init_step}"
            init_step += 1

        # After 5 failures we should still be healthy (threshold=10)
        assert startup.is_healthy() is True, "Should still be healthy during prolonged startup"
        print(f"  ✓ Allows {init_step} startup failures (threshold=10)")

        # Now initialization completes
        init_step = 5
        response = await startup.check()
        assert response.result == ProbeResult.SUCCESS, "Should succeed once initialization complete"
        assert startup.is_healthy() is True, "Should be healthy after startup"
        print(f"  ✓ Startup complete: {response.message}")

        print()

        # ==============================================================
        # TEST 7: CompositeProbe aggregation
        # ==============================================================
        print("[7/8] Testing CompositeProbe aggregation...")
        print("-" * 50)

        # Create individual probes with controllable checks
        db_healthy = True
        cache_healthy = True
        queue_healthy = True

        async def db_check() -> tuple[bool, str]:
            return db_healthy, "Database OK" if db_healthy else "Database down"

        async def cache_check() -> tuple[bool, str]:
            return cache_healthy, "Cache OK" if cache_healthy else "Cache down"

        async def queue_check() -> tuple[bool, str]:
            return queue_healthy, "Queue OK" if queue_healthy else "Queue down"

        db_probe = HealthProbe("database", db_check, ProbeConfig(failure_threshold=1))
        cache_probe = HealthProbe("cache", cache_check, ProbeConfig(failure_threshold=1))
        queue_probe = HealthProbe("queue", queue_check, ProbeConfig(failure_threshold=1))

        composite = CompositeProbe(name="service")
        composite.add_probe(db_probe)
        composite.add_probe(cache_probe)
        composite.add_probe(queue_probe)

        # All probes should be healthy initially
        assert composite.is_healthy() is True, "Composite should be healthy when all probes healthy"
        print("  ✓ Composite healthy when all probes healthy")

        # Check all probes
        results = await composite.check_all()
        assert len(results) == 3, f"Should have 3 results, got {len(results)}"
        for name, response in results.items():
            assert response.result == ProbeResult.SUCCESS, f"{name} should succeed"
        print(f"  ✓ All probes checked: {list(results.keys())}")

        # Fail one probe
        db_healthy = False
        await db_probe.check()
        assert composite.is_healthy() is False, "Composite should be unhealthy when any probe fails"
        unhealthy = composite.get_unhealthy_probes()
        assert "database" in unhealthy, f"Database should be in unhealthy list: {unhealthy}"
        print(f"  ✓ Composite unhealthy when database down: {unhealthy}")

        # Get detailed status
        status = composite.get_status()
        assert status["healthy"] is False, "Status should show unhealthy"
        assert status["probes"]["database"]["healthy"] is False
        assert status["probes"]["cache"]["healthy"] is True
        print(f"  ✓ Status reports correctly: {status['probes']['database']['last_message']}")

        # Remove failed probe
        removed = composite.remove_probe("database")
        assert removed is not None, "Should return removed probe"
        assert removed.name == "database", "Removed probe should be database"
        assert composite.is_healthy() is True, "Composite should be healthy after removing failed probe"
        print("  ✓ Composite healthy after removing failed probe")

        print()

        # ==============================================================
        # TEST 8: Periodic probe execution
        # ==============================================================
        print("[8/8] Testing periodic probe execution...")
        print("-" * 50)

        periodic_check_count = 0

        async def periodic_check() -> tuple[bool, str]:
            nonlocal periodic_check_count
            periodic_check_count += 1
            return True, f"Periodic check #{periodic_check_count}"

        periodic_probe = HealthProbe(
            name="periodic",
            check=periodic_check,
            config=ProbeConfig(
                timeout_seconds=1.0,
                period_seconds=0.1,  # Fast period for testing
                initial_delay_seconds=0.05,
            ),
        )

        # Start periodic checking
        await periodic_probe.start_periodic()
        print("  ✓ Started periodic probe")

        # Wait for some checks to complete
        await asyncio.sleep(0.5)

        # Stop periodic checking
        await periodic_probe.stop_periodic()
        final_count = periodic_check_count
        print(f"  ✓ Stopped periodic probe after {final_count} checks")

        # Verify checks happened
        assert final_count >= 3, f"Expected at least 3 periodic checks, got {final_count}"
        print(f"  ✓ Verified periodic execution ({final_count} checks in 0.5s)")

        # Verify no more checks after stop
        await asyncio.sleep(0.2)
        assert periodic_check_count == final_count, "No more checks should happen after stop"
        print("  ✓ Periodic checks stopped correctly")

        # Test probe state
        state = periodic_probe.get_state()
        assert state.total_checks == final_count, f"State should track {final_count} total checks"
        assert state.healthy is True, "State should be healthy"
        print(f"  ✓ State tracking: {state.total_checks} checks, {state.total_failures} failures")

        # Test reset
        periodic_probe.reset()
        new_state = periodic_probe.get_state()
        assert new_state.total_checks == 0, "Reset should clear total_checks"
        assert new_state.consecutive_successes == 0, "Reset should clear consecutive_successes"
        print("  ✓ Probe reset works correctly")

        print()

        # ==============================================================
        # Final Results
        # ==============================================================
        print("=" * 70)
        print("TEST RESULT: ✓ ALL TESTS PASSED")
        print()
        print("  Health probe infrastructure verified:")
        print("  - Basic HealthProbe with configurable thresholds")
        print("  - Timeout and error handling")
        print("  - LivenessProbe for process responsiveness")
        print("  - ReadinessProbe for dependency checking")
        print("  - StartupProbe for slow initialization")
        print("  - CompositeProbe for aggregation")
        print("  - Periodic probe execution")
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
    print("HEALTH PROBES SERVER INTEGRATION TEST")
    print("=" * 70)
    print("Testing health probe infrastructure for distributed nodes (AD-19)")
    print()

    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
