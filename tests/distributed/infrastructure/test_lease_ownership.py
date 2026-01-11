"""
Test: Lease-Based Job Ownership

This test validates the LeaseManager implementation:
1. Lease acquisition succeeds for unclaimed job
2. Lease renewal extends expiry
3. Lease acquisition fails if held by another node
4. Backup claims lease after primary expires
5. Fence token increments on each claim
6. Explicit release allows immediate re-acquisition
7. State sync imports/exports work correctly

Run with: python examples/servers/test_lease_ownership.py
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from hyperscale.distributed_rewrite.leases import JobLease, LeaseManager, LeaseState


def test_acquire_unclaimed():
    """Test that acquiring an unclaimed job succeeds."""
    print("\n[Test 1] Acquire Unclaimed Job")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=30.0)

    result = manager.acquire("job-123")

    assert result.success, "Should acquire unclaimed job"
    assert result.lease is not None
    assert result.lease.job_id == "job-123"
    assert result.lease.owner_node == "gate-1:9000"
    assert result.lease.fence_token == 1
    assert result.lease.is_active()

    print(f"  ✓ Acquired job-123 with fence_token={result.lease.fence_token}")
    print(f"  ✓ Expires in {result.lease.remaining_seconds():.1f}s")


def test_acquire_already_owned():
    """Test that re-acquiring own lease just extends it."""
    print("\n[Test 2] Re-acquire Own Lease")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=5.0)

    # First acquisition
    result1 = manager.acquire("job-123")
    original_token = result1.lease.fence_token

    # Wait a bit
    time.sleep(0.5)

    # Re-acquire (should just extend)
    result2 = manager.acquire("job-123")

    assert result2.success
    assert result2.lease.fence_token == original_token, "Token should not change on re-acquire"
    assert result2.lease.remaining_seconds() > 4.5, "Should have extended expiry"

    print(f"  ✓ Re-acquired without changing fence_token ({original_token})")
    print(f"  ✓ Expiry extended to {result2.lease.remaining_seconds():.1f}s")


def test_acquire_held_by_other():
    """Test that acquiring a lease held by another node fails."""
    print("\n[Test 3] Acquire Lease Held By Other")
    print("-" * 50)

    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    # Manager1 acquires
    result1 = manager1.acquire("job-123")
    assert result1.success

    # Sync the lease to manager2 (simulating state sync)
    manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=result1.lease.fence_token,
        expires_at=result1.lease.expires_at,
    )

    # Manager2 tries to acquire - should fail
    result2 = manager2.acquire("job-123")

    assert not result2.success, "Should not acquire lease held by other"
    assert result2.current_owner == "gate-1:9000"
    assert result2.expires_in > 0

    print(f"  ✓ Acquisition failed: owned by {result2.current_owner}")
    print(f"  ✓ Expires in {result2.expires_in:.1f}s")


def test_lease_renewal():
    """Test that lease renewal extends expiry."""
    print("\n[Test 4] Lease Renewal")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=2.0)

    # Acquire
    result = manager.acquire("job-123")
    original_expiry = result.lease.expires_at

    # Wait a bit
    time.sleep(0.5)

    # Renew
    renewed = manager.renew("job-123")

    assert renewed, "Renewal should succeed"
    assert result.lease.expires_at > original_expiry, "Expiry should be extended"

    print(f"  ✓ Renewed lease, new expiry in {result.lease.remaining_seconds():.1f}s")

    # Test renewal fails for non-owned job
    other_manager = LeaseManager("gate-2:9000")
    assert not other_manager.renew("job-123"), "Should not renew lease we don't own"
    print("  ✓ Renewal fails for non-owner")


def test_lease_expiry():
    """Test that expired leases can be claimed by another node."""
    print("\n[Test 5] Lease Expiry and Takeover")
    print("-" * 50)

    manager1 = LeaseManager("gate-1:9000", default_duration=0.5)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    # Manager1 acquires with short duration
    result1 = manager1.acquire("job-123")
    token1 = result1.lease.fence_token
    print(f"  Gate-1 acquired with token={token1}")

    # Sync to manager2
    manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=token1,
        expires_at=result1.lease.expires_at,
    )

    # Wait for expiry
    time.sleep(0.6)

    assert result1.lease.is_expired(), "Lease should be expired"
    print("  ✓ Gate-1 lease expired")

    # Manager2 can now acquire
    result2 = manager2.acquire("job-123")

    assert result2.success, "Should acquire after expiry"
    assert result2.lease.fence_token > token1, "Token should increment"
    assert result2.lease.owner_node == "gate-2:9000"

    print(f"  ✓ Gate-2 took over with token={result2.lease.fence_token}")


def test_fence_token_increment():
    """Test that fence tokens increment monotonically."""
    print("\n[Test 6] Fence Token Monotonicity")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=0.2)

    tokens = []
    for i in range(5):
        result = manager.acquire("job-123")
        assert result.success
        tokens.append(result.lease.fence_token)
        manager.release("job-123")
        time.sleep(0.1)

    # Verify monotonic increase
    for i in range(1, len(tokens)):
        assert tokens[i] > tokens[i - 1], f"Token {tokens[i]} should be > {tokens[i - 1]}"

    print(f"  ✓ Tokens increased monotonically: {tokens}")


def test_explicit_release():
    """Test that explicit release allows immediate re-acquisition."""
    print("\n[Test 7] Explicit Release")
    print("-" * 50)

    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    # Manager1 acquires
    result1 = manager1.acquire("job-123")
    token1 = result1.lease.fence_token

    # Sync to manager2
    manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=token1,
        expires_at=result1.lease.expires_at,
    )

    # Manager2 can't acquire (held by manager1)
    result2 = manager2.acquire("job-123")
    assert not result2.success
    print("  ✓ Gate-2 blocked while Gate-1 holds lease")

    # Manager1 releases
    released = manager1.release("job-123")
    assert released
    print("  ✓ Gate-1 released lease")

    # Manager2 can now acquire with force (simulating it saw the release)
    result3 = manager2.acquire("job-123", force=True)
    assert result3.success
    assert result3.lease.fence_token > token1

    print(f"  ✓ Gate-2 acquired after release with token={result3.lease.fence_token}")


def test_state_sync():
    """Test lease state import/export."""
    print("\n[Test 8] State Sync (Import/Export)")
    print("-" * 50)

    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    # Manager1 acquires multiple jobs
    manager1.acquire("job-1")
    manager1.acquire("job-2")
    manager1.acquire("job-3")

    # Export state
    exported = manager1.export_leases()
    assert len(exported) == 3

    print(f"  Exported {len(exported)} leases:")
    for lease_data in exported:
        print(f"    - {lease_data['job_id']}: token={lease_data['fence_token']}")

    # Import to manager2
    for lease_data in exported:
        manager2.import_lease(
            job_id=lease_data["job_id"],
            owner_node=lease_data["owner_node"],
            fence_token=lease_data["fence_token"],
            expires_at=time.monotonic() + lease_data["expires_in"],
            lease_duration=lease_data["lease_duration"],
        )

    # Manager2 should know about the leases
    for job_id in ["job-1", "job-2", "job-3"]:
        lease = manager2.get_lease(job_id)
        assert lease is not None
        assert lease.owner_node == "gate-1:9000"

    print("  ✓ All leases imported correctly")

    # Manager2 should not be able to acquire (held by manager1)
    for job_id in ["job-1", "job-2", "job-3"]:
        result = manager2.acquire(job_id)
        assert not result.success

    print("  ✓ Manager2 correctly blocked from acquiring imported leases")


def test_owned_jobs():
    """Test getting list of owned jobs."""
    print("\n[Test 9] Get Owned Jobs")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=30.0)

    # Acquire several jobs
    manager.acquire("job-1")
    manager.acquire("job-2")
    manager.acquire("job-3")

    owned = manager.get_owned_jobs()
    assert len(owned) == 3
    assert set(owned) == {"job-1", "job-2", "job-3"}

    print(f"  ✓ Owns {len(owned)} jobs: {owned}")

    # Release one
    manager.release("job-2")
    owned = manager.get_owned_jobs()
    assert len(owned) == 2
    assert "job-2" not in owned

    print(f"  ✓ After release, owns {len(owned)} jobs: {owned}")


def test_is_owner():
    """Test ownership checking."""
    print("\n[Test 10] Ownership Check")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=30.0)

    assert not manager.is_owner("job-123"), "Should not own unacquired job"
    print("  ✓ Not owner of unacquired job")

    manager.acquire("job-123")
    assert manager.is_owner("job-123"), "Should own acquired job"
    print("  ✓ Is owner of acquired job")

    manager.release("job-123")
    assert not manager.is_owner("job-123"), "Should not own released job"
    print("  ✓ Not owner of released job")


def test_concurrent_operations():
    """Test thread safety of lease operations."""
    print("\n[Test 11] Thread Safety")
    print("-" * 50)

    manager = LeaseManager("gate-1:9000", default_duration=1.0)
    errors: list[str] = []
    iterations = 500

    def acquire_renew_release(thread_id: int):
        try:
            for i in range(iterations):
                job_id = f"job-{thread_id}-{i % 10}"
                manager.acquire(job_id)
                manager.renew(job_id)
                manager.is_owner(job_id)
                manager.get_fence_token(job_id)
                manager.release(job_id)
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(acquire_renew_release, i) for i in range(4)]
        for f in futures:
            f.result()

    if errors:
        for error in errors:
            print(f"  ✗ {error}")
        raise AssertionError(f"{len(errors)} thread safety errors")

    print(f"  ✓ {iterations * 4} concurrent operations completed without errors")


def test_force_acquire():
    """Test forced acquisition for failover scenarios."""
    print("\n[Test 12] Force Acquire (Failover)")
    print("-" * 50)

    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    # Manager1 acquires
    result1 = manager1.acquire("job-123")
    token1 = result1.lease.fence_token

    # Sync to manager2
    manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=token1,
        expires_at=result1.lease.expires_at,
    )

    # Normal acquire fails
    result2 = manager2.acquire("job-123")
    assert not result2.success
    print("  ✓ Normal acquire blocked")

    # Force acquire succeeds (simulating detected failure of gate-1)
    result3 = manager2.acquire("job-123", force=True)
    assert result3.success
    assert result3.lease.fence_token > token1
    assert result3.lease.owner_node == "gate-2:9000"

    print(f"  ✓ Force acquire succeeded with token={result3.lease.fence_token}")


async def test_cleanup_task():
    """Test background cleanup task."""
    print("\n[Test 13] Background Cleanup Task")
    print("-" * 50)

    expired_leases: list[JobLease] = []

    def on_expired(lease: JobLease):
        expired_leases.append(lease)

    manager = LeaseManager(
        "gate-1:9000",
        default_duration=0.3,
        cleanup_interval=0.2,
        on_lease_expired=on_expired,
    )

    # Start cleanup task
    await manager.start_cleanup_task()

    # Acquire a lease
    manager.acquire("job-123")
    print("  ✓ Acquired lease with 0.3s duration")

    # Wait for expiry and cleanup
    await asyncio.sleep(0.6)

    # Stop cleanup task
    await manager.stop_cleanup_task()

    assert len(expired_leases) > 0, "Should have detected expired lease"
    assert expired_leases[0].job_id == "job-123"
    print(f"  ✓ Cleanup detected {len(expired_leases)} expired lease(s)")


async def main():
    """Run all lease ownership tests."""
    print("=" * 60)
    print("LEASE-BASED JOB OWNERSHIP TEST")
    print("=" * 60)

    start_time = time.monotonic()

    try:
        test_acquire_unclaimed()
        test_acquire_already_owned()
        test_acquire_held_by_other()
        test_lease_renewal()
        test_lease_expiry()
        test_fence_token_increment()
        test_explicit_release()
        test_state_sync()
        test_owned_jobs()
        test_is_owner()
        test_concurrent_operations()
        test_force_acquire()
        await test_cleanup_task()

        elapsed = time.monotonic() - start_time
        print("\n" + "=" * 60)
        print(f"ALL TESTS PASSED ({elapsed:.2f}s)")
        print("=" * 60)

    except AssertionError as e:
        elapsed = time.monotonic() - start_time
        print("\n" + "=" * 60)
        print(f"TEST FAILED ({elapsed:.2f}s): {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    asyncio.run(main())
