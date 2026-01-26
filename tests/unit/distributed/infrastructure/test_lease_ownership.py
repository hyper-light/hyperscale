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

Run with: pytest tests/unit/distributed/infrastructure/test_lease_ownership.py
"""

import asyncio
import time

import pytest

from hyperscale.distributed.leases import JobLease, LeaseManager


@pytest.mark.asyncio
async def test_acquire_unclaimed():
    """Test that acquiring an unclaimed job succeeds."""
    manager = LeaseManager("gate-1:9000", default_duration=30.0)

    result = await manager.acquire("job-123")

    assert result.success, "Should acquire unclaimed job"
    assert result.lease is not None
    assert result.lease.job_id == "job-123"
    assert result.lease.owner_node == "gate-1:9000"
    assert result.lease.fence_token == 1
    assert result.lease.is_active()


@pytest.mark.asyncio
async def test_acquire_already_owned():
    """Test that re-acquiring own lease just extends it."""
    manager = LeaseManager("gate-1:9000", default_duration=5.0)

    result1 = await manager.acquire("job-123")
    original_token = result1.lease.fence_token

    await asyncio.sleep(0.1)

    result2 = await manager.acquire("job-123")

    assert result2.success
    assert result2.lease.fence_token == original_token, (
        "Token should not change on re-acquire"
    )
    assert result2.lease.remaining_seconds() > 4.5, "Should have extended expiry"


@pytest.mark.asyncio
async def test_acquire_held_by_other():
    """Test that acquiring a lease held by another node fails."""
    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    result1 = await manager1.acquire("job-123")
    assert result1.success

    await manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=result1.lease.fence_token,
        expires_at=result1.lease.expires_at,
    )

    result2 = await manager2.acquire("job-123")

    assert not result2.success, "Should not acquire lease held by other"
    assert result2.current_owner == "gate-1:9000"
    assert result2.expires_in > 0


@pytest.mark.asyncio
async def test_lease_renewal():
    """Test that lease renewal extends expiry."""
    manager = LeaseManager("gate-1:9000", default_duration=2.0)

    result = await manager.acquire("job-123")
    original_expiry = result.lease.expires_at

    await asyncio.sleep(0.1)

    renewed = await manager.renew("job-123")

    assert renewed, "Renewal should succeed"
    assert result.lease.expires_at > original_expiry, "Expiry should be extended"

    other_manager = LeaseManager("gate-2:9000")
    assert not await other_manager.renew("job-123"), (
        "Should not renew lease we don't own"
    )


@pytest.mark.asyncio
async def test_lease_expiry():
    """Test that expired leases can be claimed by another node."""
    manager1 = LeaseManager("gate-1:9000", default_duration=0.3)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    result1 = await manager1.acquire("job-123")
    token1 = result1.lease.fence_token

    await manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=token1,
        expires_at=result1.lease.expires_at,
    )

    await asyncio.sleep(0.4)

    assert result1.lease.is_expired(), "Lease should be expired"

    result2 = await manager2.acquire("job-123")

    assert result2.success, "Should acquire after expiry"
    assert result2.lease.fence_token > token1, "Token should increment"
    assert result2.lease.owner_node == "gate-2:9000"


@pytest.mark.asyncio
async def test_fence_token_increment():
    """Test that fence tokens increment monotonically."""
    manager = LeaseManager("gate-1:9000", default_duration=0.2)

    tokens = []
    for i in range(5):
        result = await manager.acquire("job-123")
        assert result.success
        tokens.append(result.lease.fence_token)
        await manager.release("job-123")
        await asyncio.sleep(0.05)

    for i in range(1, len(tokens)):
        assert tokens[i] > tokens[i - 1], (
            f"Token {tokens[i]} should be > {tokens[i - 1]}"
        )


@pytest.mark.asyncio
async def test_explicit_release():
    """Test that explicit release allows immediate re-acquisition."""
    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    result1 = await manager1.acquire("job-123")
    token1 = result1.lease.fence_token

    await manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=token1,
        expires_at=result1.lease.expires_at,
    )

    result2 = await manager2.acquire("job-123")
    assert not result2.success

    released = await manager1.release("job-123")
    assert released

    result3 = await manager2.acquire("job-123", force=True)
    assert result3.success
    assert result3.lease.fence_token > token1


@pytest.mark.asyncio
async def test_state_sync():
    """Test lease state import/export."""
    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    await manager1.acquire("job-1")
    await manager1.acquire("job-2")
    await manager1.acquire("job-3")

    exported = await manager1.export_leases()
    assert len(exported) == 3

    for lease_data in exported:
        await manager2.import_lease(
            job_id=lease_data["job_id"],
            owner_node=lease_data["owner_node"],
            fence_token=lease_data["fence_token"],
            expires_at=time.monotonic() + lease_data["expires_in"],
            lease_duration=lease_data["lease_duration"],
        )

    for job_id in ["job-1", "job-2", "job-3"]:
        lease = await manager2.get_lease(job_id)
        assert lease is not None
        assert lease.owner_node == "gate-1:9000"

    for job_id in ["job-1", "job-2", "job-3"]:
        result = await manager2.acquire(job_id)
        assert not result.success


@pytest.mark.asyncio
async def test_owned_jobs():
    """Test getting list of owned jobs."""
    manager = LeaseManager("gate-1:9000", default_duration=30.0)

    await manager.acquire("job-1")
    await manager.acquire("job-2")
    await manager.acquire("job-3")

    owned = await manager.get_owned_jobs()
    assert len(owned) == 3
    assert set(owned) == {"job-1", "job-2", "job-3"}

    await manager.release("job-2")
    owned = await manager.get_owned_jobs()
    assert len(owned) == 2
    assert "job-2" not in owned


@pytest.mark.asyncio
async def test_is_owner():
    """Test ownership checking."""
    manager = LeaseManager("gate-1:9000", default_duration=30.0)

    assert not await manager.is_owner("job-123"), "Should not own unacquired job"

    await manager.acquire("job-123")
    assert await manager.is_owner("job-123"), "Should own acquired job"

    await manager.release("job-123")
    assert not await manager.is_owner("job-123"), "Should not own released job"


@pytest.mark.asyncio
async def test_force_acquire():
    """Test forced acquisition for failover scenarios."""
    manager1 = LeaseManager("gate-1:9000", default_duration=30.0)
    manager2 = LeaseManager("gate-2:9000", default_duration=30.0)

    result1 = await manager1.acquire("job-123")
    token1 = result1.lease.fence_token

    await manager2.import_lease(
        job_id="job-123",
        owner_node="gate-1:9000",
        fence_token=token1,
        expires_at=result1.lease.expires_at,
    )

    result2 = await manager2.acquire("job-123")
    assert not result2.success

    result3 = await manager2.acquire("job-123", force=True)
    assert result3.success
    assert result3.lease.fence_token > token1
    assert result3.lease.owner_node == "gate-2:9000"


@pytest.mark.asyncio
async def test_cleanup_task():
    """Test background cleanup task."""
    expired_leases: list[JobLease] = []

    def on_expired(lease: JobLease):
        expired_leases.append(lease)

    manager = LeaseManager(
        "gate-1:9000",
        default_duration=0.3,
        cleanup_interval=0.2,
        on_lease_expired=on_expired,
    )

    await manager.start_cleanup_task()

    await manager.acquire("job-123")

    await asyncio.sleep(0.6)

    await manager.stop_cleanup_task()

    assert len(expired_leases) > 0, "Should have detected expired lease"
    assert expired_leases[0].job_id == "job-123"


@pytest.mark.asyncio
async def test_concurrent_operations():
    manager = LeaseManager("gate-1:9000", default_duration=1.0)
    iterations = 100

    async def acquire_renew_release(task_id: int):
        for i in range(iterations):
            job_id = f"job-{task_id}-{i % 10}"
            await manager.acquire(job_id)
            await manager.renew(job_id)
            await manager.is_owner(job_id)
            await manager.get_fence_token(job_id)
            await manager.release(job_id)

    tasks = [asyncio.create_task(acquire_renew_release(i)) for i in range(4)]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) == 0, f"{len(errors)} concurrency errors: {errors}"
