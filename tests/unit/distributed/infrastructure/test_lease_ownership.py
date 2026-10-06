"""
Test: Lease-Based Job Ownership

This test validates the LeaseManager implementation:
1. Lease acquisition succeeds for unclaimed job
2. Lease renewal extends expiry
3. Fence token increments on each claim

Run with: pytest tests/unit/distributed/infrastructure/test_lease_ownership.py
"""

import asyncio

import pytest

from hyperscale.distributed.leases import LeaseManager, LeaseState

# Ended leases are kept far past every scenario here: none is forgotten
# while a test still reads it.
RELEASED_RETENTION_SECONDS = 3600.0


@pytest.mark.asyncio
async def test_acquire_unclaimed():
    """Test that acquiring an unclaimed job succeeds."""
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=30.0)

    lease = await manager.acquire("job-123")

    assert lease.job_id == "job-123"
    assert lease.owner_node == "gate-1:9000"
    assert lease.fence_token == 1
    assert lease.is_active()


@pytest.mark.asyncio
async def test_acquire_already_owned():
    """Test that re-acquiring own lease just extends it."""
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=5.0)

    first_lease = await manager.acquire("job-123")
    original_token = first_lease.fence_token

    await asyncio.sleep(0.1)

    second_lease = await manager.acquire("job-123")

    assert second_lease is first_lease
    assert second_lease.fence_token == original_token, (
        "Token should not change on re-acquire"
    )
    assert second_lease.remaining_seconds() > 4.5, "Should have extended expiry"


@pytest.mark.asyncio
async def test_lease_renewal():
    """Test that lease renewal extends expiry."""
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=2.0)

    lease = await manager.acquire("job-123")
    original_expiry = lease.expires_at

    await asyncio.sleep(0.1)

    renewed = await manager.renew("job-123")

    assert renewed, "Renewal should succeed"
    assert lease.expires_at > original_expiry, "Expiry should be extended"

    other_manager = LeaseManager("gate-2:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS)
    assert not await other_manager.renew("job-123"), (
        "Should not renew lease we don't own"
    )


@pytest.mark.asyncio
async def test_fence_token_increment():
    """Test that fence tokens increment monotonically."""
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=0.2)

    tokens = []
    for i in range(5):
        lease = await manager.acquire("job-123")
        tokens.append(lease.fence_token)
        await manager.release("job-123")
        await asyncio.sleep(0.05)

    for i in range(1, len(tokens)):
        assert tokens[i] > tokens[i - 1], (
            f"Token {tokens[i]} should be > {tokens[i - 1]}"
        )


@pytest.mark.asyncio
async def test_owned_jobs():
    """Test getting list of owned jobs."""
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=30.0)

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
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=30.0)

    assert not await manager.is_owner("job-123"), "Should not own unacquired job"

    await manager.acquire("job-123")
    assert await manager.is_owner("job-123"), "Should own acquired job"

    await manager.release("job-123")
    assert not await manager.is_owner("job-123"), "Should not own released job"


@pytest.mark.asyncio
async def test_cleanup_task():
    """The background cleanup marks a lapsed lease EXPIRED."""
    manager = LeaseManager(
        "gate-1:9000",
        released_retention_seconds=RELEASED_RETENTION_SECONDS,
        default_duration=0.3,
        cleanup_interval=0.2,
    )

    cleanup = asyncio.ensure_future(manager.run_cleanup())

    lease = await manager.acquire("job-123")

    await asyncio.sleep(0.6)

    cleanup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cleanup

    assert lease.state == LeaseState.EXPIRED, "Should have expired the lapsed lease"
    assert await manager.get_lease("job-123") is None


@pytest.mark.asyncio
async def test_concurrent_operations():
    manager = LeaseManager("gate-1:9000", released_retention_seconds=RELEASED_RETENTION_SECONDS, default_duration=1.0)
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
