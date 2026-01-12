"""
Test: Consistent Hashing Ring

This test validates the ConsistentHashRing implementation:
1. Deterministic assignment: same key always maps to same node
2. Minimal redistribution: node changes affect minimal keys
3. Backup assignment: backup is different from primary
4. Even distribution: keys are balanced across nodes

Run with: pytest tests/unit/distributed/infrastructure/test_consistent_hashing.py
"""

import random
import statistics
import string

import pytest

from hyperscale.distributed.routing import ConsistentHashRing


def generate_job_ids(count: int) -> list[str]:
    """Generate random job IDs for testing."""
    return [
        f"job-{''.join(random.choices(string.hexdigits.lower(), k=16))}"
        for _ in range(count)
    ]


@pytest.mark.asyncio
async def test_deterministic_assignment():
    """Test that the same key always maps to the same node."""
    ring = ConsistentHashRing(virtual_nodes=150)
    await ring.add_node("gate-1:9000")
    await ring.add_node("gate-2:9000")
    await ring.add_node("gate-3:9000")

    job_ids = generate_job_ids(100)

    first_assignments = {}
    for job_id in job_ids:
        first_assignments[job_id] = await ring.get_node(job_id)

    for _ in range(10):
        for job_id in job_ids:
            current = await ring.get_node(job_id)
            assert current == first_assignments[job_id], (
                f"Key {job_id} mapped to {current}, expected {first_assignments[job_id]}"
            )


@pytest.mark.asyncio
async def test_minimal_redistribution():
    """Test that adding/removing nodes causes minimal key redistribution."""
    ring = ConsistentHashRing(virtual_nodes=150)
    await ring.add_node("gate-1:9000")
    await ring.add_node("gate-2:9000")
    await ring.add_node("gate-3:9000")

    job_ids = generate_job_ids(1000)

    initial_assignments = {}
    for job_id in job_ids:
        initial_assignments[job_id] = await ring.get_node(job_id)

    await ring.add_node("gate-4:9000")

    redistributed = 0
    for job_id in job_ids:
        current = await ring.get_node(job_id)
        if current != initial_assignments[job_id]:
            redistributed += 1

    redistribution_pct = redistributed / len(job_ids) * 100

    assert 10 <= redistribution_pct <= 40, (
        f"Redistribution {redistribution_pct:.1f}% outside expected range (10-40%)"
    )

    await ring.remove_node("gate-4:9000")

    restored = 0
    for job_id in job_ids:
        current = await ring.get_node(job_id)
        if current == initial_assignments[job_id]:
            restored += 1

    assert restored == len(job_ids), "Not all keys restored after node removal"


@pytest.mark.asyncio
async def test_backup_assignment():
    """Test that backup nodes are different from primary."""
    ring = ConsistentHashRing(virtual_nodes=150)
    await ring.add_node("gate-1:9000")
    await ring.add_node("gate-2:9000")
    await ring.add_node("gate-3:9000")

    job_ids = generate_job_ids(100)

    for job_id in job_ids:
        primary = await ring.get_node(job_id)
        backup = await ring.get_backup(job_id)

        assert primary is not None, f"Primary is None for {job_id}"
        assert backup is not None, f"Backup is None for {job_id}"
        assert primary != backup, f"Primary {primary} == Backup {backup} for {job_id}"

    single_ring = ConsistentHashRing(virtual_nodes=150)
    await single_ring.add_node("gate-1:9000")

    for job_id in job_ids[:10]:
        primary = await single_ring.get_node(job_id)
        backup = await single_ring.get_backup(job_id)
        assert primary is not None, "Single node ring should have primary"
        assert backup is None, "Single node ring should have no backup"


@pytest.mark.asyncio
async def test_even_distribution():
    """Test that keys are evenly distributed across nodes."""
    ring = ConsistentHashRing(virtual_nodes=150)
    nodes = ["gate-1:9000", "gate-2:9000", "gate-3:9000", "gate-4:9000"]
    for node in nodes:
        await ring.add_node(node)

    job_ids = generate_job_ids(10000)
    distribution = await ring.key_distribution(job_ids)

    counts = list(distribution.values())
    mean_count = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    cv = stdev / mean_count * 100

    assert cv < 15, f"Coefficient of variation {cv:.1f}% too high (expected < 15%)"


@pytest.mark.asyncio
async def test_empty_ring():
    """Test behavior with empty ring."""
    ring = ConsistentHashRing(virtual_nodes=150)

    assert await ring.get_node("job-123") is None, "Empty ring should return None"
    assert await ring.get_backup("job-123") is None, (
        "Empty ring should return None for backup"
    )
    assert await ring.node_count() == 0, "Empty ring should have length 0"
    assert not await ring.contains("gate-1:9000"), (
        "Empty ring should not contain any nodes"
    )

    await ring.add_node("gate-1:9000")
    assert await ring.get_node("job-123") == "gate-1:9000"
    await ring.remove_node("gate-1:9000")
    assert await ring.get_node("job-123") is None


@pytest.mark.asyncio
async def test_get_nodes_for_key():
    """Test getting multiple nodes for replication."""
    ring = ConsistentHashRing(virtual_nodes=150)
    await ring.add_node("gate-1:9000")
    await ring.add_node("gate-2:9000")
    await ring.add_node("gate-3:9000")
    await ring.add_node("gate-4:9000")

    job_ids = generate_job_ids(50)

    for job_id in job_ids:
        nodes = await ring.get_nodes_for_key(job_id, count=3)
        assert len(nodes) == 3, f"Expected 3 nodes, got {len(nodes)}"
        assert len(set(nodes)) == 3, (
            f"Expected 3 distinct nodes, got duplicates: {nodes}"
        )

    nodes = await ring.get_nodes_for_key("job-test", count=10)
    assert len(nodes) == 4, f"Expected 4 nodes (all available), got {len(nodes)}"


@pytest.mark.asyncio
async def test_node_iteration():
    """Test iterating over nodes."""
    ring = ConsistentHashRing(virtual_nodes=150)
    expected_nodes = {"gate-1:9000", "gate-2:9000", "gate-3:9000"}
    for node in expected_nodes:
        await ring.add_node(node)

    iterated_nodes = set(await ring.get_nodes_iter())
    assert iterated_nodes == expected_nodes, f"Iteration mismatch: {iterated_nodes}"

    all_nodes = set(await ring.get_all_nodes())
    assert all_nodes == expected_nodes, f"get_all_nodes mismatch: {all_nodes}"

    assert await ring.node_count() == 3, (
        f"Expected length 3, got {await ring.node_count()}"
    )

    assert await ring.contains("gate-1:9000")
    assert not await ring.contains("gate-99:9000")


@pytest.mark.asyncio
async def test_idempotent_operations():
    """Test that add/remove are idempotent."""
    ring = ConsistentHashRing(virtual_nodes=150)

    await ring.add_node("gate-1:9000")
    await ring.add_node("gate-1:9000")
    await ring.add_node("gate-1:9000")
    assert await ring.node_count() == 1, "Duplicate adds should not increase node count"

    await ring.remove_node("gate-99:9000")
    assert await ring.node_count() == 1, (
        "Removing non-existent node should not change ring"
    )

    await ring.remove_node("gate-1:9000")
    await ring.remove_node("gate-1:9000")
    assert await ring.node_count() == 0, "Ring should be empty after removal"


@pytest.mark.asyncio
async def test_thread_safety():
    """Test thread safety with concurrent operations."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    ring = ConsistentHashRing(virtual_nodes=100)
    errors: list[str] = []
    iterations = 1000
    loop = asyncio.get_event_loop()

    def add_remove_nodes(thread_id: int):
        async def work():
            for i in range(iterations):
                node_id = f"gate-{thread_id}-{i % 10}:9000"
                await ring.add_node(node_id)
                await ring.get_node(f"job-{thread_id}-{i}")
                await ring.remove_node(node_id)

        try:
            asyncio.run(work())
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    def lookup_keys(thread_id: int):
        async def work():
            for i in range(iterations):
                await ring.get_node(f"job-{thread_id}-{i}")
                await ring.get_backup(f"job-{thread_id}-{i}")
                await ring.get_nodes_for_key(f"job-{thread_id}-{i}", count=2)

        try:
            asyncio.run(work())
        except Exception as e:
            errors.append(f"Lookup thread {thread_id}: {e}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for i in range(4):
            futures.append(executor.submit(add_remove_nodes, i))
            futures.append(executor.submit(lookup_keys, i + 4))

        for f in futures:
            f.result()

    assert len(errors) == 0, f"{len(errors)} thread safety errors: {errors}"
