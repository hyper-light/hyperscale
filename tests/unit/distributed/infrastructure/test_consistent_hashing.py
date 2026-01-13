"""
Test: Consistent Hashing Ring

This test validates the ConsistentHashRing implementation:
1. Deterministic assignment: same key always maps to same node
2. Minimal redistribution: node changes affect minimal keys
3. Even distribution: keys are balanced across nodes
"""

import asyncio
import random
import statistics
import string

import pytest

from hyperscale.distributed.jobs.gates import ConsistentHashRing


def generate_job_ids(count: int) -> list[str]:
    return [
        f"job-{''.join(random.choices(string.hexdigits.lower(), k=16))}"
        for _ in range(count)
    ]


@pytest.mark.asyncio
async def test_deterministic_assignment():
    ring = ConsistentHashRing(replicas=150)
    await ring.add_node("gate-1", "127.0.0.1", 9000)
    await ring.add_node("gate-2", "127.0.0.1", 9001)
    await ring.add_node("gate-3", "127.0.0.1", 9002)

    job_ids = generate_job_ids(100)

    first_assignments = {}
    for job_id in job_ids:
        node = await ring.get_node(job_id)
        first_assignments[job_id] = node.node_id if node else None

    for _ in range(10):
        for job_id in job_ids:
            node = await ring.get_node(job_id)
            current = node.node_id if node else None
            assert current == first_assignments[job_id], (
                f"Key {job_id} mapped to {current}, expected {first_assignments[job_id]}"
            )


@pytest.mark.asyncio
async def test_minimal_redistribution():
    ring = ConsistentHashRing(replicas=150)
    await ring.add_node("gate-1", "127.0.0.1", 9000)
    await ring.add_node("gate-2", "127.0.0.1", 9001)
    await ring.add_node("gate-3", "127.0.0.1", 9002)

    job_ids = generate_job_ids(1000)

    initial_assignments = {}
    for job_id in job_ids:
        node = await ring.get_node(job_id)
        initial_assignments[job_id] = node.node_id if node else None

    await ring.add_node("gate-4", "127.0.0.1", 9003)

    redistributed = 0
    for job_id in job_ids:
        node = await ring.get_node(job_id)
        current = node.node_id if node else None
        if current != initial_assignments[job_id]:
            redistributed += 1

    redistribution_pct = redistributed / len(job_ids) * 100

    assert 10 <= redistribution_pct <= 40, (
        f"Redistribution {redistribution_pct:.1f}% outside expected range (10-40%)"
    )

    await ring.remove_node("gate-4")

    restored = 0
    for job_id in job_ids:
        node = await ring.get_node(job_id)
        current = node.node_id if node else None
        if current == initial_assignments[job_id]:
            restored += 1

    assert restored == len(job_ids), "Not all keys restored after node removal"


@pytest.mark.asyncio
async def test_even_distribution():
    ring = ConsistentHashRing(replicas=150)
    nodes = [
        ("gate-1", "127.0.0.1", 9000),
        ("gate-2", "127.0.0.1", 9001),
        ("gate-3", "127.0.0.1", 9002),
        ("gate-4", "127.0.0.1", 9003),
    ]
    for node_id, host, port in nodes:
        await ring.add_node(node_id, host, port)

    job_ids = generate_job_ids(10000)
    distribution = await ring.get_distribution(job_ids)

    counts = list(distribution.values())
    mean_count = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    cv = stdev / mean_count * 100

    assert cv < 15, f"Coefficient of variation {cv:.1f}% too high (expected < 15%)"


@pytest.mark.asyncio
async def test_empty_ring():
    ring = ConsistentHashRing(replicas=150)

    assert await ring.get_node("job-123") is None, "Empty ring should return None"
    assert await ring.node_count() == 0, "Empty ring should have length 0"
    assert not await ring.has_node("gate-1"), "Empty ring should not contain any nodes"

    await ring.add_node("gate-1", "127.0.0.1", 9000)
    node = await ring.get_node("job-123")
    assert node is not None and node.node_id == "gate-1"
    await ring.remove_node("gate-1")
    assert await ring.get_node("job-123") is None


@pytest.mark.asyncio
async def test_get_nodes_for_key():
    ring = ConsistentHashRing(replicas=150)
    await ring.add_node("gate-1", "127.0.0.1", 9000)
    await ring.add_node("gate-2", "127.0.0.1", 9001)
    await ring.add_node("gate-3", "127.0.0.1", 9002)
    await ring.add_node("gate-4", "127.0.0.1", 9003)

    job_ids = generate_job_ids(50)

    for job_id in job_ids:
        nodes = await ring.get_nodes(job_id, count=3)
        assert len(nodes) == 3, f"Expected 3 nodes, got {len(nodes)}"
        node_ids = [n.node_id for n in nodes]
        assert len(set(node_ids)) == 3, (
            f"Expected 3 distinct nodes, got duplicates: {node_ids}"
        )

    nodes = await ring.get_nodes("job-test", count=10)
    assert len(nodes) == 4, f"Expected 4 nodes (all available), got {len(nodes)}"


@pytest.mark.asyncio
async def test_node_operations():
    ring = ConsistentHashRing(replicas=150)
    expected_nodes = {"gate-1", "gate-2", "gate-3"}
    for i, node_id in enumerate(expected_nodes):
        await ring.add_node(node_id, "127.0.0.1", 9000 + i)

    all_nodes = await ring.get_all_nodes()
    all_node_ids = {n.node_id for n in all_nodes}
    assert all_node_ids == expected_nodes, f"get_all_nodes mismatch: {all_node_ids}"

    assert await ring.node_count() == 3, (
        f"Expected length 3, got {await ring.node_count()}"
    )

    assert await ring.has_node("gate-1")
    assert not await ring.has_node("gate-99")


@pytest.mark.asyncio
async def test_idempotent_operations():
    ring = ConsistentHashRing(replicas=150)

    await ring.add_node("gate-1", "127.0.0.1", 9000)
    await ring.add_node("gate-1", "127.0.0.1", 9000)
    await ring.add_node("gate-1", "127.0.0.1", 9000)
    assert await ring.node_count() == 1, "Duplicate adds should not increase node count"

    await ring.remove_node("gate-99")
    assert await ring.node_count() == 1, (
        "Removing non-existent node should not change ring"
    )

    await ring.remove_node("gate-1")
    await ring.remove_node("gate-1")
    assert await ring.node_count() == 0, "Ring should be empty after removal"


@pytest.mark.asyncio
async def test_concurrent_operations():
    ring = ConsistentHashRing(replicas=100)
    iterations = 100

    async def add_remove_nodes(task_id: int):
        for i in range(iterations):
            node_id = f"gate-{task_id}-{i % 10}"
            await ring.add_node(node_id, "127.0.0.1", 9000 + task_id)
            await ring.get_node(f"job-{task_id}-{i}")
            await ring.remove_node(node_id)

    async def lookup_keys(task_id: int):
        for i in range(iterations):
            await ring.get_node(f"job-{task_id}-{i}")
            await ring.get_nodes(f"job-{task_id}-{i}", count=2)

    tasks = []
    for i in range(4):
        tasks.append(asyncio.create_task(add_remove_nodes(i)))
        tasks.append(asyncio.create_task(lookup_keys(i + 4)))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [r for r in results if isinstance(r, Exception)]
    assert len(errors) == 0, f"{len(errors)} concurrency errors: {errors}"


@pytest.mark.asyncio
async def test_node_metadata():
    ring = ConsistentHashRing(replicas=150)
    await ring.add_node("gate-1", "10.0.0.1", 8080, weight=2)

    node = await ring.get_node("some-job")
    assert node is not None
    assert node.node_id == "gate-1"
    assert node.tcp_host == "10.0.0.1"
    assert node.tcp_port == 8080
    assert node.weight == 2

    addr = await ring.get_node_addr(node)
    assert addr == ("10.0.0.1", 8080)

    assert await ring.get_node_addr(None) is None
