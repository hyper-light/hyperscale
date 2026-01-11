"""
Test: Consistent Hashing Ring

This test validates the ConsistentHashRing implementation:
1. Deterministic assignment: same key always maps to same node
2. Minimal redistribution: node changes affect minimal keys
3. Backup assignment: backup is different from primary
4. Even distribution: keys are balanced across nodes
5. Thread safety: concurrent operations don't corrupt state

Run with: python examples/servers/test_consistent_hashing.py
"""

import asyncio
import random
import statistics
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from hyperscale.distributed.routing import ConsistentHashRing


def generate_job_ids(count: int) -> list[str]:
    """Generate random job IDs for testing."""
    return [
        f"job-{''.join(random.choices(string.hexdigits.lower(), k=16))}"
        for _ in range(count)
    ]


def test_deterministic_assignment():
    """Test that the same key always maps to the same node."""
    print("\n[Test 1] Deterministic Assignment")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_node("gate-1:9000")
    ring.add_node("gate-2:9000")
    ring.add_node("gate-3:9000")

    job_ids = generate_job_ids(100)

    # First assignment
    first_assignments = {job_id: ring.get_node(job_id) for job_id in job_ids}

    # Verify same assignments on subsequent lookups
    for _ in range(10):
        for job_id in job_ids:
            current = ring.get_node(job_id)
            assert current == first_assignments[job_id], (
                f"Key {job_id} mapped to {current}, expected {first_assignments[job_id]}"
            )

    print("  ✓ All 100 keys map to same nodes across 10 iterations")


def test_minimal_redistribution():
    """Test that adding/removing nodes causes minimal key redistribution."""
    print("\n[Test 2] Minimal Redistribution")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_node("gate-1:9000")
    ring.add_node("gate-2:9000")
    ring.add_node("gate-3:9000")

    job_ids = generate_job_ids(1000)

    # Record initial assignments
    initial_assignments = {job_id: ring.get_node(job_id) for job_id in job_ids}

    # Add a new node
    ring.add_node("gate-4:9000")

    # Count redistributed keys
    redistributed = sum(
        1 for job_id in job_ids if ring.get_node(job_id) != initial_assignments[job_id]
    )

    # With consistent hashing, ~25% of keys should move to new node (1/4 of ring)
    # Allow some variance: 15-35%
    redistribution_pct = redistributed / len(job_ids) * 100
    print(f"  Keys redistributed after adding node: {redistributed}/{len(job_ids)} ({redistribution_pct:.1f}%)")

    # Ideal is 25% (1/N where N=4), allow 10-40% range
    assert 10 <= redistribution_pct <= 40, (
        f"Redistribution {redistribution_pct:.1f}% outside expected range (10-40%)"
    )
    print("  ✓ Redistribution within expected range")

    # Remove the new node
    ring.remove_node("gate-4:9000")

    # All keys should return to original assignments
    restored = sum(
        1 for job_id in job_ids if ring.get_node(job_id) == initial_assignments[job_id]
    )
    print(f"  Keys restored after removing node: {restored}/{len(job_ids)}")
    assert restored == len(job_ids), "Not all keys restored after node removal"
    print("  ✓ All keys restored to original nodes")


def test_backup_assignment():
    """Test that backup nodes are different from primary."""
    print("\n[Test 3] Backup Assignment")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_node("gate-1:9000")
    ring.add_node("gate-2:9000")
    ring.add_node("gate-3:9000")

    job_ids = generate_job_ids(100)

    for job_id in job_ids:
        primary = ring.get_node(job_id)
        backup = ring.get_backup(job_id)

        assert primary is not None, f"Primary is None for {job_id}"
        assert backup is not None, f"Backup is None for {job_id}"
        assert primary != backup, f"Primary {primary} == Backup {backup} for {job_id}"

    print("  ✓ All 100 keys have distinct primary and backup nodes")

    # Test with only one node (no backup available)
    single_ring = ConsistentHashRing(virtual_nodes=150)
    single_ring.add_node("gate-1:9000")

    for job_id in job_ids[:10]:
        primary = single_ring.get_node(job_id)
        backup = single_ring.get_backup(job_id)
        assert primary is not None, "Single node ring should have primary"
        assert backup is None, "Single node ring should have no backup"

    print("  ✓ Single-node ring correctly returns None for backup")


def test_even_distribution():
    """Test that keys are evenly distributed across nodes."""
    print("\n[Test 4] Even Distribution")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)
    nodes = ["gate-1:9000", "gate-2:9000", "gate-3:9000", "gate-4:9000"]
    for node in nodes:
        ring.add_node(node)

    job_ids = generate_job_ids(10000)
    distribution = ring.key_distribution(job_ids)

    print(f"  Distribution across {len(nodes)} nodes:")
    for node, count in sorted(distribution.items()):
        pct = count / len(job_ids) * 100
        print(f"    {node}: {count} keys ({pct:.1f}%)")

    # Calculate standard deviation
    counts = list(distribution.values())
    mean_count = statistics.mean(counts)
    stdev = statistics.stdev(counts)
    cv = stdev / mean_count * 100  # Coefficient of variation

    print(f"  Mean: {mean_count:.1f}, StdDev: {stdev:.1f}, CV: {cv:.1f}%")

    # With 150 vnodes and 4 nodes, CV should be < 10%
    assert cv < 15, f"Coefficient of variation {cv:.1f}% too high (expected < 15%)"
    print("  ✓ Distribution is even (CV < 15%)")


def test_empty_ring():
    """Test behavior with empty ring."""
    print("\n[Test 5] Empty Ring Handling")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)

    assert ring.get_node("job-123") is None, "Empty ring should return None"
    assert ring.get_backup("job-123") is None, "Empty ring should return None for backup"
    assert len(ring) == 0, "Empty ring should have length 0"
    assert "gate-1:9000" not in ring, "Empty ring should not contain any nodes"

    print("  ✓ Empty ring returns None for all lookups")

    # Add and remove node
    ring.add_node("gate-1:9000")
    assert ring.get_node("job-123") == "gate-1:9000"
    ring.remove_node("gate-1:9000")
    assert ring.get_node("job-123") is None

    print("  ✓ Ring correctly handles add/remove cycle")


def test_get_nodes_for_key():
    """Test getting multiple nodes for replication."""
    print("\n[Test 6] Multi-Node Assignment (Replication)")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_node("gate-1:9000")
    ring.add_node("gate-2:9000")
    ring.add_node("gate-3:9000")
    ring.add_node("gate-4:9000")

    job_ids = generate_job_ids(50)

    for job_id in job_ids:
        nodes = ring.get_nodes_for_key(job_id, count=3)
        assert len(nodes) == 3, f"Expected 3 nodes, got {len(nodes)}"
        assert len(set(nodes)) == 3, f"Expected 3 distinct nodes, got duplicates: {nodes}"

    print("  ✓ All keys get 3 distinct nodes for replication")

    # Test requesting more nodes than available
    nodes = ring.get_nodes_for_key("job-test", count=10)
    assert len(nodes) == 4, f"Expected 4 nodes (all available), got {len(nodes)}"
    print("  ✓ Correctly limits to available nodes")


def test_thread_safety():
    """Test thread safety with concurrent operations."""
    print("\n[Test 7] Thread Safety")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=100)
    errors: list[str] = []
    iterations = 1000

    def add_remove_nodes(thread_id: int):
        """Repeatedly add and remove nodes."""
        try:
            for i in range(iterations):
                node_id = f"gate-{thread_id}-{i % 10}:9000"
                ring.add_node(node_id)
                ring.get_node(f"job-{thread_id}-{i}")
                ring.remove_node(node_id)
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    def lookup_keys(thread_id: int):
        """Repeatedly look up keys."""
        try:
            for i in range(iterations):
                ring.get_node(f"job-{thread_id}-{i}")
                ring.get_backup(f"job-{thread_id}-{i}")
                ring.get_nodes_for_key(f"job-{thread_id}-{i}", count=2)
        except Exception as e:
            errors.append(f"Lookup thread {thread_id}: {e}")

    # Run concurrent operations
    with ThreadPoolExecutor(max_workers=8) as executor:
        # 4 threads adding/removing, 4 threads looking up
        futures = []
        for i in range(4):
            futures.append(executor.submit(add_remove_nodes, i))
            futures.append(executor.submit(lookup_keys, i + 4))

        for f in futures:
            f.result()

    if errors:
        for error in errors:
            print(f"  ✗ {error}")
        raise AssertionError(f"{len(errors)} thread safety errors")

    print(f"  ✓ {iterations * 8} concurrent operations completed without errors")


def test_node_iteration():
    """Test iterating over nodes."""
    print("\n[Test 8] Node Iteration")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)
    expected_nodes = {"gate-1:9000", "gate-2:9000", "gate-3:9000"}
    for node in expected_nodes:
        ring.add_node(node)

    # Test __iter__
    iterated_nodes = set(ring)
    assert iterated_nodes == expected_nodes, f"Iteration mismatch: {iterated_nodes}"
    print("  ✓ Iteration returns all nodes")

    # Test get_all_nodes
    all_nodes = set(ring.get_all_nodes())
    assert all_nodes == expected_nodes, f"get_all_nodes mismatch: {all_nodes}"
    print("  ✓ get_all_nodes returns all nodes")

    # Test __len__
    assert len(ring) == 3, f"Expected length 3, got {len(ring)}"
    print("  ✓ Length is correct")

    # Test __contains__
    assert "gate-1:9000" in ring
    assert "gate-99:9000" not in ring
    print("  ✓ Containment check works")


def test_idempotent_operations():
    """Test that add/remove are idempotent."""
    print("\n[Test 9] Idempotent Operations")
    print("-" * 50)

    ring = ConsistentHashRing(virtual_nodes=150)

    # Adding same node multiple times should be idempotent
    ring.add_node("gate-1:9000")
    ring.add_node("gate-1:9000")
    ring.add_node("gate-1:9000")
    assert len(ring) == 1, "Duplicate adds should not increase node count"
    print("  ✓ Duplicate add_node is idempotent")

    # Removing non-existent node should be no-op
    ring.remove_node("gate-99:9000")
    assert len(ring) == 1, "Removing non-existent node should not change ring"
    print("  ✓ Removing non-existent node is no-op")

    # Removing same node multiple times should be idempotent
    ring.remove_node("gate-1:9000")
    ring.remove_node("gate-1:9000")
    assert len(ring) == 0, "Ring should be empty after removal"
    print("  ✓ Duplicate remove_node is idempotent")


async def main():
    """Run all consistent hashing tests."""
    print("=" * 60)
    print("CONSISTENT HASHING RING TEST")
    print("=" * 60)

    start_time = time.monotonic()

    try:
        test_deterministic_assignment()
        test_minimal_redistribution()
        test_backup_assignment()
        test_even_distribution()
        test_empty_ring()
        test_get_nodes_for_key()
        test_thread_safety()
        test_node_iteration()
        test_idempotent_operations()

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
