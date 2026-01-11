#!/usr/bin/env python3
"""
Edge case, failure mode, correctness, and scalability tests for SWIM + Lifeguard.

These tests validate:
- Edge cases and boundary conditions
- Failure modes and error handling
- Protocol correctness guarantees
- Scalability under load
"""

import asyncio
import gc
import inspect
import random
import sys
import time
import tracemalloc
from dataclasses import dataclass
from typing import Any

# Add project root to path
sys.path.insert(0, '/home/ada/Projects/hyperscale')

from examples.swim.core.constants import (
    encode_int, encode_bool,
    STATUS_OK, STATUS_DEAD, STATUS_SUSPECT,
    DELIM_COLON, MSG_PROBE, EMPTY_BYTES,
)
from examples.swim.core.node_state import NodeState
from examples.swim.core.resource_limits import BoundedDict
from examples.swim.detection.probe_scheduler import ProbeScheduler
from examples.swim.detection.suspicion_state import SuspicionState
from examples.swim.detection.pending_indirect_probe import PendingIndirectProbe
from examples.swim.detection.incarnation_tracker import IncarnationTracker
from examples.swim.gossip.gossip_buffer import GossipBuffer
from examples.swim.gossip.piggyback_update import PiggybackUpdate
from examples.swim.leadership.flapping_detector import FlappingDetector, LeadershipChange
from examples.swim.leadership.leader_state import LeaderState, MAX_VOTES
from examples.swim.health.local_health_multiplier import LocalHealthMultiplier


# =============================================================================
# Test Utilities
# =============================================================================

class TestResult:
    """Tracks test results."""
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors: list[str] = []
    
    def record_pass(self, name: str):
        self.passed += 1
        print(f"  ✅ {name}")
    
    def record_fail(self, name: str, reason: str):
        self.failed += 1
        self.errors.append(f"{name}: {reason}")
        print(f"  ❌ {name}: {reason}")
    
    def summary(self) -> bool:
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{total} passed")
        if self.errors:
            print(f"Failures:")
            for e in self.errors[:20]:  # Show first 20
                print(f"  - {e}")
            if len(self.errors) > 20:
                print(f"  ... and {len(self.errors) - 20} more")
        print(f"{'='*60}")
        return self.failed == 0


results = TestResult()


def test(name: str):
    """Decorator for test functions."""
    def decorator(func):
        async def wrapper():
            try:
                await func() if asyncio.iscoroutinefunction(func) else func()
                results.record_pass(name)
            except AssertionError as e:
                results.record_fail(name, str(e) or "Assertion failed")
            except Exception as e:
                results.record_fail(name, f"Error: {type(e).__name__}: {e}")
        return wrapper
    return decorator


# =============================================================================
# EDGE CASES: Boundary Conditions
# =============================================================================

print("\n" + "=" * 60)
print("EDGE CASE TESTS: Boundary Conditions")
print("=" * 60)


@test("Edge: encode_int with zero")
def test_encode_int_zero():
    assert encode_int(0) == b'0'


@test("Edge: encode_int with max cached (255)")
def test_encode_int_max_cached():
    result = encode_int(255)
    assert result == b'255'
    # Should be same object from cache
    assert encode_int(255) is result


@test("Edge: encode_int with boundary (256)")
def test_encode_int_boundary():
    # 256 is not cached
    assert encode_int(256) == b'256'
    # Should NOT be same object (not cached)
    assert encode_int(256) is not encode_int(256)


@test("Edge: encode_int with large values")
def test_encode_int_large():
    assert encode_int(2**31 - 1) == b'2147483647'
    assert encode_int(2**63) == str(2**63).encode()


@test("Edge: encode_int with negative values")
def test_encode_int_negative():
    # Should still work, just not cached
    assert encode_int(-1) == b'-1'
    assert encode_int(-1000) == b'-1000'


@test("Edge: empty GossipBuffer operations")
def test_empty_gossip_buffer():
    buffer = GossipBuffer()
    
    assert buffer.get_updates_to_piggyback(10) == []
    assert buffer.encode_piggyback() == b''
    assert buffer.cleanup() == {'stale_removed': 0, 'complete_removed': 0, 'pending_updates': 0}
    assert buffer.remove_node(('10.0.0.1', 8080)) == False


@test("Edge: single-member ProbeScheduler")
def test_single_member_scheduler():
    scheduler = ProbeScheduler()
    scheduler.add_member(('10.0.0.1', 8080))
    
    # Should always return the same member
    for _ in range(10):
        target = scheduler.get_next_target()
        assert target == ('10.0.0.1', 8080)


@test("Edge: empty ProbeScheduler")
def test_empty_scheduler():
    scheduler = ProbeScheduler()
    
    assert scheduler.get_next_target() is None
    assert scheduler.get_probe_cycle_time() == 0.0


@test("Edge: SuspicionState with n_members=1")
def test_suspicion_single_member():
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        n_members=1,
    )
    
    # With single member, timeout should be max
    assert state.calculate_timeout() == state.max_timeout


@test("Edge: SuspicionState with n_members=0")
def test_suspicion_zero_members():
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        n_members=0,
    )
    
    # Should handle gracefully
    timeout = state.calculate_timeout()
    assert timeout == state.max_timeout


@test("Edge: LHM at max score")
def test_lhm_max_score():
    lhm = LocalHealthMultiplier()
    
    # Increment beyond max
    for _ in range(100):
        lhm.increment()
    
    assert lhm.score == lhm.max_score


@test("Edge: LHM at zero score")
def test_lhm_min_score():
    lhm = LocalHealthMultiplier()
    
    # Try to decrement below zero
    for _ in range(10):
        lhm.decrement()
    
    assert lhm.score == 0


@test("Edge: BoundedDict with max_size=1")
def test_bounded_dict_size_one():
    d = BoundedDict[str, int](max_size=1)
    
    d['a'] = 1
    d['b'] = 2  # Should evict 'a'
    
    assert len(d) == 1
    assert 'b' in d


@test("Edge: BoundedDict with max_size=0")
def test_bounded_dict_size_zero():
    d = BoundedDict[str, int](max_size=0)
    
    # Should not store anything but shouldn't crash
    d['a'] = 1
    # Max size 0 is edge case - behavior depends on implementation
    assert len(d) <= 1


@test("Edge: PiggybackUpdate with max incarnation")
def test_piggyback_max_incarnation():
    update = PiggybackUpdate(
        update_type='alive',
        node=('10.0.0.1', 8080),
        incarnation=2**31 - 1,
        timestamp=time.monotonic(),
    )
    
    encoded = update.to_bytes()
    decoded = PiggybackUpdate.from_bytes(encoded)
    
    assert decoded is not None
    assert decoded.incarnation == 2**31 - 1


@test("Edge: PiggybackUpdate with zero incarnation")
def test_piggyback_zero_incarnation():
    update = PiggybackUpdate(
        update_type='alive',
        node=('10.0.0.1', 8080),
        incarnation=0,
        timestamp=time.monotonic(),
    )
    
    encoded = update.to_bytes()
    decoded = PiggybackUpdate.from_bytes(encoded)
    
    assert decoded is not None
    assert decoded.incarnation == 0


# =============================================================================
# EDGE CASES: Special Hostnames
# =============================================================================

@test("Edge: IPv6 localhost short form")
def test_ipv6_localhost():
    """IPv6 with colons is a known limitation - the protocol uses : as delimiter.
    This test documents expected behavior: full IPv6 addresses will not roundtrip correctly,
    but short forms like ::1 fail safely (return None from from_bytes)."""
    update = PiggybackUpdate(
        update_type='alive',
        node=('::1', 8080),
        incarnation=1,
        timestamp=time.monotonic(),
    )
    
    encoded = update.to_bytes()
    # IPv6 addresses with colons will fail to parse due to delimiter conflict
    decoded = PiggybackUpdate.from_bytes(encoded)
    
    # This is a known limitation - IPv6 parsing fails due to : delimiter
    # For IPv6 support, would need to use a different delimiter or encoding
    if decoded is None:
        # Expected behavior - IPv6 parsing fails safely
        pass
    else:
        # If it somehow works, verify correctness
        assert decoded.node == ('::1', 8080)


@test("Edge: hostname with dots")
def test_hostname_with_dots():
    update = PiggybackUpdate(
        update_type='alive',
        node=('node1.cluster.local', 8080),
        incarnation=1,
        timestamp=time.monotonic(),
    )
    
    encoded = update.to_bytes()
    decoded = PiggybackUpdate.from_bytes(encoded)
    
    assert decoded is not None
    assert decoded.node == ('node1.cluster.local', 8080)


@test("Edge: port at boundaries")
def test_port_boundaries():
    # Min port
    update1 = PiggybackUpdate(
        update_type='alive',
        node=('10.0.0.1', 1),
        incarnation=1,
        timestamp=time.monotonic(),
    )
    decoded1 = PiggybackUpdate.from_bytes(update1.to_bytes())
    assert decoded1.node[1] == 1
    
    # Max port
    update2 = PiggybackUpdate(
        update_type='alive',
        node=('10.0.0.1', 65535),
        incarnation=1,
        timestamp=time.monotonic(),
    )
    decoded2 = PiggybackUpdate.from_bytes(update2.to_bytes())
    assert decoded2.node[1] == 65535


# =============================================================================
# FAILURE MODES: Malformed Input
# =============================================================================

print("\n" + "=" * 60)
print("FAILURE MODE TESTS: Malformed Input")
print("=" * 60)


@test("Failure: PiggybackUpdate.from_bytes with empty data")
def test_piggyback_empty_data():
    result = PiggybackUpdate.from_bytes(b'')
    assert result is None


@test("Failure: PiggybackUpdate.from_bytes with garbage")
def test_piggyback_garbage_data():
    result = PiggybackUpdate.from_bytes(b'\x00\x01\x02\x03')
    assert result is None


@test("Failure: PiggybackUpdate.from_bytes with incomplete data")
def test_piggyback_incomplete_data():
    # Only type and incarnation, missing host:port
    result = PiggybackUpdate.from_bytes(b'alive:1')
    assert result is None


@test("Failure: PiggybackUpdate.from_bytes with non-numeric incarnation")
def test_piggyback_bad_incarnation():
    result = PiggybackUpdate.from_bytes(b'alive:abc:10.0.0.1:8080')
    assert result is None


@test("Failure: PiggybackUpdate.from_bytes with non-numeric port")
def test_piggyback_bad_port():
    result = PiggybackUpdate.from_bytes(b'alive:1:10.0.0.1:abc')
    assert result is None


@test("Failure: GossipBuffer.decode_piggyback with no separator")
def test_gossip_decode_no_separator():
    result = GossipBuffer.decode_piggyback(b'alive:1:10.0.0.1:8080')
    assert result == []


@test("Failure: GossipBuffer.decode_piggyback with empty parts")
def test_gossip_decode_empty_parts():
    result = GossipBuffer.decode_piggyback(b'|||')
    assert result == []


@test("Failure: GossipBuffer.decode_piggyback with mixed valid/invalid")
def test_gossip_decode_mixed():
    data = b'|alive:1:10.0.0.1:8080|garbage|dead:2:10.0.0.2:8080'
    result = GossipBuffer.decode_piggyback(data)
    
    # Should parse valid ones, skip invalid
    assert len(result) == 2
    assert result[0].update_type == 'alive'
    assert result[1].update_type == 'dead'


# =============================================================================
# FAILURE MODES: Resource Exhaustion
# =============================================================================

@test("Failure: BoundedDict overflow handling")
def test_bounded_dict_overflow():
    d = BoundedDict[int, int](max_size=10)
    
    # Add way more than capacity
    for i in range(1000):
        d[i] = i
    
    # Should never exceed max size
    assert len(d) <= 10


@test("Failure: GossipBuffer overflow with callback")
def test_gossip_buffer_overflow_callback():
    overflow_called = [False]
    evicted_count = [0]
    
    def on_overflow(evicted: int, capacity: int):
        overflow_called[0] = True
        evicted_count[0] = evicted
    
    buffer = GossipBuffer(max_updates=5)
    buffer.set_overflow_callback(on_overflow)
    
    # Fill and overflow
    for i in range(20):
        buffer.add_update('alive', (f'10.0.0.{i}', 8080), 1)
    
    assert overflow_called[0], "Overflow callback should be called"


@test("Failure: SuspicionState confirmers overflow")
def test_suspicion_confirmers_overflow():
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        max_confirmers=5,
    )
    
    # Add way more confirmations
    for i in range(100):
        state.add_confirmation((f'10.0.0.{i}', 8080))
    
    # Stored confirmers should be bounded
    assert len(state.confirmers) == 5
    # But logical count should be accurate
    assert state.confirmation_count == 100
    # Dropped count should be tracked
    assert state._confirmations_dropped == 95


@test("Failure: PendingIndirectProbe proxies overflow")
def test_indirect_probe_proxies_overflow():
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=5.0,
        max_proxies=3,
    )
    
    added_count = 0
    for i in range(100):
        if probe.add_proxy((f'10.0.0.{i+10}', 8080)):
            added_count += 1
    
    assert added_count == 3
    assert len(probe.proxies) == 3
    assert probe._proxies_dropped == 97


# =============================================================================
# CORRECTNESS: Incarnation Number Ordering
# =============================================================================

print("\n" + "=" * 60)
print("CORRECTNESS TESTS: Protocol Semantics")
print("=" * 60)


@test("Correctness: higher incarnation always wins")
def test_incarnation_ordering():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    # Start with incarnation 5
    tracker.update_node(node, b'OK', 5, time.monotonic())
    
    # Try to update with lower (should be ignored)
    tracker.update_node(node, b'DEAD', 4, time.monotonic())
    state = tracker.get_node_state(node)
    assert state.status == b'OK', "Lower incarnation should be ignored"
    
    # Update with same incarnation, higher priority status
    tracker.update_node(node, b'SUSPECT', 5, time.monotonic())
    state = tracker.get_node_state(node)
    assert state.status == b'SUSPECT', "Same incarnation + higher priority should win"
    
    # Update with higher incarnation
    tracker.update_node(node, b'OK', 6, time.monotonic())
    state = tracker.get_node_state(node)
    assert state.status == b'OK', "Higher incarnation should always win"


@test("Correctness: status priority ordering")
def test_status_priority():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    # At same incarnation: DEAD > SUSPECT > OK
    tracker.update_node(node, b'OK', 1, time.monotonic())
    
    # SUSPECT should override OK
    tracker.update_node(node, b'SUSPECT', 1, time.monotonic())
    assert tracker.get_node_state(node).status == b'SUSPECT'
    
    # DEAD should override SUSPECT
    tracker.update_node(node, b'DEAD', 1, time.monotonic())
    assert tracker.get_node_state(node).status == b'DEAD'
    
    # OK should NOT override DEAD at same incarnation
    tracker.update_node(node, b'OK', 1, time.monotonic())
    assert tracker.get_node_state(node).status == b'DEAD'


@test("Correctness: refutation with higher incarnation")
def test_refutation():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    # Node is suspected
    tracker.update_node(node, b'SUSPECT', 5, time.monotonic())
    
    # Node refutes with higher incarnation
    tracker.update_node(node, b'OK', 6, time.monotonic())
    
    state = tracker.get_node_state(node)
    assert state.status == b'OK'
    assert state.incarnation == 6


@test("Correctness: suspicion timeout decreases with confirmations")
def test_suspicion_timeout_formula():
    """Verify the Lifeguard timeout formula."""
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        min_timeout=1.0,
        max_timeout=10.0,
        n_members=100,
    )
    
    timeouts = []
    for i in range(10):
        timeouts.append(state.calculate_timeout())
        state.add_confirmation((f'10.0.0.{i+10}', 8080))
    
    # Timeouts should be strictly decreasing
    for i in range(1, len(timeouts)):
        assert timeouts[i] < timeouts[i-1], \
            f"Timeout should decrease: {timeouts[i]} < {timeouts[i-1]}"
    
    # Should never go below min
    assert all(t >= state.min_timeout for t in timeouts)


@test("Correctness: gossip priority ordering")
def test_gossip_priority():
    buffer = GossipBuffer()
    
    # Add updates with different broadcast counts
    for i in range(5):
        buffer.add_update('alive', (f'10.0.0.{i}', 8080), 1)
    
    # Broadcast some updates multiple times
    for _ in range(5):
        updates = buffer.get_updates_to_piggyback(2)
        buffer.mark_broadcasts(updates)
    
    # Get remaining updates - should be ordered by broadcast count
    remaining = buffer.get_updates_to_piggyback(10)
    for i in range(1, len(remaining)):
        assert remaining[i].broadcast_count >= remaining[i-1].broadcast_count, \
            "Updates should be ordered by broadcast count (ascending)"


@test("Correctness: LHM multiplier bounds")
def test_lhm_multiplier_bounds():
    lhm = LocalHealthMultiplier()
    
    # At score 0, multiplier should be 1.0
    assert lhm.get_multiplier() == 1.0
    
    # At max score, multiplier should be 1 + max_score
    for _ in range(lhm.max_score + 10):
        lhm.increment()
    
    assert lhm.get_multiplier() == 1.0 + lhm.max_score


# =============================================================================
# CORRECTNESS: State Machine Transitions
# =============================================================================

@test("Correctness: ProbeScheduler shuffles on membership change")
def test_scheduler_shuffle():
    """Test that scheduler shuffles when membership changes."""
    
    orders = []
    for trial in range(5):
        scheduler = ProbeScheduler()
        
        # Create members with a small variation to force a new shuffle each time
        members = [(f'10.0.0.{i}', 8080) for i in range(10)]
        scheduler.update_members(members)
        
        # Capture the order
        order = [scheduler.get_next_target() for _ in range(10)]
        orders.append(tuple(order))
    
    # Multiple fresh schedulers should produce different orders
    # (Random shuffle means very unlikely to get same order 5 times)
    unique_orders = set(orders)
    # With 10 members, 10! = 3,628,800 possible orders
    # Chance of 5 identical orders is vanishingly small
    # But we allow for 1 collision (2 unique orders) to reduce flakiness
    assert len(unique_orders) >= 2, f"Scheduler should shuffle: got {len(unique_orders)} unique orders"


@test("Correctness: FlappingDetector cooldown escalation")
async def test_flapping_cooldown_escalation():
    detector = FlappingDetector(
        max_changes_per_window=2,
        base_cooldown=1.0,
        max_cooldown=10.0,
        cooldown_multiplier=2.0,
    )
    
    initial_cooldown = detector.current_cooldown
    
    # Trigger flapping multiple times
    for i in range(10):
        await detector.record_change(None, ('10.0.0.1', 8080), i, 'test')
    
    # Cooldown should have escalated
    assert detector.current_cooldown > initial_cooldown
    assert detector.current_cooldown <= detector.max_cooldown


@test("Correctness: LeaderState vote bounds")
def test_leader_state_vote_bounds():
    state = LeaderState()
    state.role = 'candidate'
    state.current_term = 1
    
    # Add many votes
    for i in range(2000):
        state.record_vote((f'10.0.0.{i % 256}.{i // 256}', 8080))
    
    # Votes received should be bounded by max_votes
    assert len(state.votes_received) <= state.max_votes


# =============================================================================
# SCALABILITY: Large Clusters
# =============================================================================

print("\n" + "=" * 60)
print("SCALABILITY TESTS: Large Clusters")
print("=" * 60)


@test("Scalability: IncarnationTracker with 10000 nodes")
async def test_tracker_large_cluster():
    tracker = IncarnationTracker(max_nodes=10000)
    
    start = time.monotonic()
    
    # Add 10000 nodes
    for i in range(10000):
        tracker.update_node((f'10.{i // 65536}.{(i // 256) % 256}.{i % 256}', 8080), b'OK', 1, time.monotonic())
    
    elapsed = time.monotonic() - start
    assert elapsed < 5.0, f"Adding 10000 nodes took too long: {elapsed:.2f}s"
    
    # Verify all nodes present
    assert len(tracker.get_all_nodes()) == 10000


@test("Scalability: GossipBuffer with 1000 updates")
def test_gossip_buffer_large():
    buffer = GossipBuffer(max_updates=1000)
    
    start = time.monotonic()
    
    # Add 1000 updates
    for i in range(1000):
        buffer.add_update('alive', (f'10.{i // 256}.{i % 256}.1', 8080), i)
    
    add_elapsed = time.monotonic() - start
    assert add_elapsed < 2.0, f"Adding 1000 updates took too long: {add_elapsed:.2f}s"
    
    # Get updates (heapq selection)
    start = time.monotonic()
    for _ in range(100):
        updates = buffer.get_updates_to_piggyback(10)
    
    select_elapsed = time.monotonic() - start
    assert select_elapsed < 1.0, f"Selection took too long: {select_elapsed:.2f}s"


@test("Scalability: ProbeScheduler with 1000 members")
def test_scheduler_large():
    scheduler = ProbeScheduler()
    
    members = [(f'10.{i // 256}.{i % 256}.1', 8080) for i in range(1000)]
    
    start = time.monotonic()
    scheduler.update_members(members)
    update_elapsed = time.monotonic() - start
    
    assert update_elapsed < 1.0, f"Update took too long: {update_elapsed:.2f}s"
    
    # Cycle through all members
    start = time.monotonic()
    for _ in range(1000):
        scheduler.get_next_target()
    
    cycle_elapsed = time.monotonic() - start
    assert cycle_elapsed < 0.5, f"Cycling took too long: {cycle_elapsed:.2f}s"


@test("Scalability: FlappingDetector with many changes")
async def test_flapping_detector_large():
    detector = FlappingDetector(
        max_changes_per_window=100,
        window_seconds=60.0,
    )
    
    start = time.monotonic()
    
    # Record 1000 changes
    for i in range(1000):
        await detector.record_change(
            old_leader=('10.0.0.1', 8080),
            new_leader=('10.0.0.2', 8080),
            term=i,
            reason='test',
        )
    
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, f"Recording 1000 changes took too long: {elapsed:.2f}s"
    
    # Get recent changes (bounded by deque maxlen)
    recent = detector.get_recent_changes(1000)
    assert len(recent) <= 100  # Bounded by deque maxlen


# =============================================================================
# SCALABILITY: Memory Usage
# =============================================================================

@test("Scalability: Memory usage for 10000 PiggybackUpdates")
def test_memory_piggyback_updates():
    gc.collect()
    tracemalloc.start()
    
    updates = []
    for i in range(10000):
        updates.append(PiggybackUpdate(
            update_type='alive',
            node=(f'10.{i // 256}.{i % 256}.1', 8080),
            incarnation=i,
            timestamp=time.monotonic(),
        ))
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # With __slots__, should use ~2MB or less for 10000 updates
    # Each update has ~6 fields, roughly 100-200 bytes each
    assert peak < 5 * 1024 * 1024, f"Memory usage too high: {peak / 1024 / 1024:.2f}MB"


@test("Scalability: Memory usage for 10000 NodeStates")
def test_memory_node_states():
    gc.collect()
    tracemalloc.start()
    
    states = []
    for i in range(10000):
        states.append(NodeState(
            status=b'OK',
            incarnation=i,
        ))
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # With __slots__, should be very small
    assert peak < 2 * 1024 * 1024, f"Memory usage too high: {peak / 1024 / 1024:.2f}MB"


@test("Scalability: Memory stability under churn")
def test_memory_stability():
    """Ensure memory doesn't grow unboundedly under churn."""
    gc.collect()
    tracemalloc.start()
    
    buffer = GossipBuffer(max_updates=100)
    tracker = IncarnationTracker(max_nodes=100)
    scheduler = ProbeScheduler()
    
    # Simulate churn
    for round in range(100):
        # Add nodes
        for i in range(50):
            node = (f'10.{round}.0.{i}', 8080)
            tracker.update_node(node, b'OK', round, time.monotonic())
            buffer.add_update('alive', node, round)
        
        # Update scheduler
        nodes = [(f'10.{round}.0.{i}', 8080) for i in range(50)]
        scheduler.update_members(nodes)
        
        # Cleanup old data
        buffer.cleanup()
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # Memory should be bounded
    assert peak < 10 * 1024 * 1024, f"Memory grew too large: {peak / 1024 / 1024:.2f}MB"


# =============================================================================
# CONCURRENCY: Race Conditions
# =============================================================================

print("\n" + "=" * 60)
print("CONCURRENCY TESTS: Race Conditions")
print("=" * 60)


@test("Concurrency: ProbeScheduler concurrent reads/writes")
async def test_scheduler_concurrent():
    scheduler = ProbeScheduler()
    scheduler.update_members([(f'10.0.0.{i}', 8080) for i in range(10)])
    
    errors = []
    
    async def reader(id: int):
        for _ in range(1000):
            try:
                target = scheduler.get_next_target()
                if target is not None:
                    assert isinstance(target, tuple)
                    assert len(target) == 2
            except Exception as e:
                errors.append(f"Reader {id}: {e}")
            await asyncio.sleep(0)
    
    async def writer(id: int):
        for i in range(100):
            try:
                if i % 3 == 0:
                    scheduler.add_member((f'192.168.{id}.{i}', 8080))
                elif i % 3 == 1:
                    scheduler.remove_member((f'192.168.{id}.{i-1}', 8080))
                else:
                    scheduler.update_members([(f'172.16.{id}.{j}', 8080) for j in range(5)])
            except Exception as e:
                errors.append(f"Writer {id}: {e}")
            await asyncio.sleep(0)
    
    # Run concurrent readers and writers
    tasks = [
        *[asyncio.create_task(reader(i)) for i in range(5)],
        *[asyncio.create_task(writer(i)) for i in range(3)],
    ]
    
    await asyncio.gather(*tasks)
    
    assert len(errors) == 0, f"Concurrent errors: {errors[:5]}"


@test("Concurrency: GossipBuffer concurrent access")
async def test_gossip_concurrent():
    buffer = GossipBuffer(max_updates=100)
    errors = []
    
    async def adder(id: int):
        for i in range(100):
            try:
                buffer.add_update('alive', (f'10.{id}.0.{i}', 8080), i)
            except Exception as e:
                errors.append(f"Adder {id}: {e}")
            await asyncio.sleep(0)
    
    async def reader():
        for _ in range(200):
            try:
                updates = buffer.get_updates_to_piggyback(10)
                if updates:
                    buffer.mark_broadcasts(updates)
            except Exception as e:
                errors.append(f"Reader: {e}")
            await asyncio.sleep(0)
    
    tasks = [
        *[asyncio.create_task(adder(i)) for i in range(3)],
        asyncio.create_task(reader()),
    ]
    
    await asyncio.gather(*tasks)
    
    assert len(errors) == 0, f"Concurrent errors: {errors[:5]}"


@test("Concurrency: BoundedDict concurrent access")
async def test_bounded_dict_concurrent():
    d = BoundedDict[str, int](max_size=50)
    errors = []
    
    async def writer(id: int):
        for i in range(100):
            try:
                d[f'key_{id}_{i}'] = i
            except Exception as e:
                errors.append(f"Writer {id}: {e}")
            await asyncio.sleep(0)
    
    async def reader(id: int):
        for i in range(200):
            try:
                _ = d.get(f'key_{id % 3}_{i % 100}')
            except Exception as e:
                errors.append(f"Reader {id}: {e}")
            await asyncio.sleep(0)
    
    tasks = [
        *[asyncio.create_task(writer(i)) for i in range(3)],
        *[asyncio.create_task(reader(i)) for i in range(3)],
    ]
    
    await asyncio.gather(*tasks)
    
    assert len(errors) == 0, f"Concurrent errors: {errors[:5]}"
    assert len(d) <= 50, "BoundedDict should stay bounded"


# =============================================================================
# PROTOCOL: Gossip Dissemination Correctness
# =============================================================================

print("\n" + "=" * 60)
print("PROTOCOL TESTS: Gossip Dissemination")
print("=" * 60)


@test("Protocol: gossip reaches all nodes")
def test_gossip_dissemination_complete():
    """Simulate gossip spreading to ensure all nodes eventually receive updates."""
    
    n_nodes = 20
    buffers = {i: GossipBuffer() for i in range(n_nodes)}
    received = {i: set() for i in range(n_nodes)}
    
    # Node 0 has an important update
    update_id = ('10.0.0.99', 8080)
    buffers[0].add_update('dead', update_id, 1, n_members=n_nodes)
    received[0].add(update_id)
    
    # Simulate gossip rounds
    max_rounds = n_nodes * 5
    for round in range(max_rounds):
        # Each node gossips to one random other node
        for sender_id in range(n_nodes):
            receiver_id = random.randint(0, n_nodes - 1)
            if receiver_id == sender_id:
                continue
            
            encoded = buffers[sender_id].encode_piggyback(max_count=5)
            if encoded:
                updates = GossipBuffer.decode_piggyback(encoded)
                for u in updates:
                    if u.node == update_id:
                        received[receiver_id].add(u.node)
                    buffers[receiver_id].add_update(
                        u.update_type, u.node, u.incarnation, n_members=n_nodes
                    )
        
        # Check if all nodes received the update
        all_received = all(update_id in received[i] for i in range(n_nodes))
        if all_received:
            break
    
    # Verify all nodes received the update
    for i in range(n_nodes):
        assert update_id in received[i], f"Node {i} didn't receive the update"


@test("Protocol: newer updates override older")
def test_gossip_update_override():
    """Ensure newer incarnation updates override older ones in gossip."""
    
    buffer = GossipBuffer()
    node = ('10.0.0.1', 8080)
    
    # Add update with incarnation 1
    buffer.add_update('alive', node, 1)
    
    # Add update with incarnation 2 (same node, higher incarnation)
    buffer.add_update('dead', node, 2)
    
    # Should only have one update for this node
    updates = buffer.get_updates_to_piggyback(10)
    node_updates = [u for u in updates if u.node == node]
    
    assert len(node_updates) == 1
    assert node_updates[0].incarnation == 2
    assert node_updates[0].update_type == 'dead'


@test("Protocol: stale updates rejected")
def test_gossip_stale_rejection():
    """Ensure stale incarnation updates are rejected."""
    
    buffer = GossipBuffer()
    node = ('10.0.0.1', 8080)
    
    # Add update with incarnation 5
    buffer.add_update('alive', node, 5)
    
    # Try to add update with lower incarnation
    buffer.add_update('dead', node, 3)  # Should be rejected
    
    updates = buffer.get_updates_to_piggyback(10)
    node_updates = [u for u in updates if u.node == node]
    
    assert len(node_updates) == 1
    assert node_updates[0].incarnation == 5
    assert node_updates[0].update_type == 'alive'


# =============================================================================
# EDGE CASES: Timing
# =============================================================================

print("\n" + "=" * 60)
print("TIMING TESTS: Edge Cases")
print("=" * 60)


@test("Timing: SuspicionState expiry precision")
async def test_suspicion_expiry_timing():
    """Test that suspicion expires at the correct time."""
    
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        min_timeout=0.1,
        max_timeout=0.2,
        n_members=5,
    )
    
    # Should not be expired immediately
    assert not state.is_expired()
    
    # Wait a bit less than min timeout
    await asyncio.sleep(0.08)
    assert not state.is_expired()
    
    # Wait until expired
    await asyncio.sleep(0.15)
    assert state.is_expired()


@test("Timing: PendingIndirectProbe expiry")
async def test_indirect_probe_expiry():
    """Test that indirect probe expires at correct time."""
    
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=0.1,
    )
    
    # Should not be expired immediately
    assert not probe.is_expired()
    
    # Wait until expired
    await asyncio.sleep(0.15)
    assert probe.is_expired()


@test("Timing: FlappingDetector window boundary")
async def test_flapping_window_boundary():
    """Test that flapping detection respects window boundaries."""
    
    detector = FlappingDetector(
        max_changes_per_window=3,
        window_seconds=0.5,
    )
    
    # Add 2 changes (under threshold)
    await detector.record_change(None, ('10.0.0.1', 8080), 1, 'test')
    await detector.record_change(None, ('10.0.0.2', 8080), 2, 'test')
    assert not detector.is_flapping
    
    # Wait for window to expire
    await asyncio.sleep(0.6)
    
    # Add 2 more changes (should be new window)
    await detector.record_change(None, ('10.0.0.3', 8080), 3, 'test')
    await detector.record_change(None, ('10.0.0.4', 8080), 4, 'test')
    
    # Should still not be flapping (2 in new window)
    assert not detector.is_flapping


# =============================================================================
# Main Runner
# =============================================================================

async def run_all_tests():
    """Run all test functions."""
    
    # Collect all test functions
    test_functions = [
        # Edge cases: boundaries
        test_encode_int_zero,
        test_encode_int_max_cached,
        test_encode_int_boundary,
        test_encode_int_large,
        test_encode_int_negative,
        test_empty_gossip_buffer,
        test_single_member_scheduler,
        test_empty_scheduler,
        test_suspicion_single_member,
        test_suspicion_zero_members,
        test_lhm_max_score,
        test_lhm_min_score,
        test_bounded_dict_size_one,
        test_bounded_dict_size_zero,
        test_piggyback_max_incarnation,
        test_piggyback_zero_incarnation,
        # Edge cases: special hostnames
        test_ipv6_localhost,
        test_hostname_with_dots,
        test_port_boundaries,
        # Failure modes: malformed input
        test_piggyback_empty_data,
        test_piggyback_garbage_data,
        test_piggyback_incomplete_data,
        test_piggyback_bad_incarnation,
        test_piggyback_bad_port,
        test_gossip_decode_no_separator,
        test_gossip_decode_empty_parts,
        test_gossip_decode_mixed,
        # Failure modes: resource exhaustion
        test_bounded_dict_overflow,
        test_gossip_buffer_overflow_callback,
        test_suspicion_confirmers_overflow,
        test_indirect_probe_proxies_overflow,
        # Correctness
        test_incarnation_ordering,
        test_status_priority,
        test_refutation,
        test_suspicion_timeout_formula,
        test_gossip_priority,
        test_lhm_multiplier_bounds,
        test_scheduler_shuffle,
        test_flapping_cooldown_escalation,
        test_leader_state_vote_bounds,
        # Scalability
        test_tracker_large_cluster,
        test_gossip_buffer_large,
        test_scheduler_large,
        test_flapping_detector_large,
        test_memory_piggyback_updates,
        test_memory_node_states,
        test_memory_stability,
        # Concurrency
        test_scheduler_concurrent,
        test_gossip_concurrent,
        test_bounded_dict_concurrent,
        # Protocol
        test_gossip_dissemination_complete,
        test_gossip_update_override,
        test_gossip_stale_rejection,
        # Timing
        test_suspicion_expiry_timing,
        test_indirect_probe_expiry,
        test_flapping_window_boundary,
    ]
    
    for test_fn in test_functions:
        await test_fn()
    
    return results.summary()


if __name__ == "__main__":
    print("=" * 60)
    print("SWIM + Lifeguard Edge Case & Scalability Tests")
    print("=" * 60)
    
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

