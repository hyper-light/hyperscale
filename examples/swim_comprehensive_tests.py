#!/usr/bin/env python3
"""
Comprehensive tests for SWIM + Lifeguard protocol behaviors.

Tests critical scenarios including:
- Node rejoining after failure
- Suspicion and eviction
- Incarnation number handling
- Gossip buffer operations
- Message deduplication
- Rate limiting
- Leadership election
- Probe scheduling
"""

import asyncio
import sys
import time
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, '/home/ada/Projects/hyperscale')

from examples.swim.core.constants import (
    encode_int, encode_bool,
    STATUS_OK, STATUS_DEAD, STATUS_SUSPECT,
    UPDATE_ALIVE, UPDATE_DEAD, UPDATE_SUSPECT,
    DELIM_COLON, MSG_PROBE,
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
            for e in self.errors:
                print(f"  - {e}")
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
                results.record_fail(name, str(e))
            except Exception as e:
                results.record_fail(name, f"Error: {type(e).__name__}: {e}")
        return wrapper
    return decorator


# =============================================================================
# Constants Tests
# =============================================================================

@test("encode_int: small values use cache")
def test_encode_int_small():
    assert encode_int(0) == b'0'
    assert encode_int(1) == b'1'
    assert encode_int(255) == b'255'
    # Same object from cache
    assert encode_int(42) is encode_int(42)


@test("encode_int: large values encoded fresh")
def test_encode_int_large():
    assert encode_int(256) == b'256'
    assert encode_int(12345) == b'12345'


@test("encode_bool: correct encoding")
def test_encode_bool():
    assert encode_bool(True) == b'1'
    assert encode_bool(False) == b'0'


@test("constants: pre-allocated bytes are correct")
def test_constants():
    assert STATUS_OK == b'OK'
    assert STATUS_DEAD == b'DEAD'
    assert DELIM_COLON == b':'
    assert MSG_PROBE == b'PROBE:'


# =============================================================================
# NodeState Tests
# =============================================================================

@test("NodeState: default values")
def test_node_state_default():
    state = NodeState()
    assert state.status == b'OK'
    assert state.incarnation == 0


@test("NodeState: uses __slots__")
def test_node_state_slots():
    state = NodeState()
    assert hasattr(state, '__slots__')
    assert not hasattr(state, '__dict__')


# =============================================================================
# PiggybackUpdate Tests
# =============================================================================

@test("PiggybackUpdate: serialization roundtrip")
def test_piggyback_roundtrip():
    update = PiggybackUpdate(
        update_type='alive',
        node=('192.168.1.1', 8080),
        incarnation=42,
        timestamp=time.monotonic(),
    )
    encoded = update.to_bytes()
    decoded = PiggybackUpdate.from_bytes(encoded)
    
    assert decoded is not None
    assert decoded.update_type == 'alive'
    assert decoded.node == ('192.168.1.1', 8080)
    assert decoded.incarnation == 42


@test("PiggybackUpdate: uses __slots__")
def test_piggyback_slots():
    update = PiggybackUpdate(
        update_type='alive',
        node=('127.0.0.1', 8080),
        incarnation=0,
        timestamp=time.monotonic(),
    )
    assert hasattr(type(update), '__slots__')


@test("PiggybackUpdate: broadcast counting")
def test_piggyback_broadcast_count():
    update = PiggybackUpdate(
        update_type='dead',
        node=('10.0.0.1', 8080),
        incarnation=1,
        timestamp=time.monotonic(),
        max_broadcasts=3,
    )
    
    assert update.should_broadcast()
    update.mark_broadcast()
    assert update.broadcast_count == 1
    assert update.should_broadcast()
    update.mark_broadcast()
    update.mark_broadcast()
    assert not update.should_broadcast()


# =============================================================================
# GossipBuffer Tests
# =============================================================================

@test("GossipBuffer: add and retrieve updates")
def test_gossip_buffer_basic():
    buffer = GossipBuffer()
    
    buffer.add_update('alive', ('10.0.0.1', 8080), 1)
    buffer.add_update('suspect', ('10.0.0.2', 8080), 2)
    
    updates = buffer.get_updates_to_piggyback(10)
    assert len(updates) == 2


@test("GossipBuffer: higher incarnation replaces")
def test_gossip_buffer_incarnation():
    buffer = GossipBuffer()
    
    buffer.add_update('alive', ('10.0.0.1', 8080), 1)
    buffer.add_update('suspect', ('10.0.0.1', 8080), 2)  # Higher incarnation
    
    updates = buffer.get_updates_to_piggyback(10)
    assert len(updates) == 1
    assert updates[0].update_type == 'suspect'
    assert updates[0].incarnation == 2


@test("GossipBuffer: prioritizes by broadcast count (heapq)")
def test_gossip_buffer_priority():
    buffer = GossipBuffer()
    
    # Add updates and broadcast some
    buffer.add_update('alive', ('10.0.0.1', 8080), 1)
    buffer.add_update('alive', ('10.0.0.2', 8080), 1)
    buffer.add_update('alive', ('10.0.0.3', 8080), 1)
    
    # Mark first one as broadcast multiple times
    updates = buffer.get_updates_to_piggyback(1)
    buffer.mark_broadcasts(updates)
    buffer.mark_broadcasts(updates)
    
    # Now get updates - first one should be last (highest broadcast count)
    all_updates = buffer.get_updates_to_piggyback(10)
    # The one we broadcast should have higher count
    broadcast_counts = [u.broadcast_count for u in all_updates]
    assert broadcast_counts == sorted(broadcast_counts)  # Should be sorted ascending


@test("GossipBuffer: evicts oldest when at capacity")
def test_gossip_buffer_eviction():
    buffer = GossipBuffer(max_updates=5)
    
    # Add 5 updates
    for i in range(5):
        buffer.add_update('alive', (f'10.0.0.{i}', 8080), 1)
    
    assert len(buffer.updates) == 5
    
    # Add one more - should trigger eviction or cleanup
    buffer.add_update('alive', ('10.0.0.99', 8080), 1)
    
    # Buffer should stay at max size
    assert len(buffer.updates) <= 5


@test("GossipBuffer: encode respects size limits")
def test_gossip_buffer_size_limit():
    buffer = GossipBuffer(max_piggyback_size=50)
    
    # Add many updates
    for i in range(20):
        buffer.add_update('alive', (f'192.168.1.{i}', 8080), i)
    
    encoded = buffer.encode_piggyback(max_count=20)
    
    # Should fit within size limit
    assert len(encoded) <= 50


@test("GossipBuffer: decode limits updates")
def test_gossip_buffer_decode_limit():
    # Create encoded data with many updates
    data = b'|alive:1:10.0.0.1:8080|dead:2:10.0.0.2:8080|suspect:3:10.0.0.3:8080'
    
    # Decode with limit
    updates = GossipBuffer.decode_piggyback(data, max_updates=2)
    assert len(updates) == 2


# =============================================================================
# SuspicionState Tests
# =============================================================================

@test("SuspicionState: timeout decreases with confirmations")
def test_suspicion_timeout():
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        min_timeout=1.0,
        max_timeout=10.0,
        n_members=10,
    )
    
    initial_timeout = state.calculate_timeout()
    
    # Add confirmations
    state.add_confirmation(('10.0.0.2', 8080))
    state.add_confirmation(('10.0.0.3', 8080))
    
    new_timeout = state.calculate_timeout()
    
    # Timeout should decrease
    assert new_timeout < initial_timeout


@test("SuspicionState: confirmation counting with bounds")
def test_suspicion_bounded_confirmers():
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
        max_confirmers=3,
    )
    
    # Add confirmations
    for i in range(10):
        state.add_confirmation((f'10.0.0.{i}', 8080))
    
    # Only 3 stored, but all counted
    assert len(state.confirmers) == 3
    assert state.confirmation_count == 10


@test("SuspicionState: uses __slots__")
def test_suspicion_slots():
    state = SuspicionState(
        node=('10.0.0.1', 8080),
        incarnation=1,
        start_time=time.monotonic(),
    )
    assert hasattr(type(state), '__slots__')


# =============================================================================
# PendingIndirectProbe Tests
# =============================================================================

@test("PendingIndirectProbe: proxy bounds")
def test_indirect_probe_bounds():
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=5.0,
        max_proxies=3,
    )
    
    # Add proxies
    assert probe.add_proxy(('10.0.0.3', 8080))
    assert probe.add_proxy(('10.0.0.4', 8080))
    assert probe.add_proxy(('10.0.0.5', 8080))
    assert not probe.add_proxy(('10.0.0.6', 8080))  # At limit
    
    assert len(probe.proxies) == 3
    assert probe._proxies_dropped == 1


@test("PendingIndirectProbe: uses __slots__")
def test_indirect_probe_slots():
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=5.0,
    )
    assert hasattr(type(probe), '__slots__')


# =============================================================================
# ProbeScheduler Tests (Lockless Copy-on-Write)
# =============================================================================

@test("ProbeScheduler: lockless add/remove")
def test_probe_scheduler_lockless():
    scheduler = ProbeScheduler()
    
    # Add members
    scheduler.add_member(('10.0.0.1', 8080))
    scheduler.add_member(('10.0.0.2', 8080))
    
    assert len(scheduler.members) == 2
    
    # Remove member
    scheduler.remove_member(('10.0.0.1', 8080))
    assert len(scheduler.members) == 1


@test("ProbeScheduler: round-robin cycling")
def test_probe_scheduler_roundrobin():
    scheduler = ProbeScheduler()
    scheduler.update_members([
        ('10.0.0.1', 8080),
        ('10.0.0.2', 8080),
        ('10.0.0.3', 8080),
    ])
    
    seen = set()
    for _ in range(10):
        target = scheduler.get_next_target()
        if target:
            seen.add(target)
    
    # Should see all members
    assert len(seen) == 3


@test("ProbeScheduler: stats tracking")
def test_probe_scheduler_stats():
    scheduler = ProbeScheduler()
    scheduler.update_members([('10.0.0.1', 8080)])
    
    stats = scheduler.get_stats()
    assert stats['member_count'] == 1
    assert stats['reshuffles'] == 1


# =============================================================================
# IncarnationTracker Tests
# =============================================================================

@test("IncarnationTracker: tracks incarnations")
def test_incarnation_tracker_basic():
    tracker = IncarnationTracker()
    
    tracker.update_node(('10.0.0.1', 8080), b'OK', 1, time.monotonic())
    tracker.update_node(('10.0.0.1', 8080), b'OK', 2, time.monotonic())
    
    # Should have the higher incarnation
    state = tracker.get_node_state(('10.0.0.1', 8080))
    assert state is not None
    assert state.incarnation == 2


@test("IncarnationTracker: rejects stale incarnations")
def test_incarnation_tracker_stale():
    tracker = IncarnationTracker()
    
    tracker.update_node(('10.0.0.1', 8080), b'OK', 5, time.monotonic())
    tracker.update_node(('10.0.0.1', 8080), b'SUSPECT', 3, time.monotonic())  # Stale
    
    # Should still be OK with incarnation 5
    state = tracker.get_node_state(('10.0.0.1', 8080))
    assert state.incarnation == 5
    assert state.status == b'OK'


@test("IncarnationTracker: bounded size")
async def test_incarnation_tracker_bounded():
    tracker = IncarnationTracker(max_nodes=5)
    
    # Add 10 nodes
    for i in range(10):
        tracker.update_node((f'10.0.0.{i}', 8080), b'OK', 1, time.monotonic())
    
    # Run cleanup to trigger eviction
    await tracker.cleanup()
    
    # Should be bounded
    assert len(tracker.get_all_nodes()) <= 5


# =============================================================================
# FlappingDetector Tests
# =============================================================================

@test("FlappingDetector: detects flapping")
async def test_flapping_detection():
    detector = FlappingDetector(
        max_changes_per_window=3,
        window_seconds=60.0,
    )
    
    # Record changes
    for i in range(5):
        triggered = await detector.record_change(
            old_leader=('10.0.0.1', 8080),
            new_leader=('10.0.0.2', 8080),
            term=i,
            reason='test',
        )
    
    assert detector.is_flapping


@test("FlappingDetector: escalates cooldown")
async def test_flapping_cooldown():
    detector = FlappingDetector(
        max_changes_per_window=2,
        base_cooldown=1.0,
        cooldown_multiplier=2.0,
    )
    
    initial = detector.current_cooldown
    
    # Trigger flapping
    for i in range(5):
        await detector.record_change(None, ('10.0.0.1', 8080), i, 'test')
    
    # Cooldown should have increased
    assert detector.current_cooldown > initial


@test("LeadershipChange: uses __slots__")
def test_leadership_change_slots():
    change = LeadershipChange(
        timestamp=time.monotonic(),
        old_leader=None,
        new_leader=('10.0.0.1', 8080),
        term=1,
        reason='test',
    )
    assert hasattr(type(change), '__slots__')


# =============================================================================
# LocalHealthMultiplier Tests
# =============================================================================

@test("LHM: degradation on nack")
def test_lhm_degradation():
    lhm = LocalHealthMultiplier()
    
    initial = lhm.score
    lhm.on_missed_nack()
    
    assert lhm.score > initial


@test("LHM: recovery on success")
def test_lhm_recovery():
    lhm = LocalHealthMultiplier()
    
    # Degrade
    for _ in range(5):
        lhm.on_missed_nack()
    
    degraded = lhm.score
    
    # Recover
    for _ in range(3):
        lhm.on_successful_probe()
    
    assert lhm.score < degraded


@test("LHM: multiplier calculation")
def test_lhm_multiplier():
    lhm = LocalHealthMultiplier()
    
    # At score 0, multiplier is 1.0
    assert lhm.get_multiplier() == 1.0
    
    # Degrade
    for _ in range(4):
        lhm.on_missed_nack()
    
    # Multiplier should increase
    assert lhm.get_multiplier() > 1.0


# =============================================================================
# BoundedDict Tests
# =============================================================================

@test("BoundedDict: respects max size")
def test_bounded_dict_size():
    d = BoundedDict[str, str](max_size=5)
    
    for i in range(10):
        d[f'key{i}'] = f'value{i}'
    
    assert len(d) <= 5


@test("BoundedDict: LRU eviction")
def test_bounded_dict_lru():
    d = BoundedDict[str, int](max_size=3, eviction_policy='LRU')
    
    d['a'] = 1
    d['b'] = 2
    d['c'] = 3
    
    # Access 'a' to make it recently used
    _ = d['a']
    
    # Add new item - should evict something
    d['d'] = 4
    
    # Should still be at max size
    assert len(d) <= 3
    assert 'd' in d  # Just added


@test("BoundedDict: OLDEST eviction")
def test_bounded_dict_oldest():
    d = BoundedDict[str, int](max_size=3, eviction_policy='OLDEST')
    
    d['a'] = 1
    d['b'] = 2
    d['c'] = 3
    d['d'] = 4  # Should evict oldest
    
    # Should still be at max size
    assert len(d) <= 3
    assert 'd' in d  # Just added


# =============================================================================
# Integration: Rejoin Scenario
# =============================================================================

@test("Integration: node rejoin clears stale state")
def test_node_rejoin():
    """Simulate a node leaving and rejoining."""
    tracker = IncarnationTracker()
    buffer = GossipBuffer()
    
    node = ('10.0.0.1', 8080)
    
    # Node joins
    tracker.update_node(node, b'OK', 1, time.monotonic())
    buffer.add_update('alive', node, 1)
    
    # Node dies
    tracker.update_node(node, b'DEAD', 1, time.monotonic())
    buffer.add_update('dead', node, 1)
    
    # Node rejoins with higher incarnation
    tracker.update_node(node, b'OK', 2, time.monotonic())
    buffer.remove_node(node)  # Clear stale updates
    buffer.add_update('alive', node, 2)
    
    # Verify state
    state = tracker.get_node_state(node)
    assert state.status == b'OK'
    assert state.incarnation == 2
    
    updates = buffer.get_updates_to_piggyback(10)
    assert len(updates) == 1
    assert updates[0].update_type == 'alive'
    assert updates[0].incarnation == 2


# =============================================================================
# Integration: Suspicion -> Dead Flow
# =============================================================================

@test("Integration: suspicion to dead transition")
async def test_suspicion_to_dead():
    """Simulate a node being suspected and declared dead."""
    tracker = IncarnationTracker()
    
    node = ('10.0.0.1', 8080)
    
    # Node is healthy
    tracker.update_node(node, b'OK', 1, time.monotonic())
    
    # Node becomes suspect
    tracker.update_node(node, b'SUSPECT', 1, time.monotonic())
    
    # Create suspicion state
    suspicion = SuspicionState(
        node=node,
        incarnation=1,
        start_time=time.monotonic(),
        min_timeout=0.1,
        max_timeout=0.5,
        n_members=5,
    )
    
    # Add confirmations
    for i in range(3):
        suspicion.add_confirmation((f'10.0.0.{i+10}', 8080))
    
    # Wait for timeout
    await asyncio.sleep(0.6)
    
    # Should be expired
    assert suspicion.is_expired()
    
    # Mark dead
    tracker.update_node(node, b'DEAD', 1, time.monotonic())
    
    state = tracker.get_node_state(node)
    assert state.status == b'DEAD'


# =============================================================================
# Integration: Gossip Dissemination
# =============================================================================

@test("Integration: gossip dissemination simulation")
def test_gossip_dissemination():
    """Simulate gossip spreading through a cluster."""
    
    # Create buffers for 5 nodes
    buffers = {f'node{i}': GossipBuffer() for i in range(5)}
    
    # Node 0 has an update
    buffers['node0'].add_update('dead', ('10.0.0.99', 8080), 1, n_members=5)
    
    # Simulate gossip rounds
    for round in range(10):
        for sender_name, sender_buf in list(buffers.items()):
            encoded = sender_buf.encode_piggyback(max_count=3)
            if encoded:
                # Broadcast to all other nodes
                for receiver_name, receiver_buf in buffers.items():
                    if receiver_name != sender_name:
                        updates = GossipBuffer.decode_piggyback(encoded)
                        for u in updates:
                            receiver_buf.add_update(
                                u.update_type, u.node, u.incarnation, n_members=5
                            )
    
    # All nodes should have the update eventually
    # (or it should be fully disseminated)
    total_updates = sum(len(b.updates) for b in buffers.values())
    # Updates should have spread or been marked as broadcast-complete


# =============================================================================
# Performance Tests
# =============================================================================

@test("Performance: PiggybackUpdate serialization")
def test_piggyback_performance():
    """Ensure serialization is fast."""
    import time
    
    update = PiggybackUpdate(
        update_type='alive',
        node=('192.168.1.1', 8080),
        incarnation=42,
        timestamp=time.monotonic(),
    )
    
    start = time.monotonic()
    for _ in range(10000):
        _ = update.to_bytes()
    elapsed = time.monotonic() - start
    
    # Should complete in under 1 second
    assert elapsed < 1.0, f"Serialization too slow: {elapsed:.3f}s for 10k ops"


@test("Performance: GossipBuffer heapq selection")
def test_gossip_buffer_performance():
    """Ensure heapq-based selection is fast."""
    import time
    
    buffer = GossipBuffer()
    
    # Add many updates
    for i in range(1000):
        buffer.add_update('alive', (f'10.0.{i//256}.{i%256}', 8080), i)
    
    start = time.monotonic()
    for _ in range(1000):
        _ = buffer.get_updates_to_piggyback(10)
    elapsed = time.monotonic() - start
    
    # Should complete in under 1 second
    assert elapsed < 1.0, f"Selection too slow: {elapsed:.3f}s for 1k ops"


# =============================================================================
# Main Runner
# =============================================================================

async def run_all_tests():
    """Run all test functions."""
    
    print("=" * 60)
    print("SWIM + Lifeguard Comprehensive Tests")
    print("=" * 60)
    
    # Get all test functions
    test_functions = [
        # Constants
        test_encode_int_small,
        test_encode_int_large,
        test_encode_bool,
        test_constants,
        # NodeState
        test_node_state_default,
        test_node_state_slots,
        # PiggybackUpdate
        test_piggyback_roundtrip,
        test_piggyback_slots,
        test_piggyback_broadcast_count,
        # GossipBuffer
        test_gossip_buffer_basic,
        test_gossip_buffer_incarnation,
        test_gossip_buffer_priority,
        test_gossip_buffer_eviction,
        test_gossip_buffer_size_limit,
        test_gossip_buffer_decode_limit,
        # SuspicionState
        test_suspicion_timeout,
        test_suspicion_bounded_confirmers,
        test_suspicion_slots,
        # PendingIndirectProbe
        test_indirect_probe_bounds,
        test_indirect_probe_slots,
        # ProbeScheduler
        test_probe_scheduler_lockless,
        test_probe_scheduler_roundrobin,
        test_probe_scheduler_stats,
        # IncarnationTracker
        test_incarnation_tracker_basic,
        test_incarnation_tracker_stale,
        test_incarnation_tracker_bounded,
        # FlappingDetector
        test_flapping_detection,
        test_flapping_cooldown,
        test_leadership_change_slots,
        # LocalHealthMultiplier
        test_lhm_degradation,
        test_lhm_recovery,
        test_lhm_multiplier,
        # BoundedDict
        test_bounded_dict_size,
        test_bounded_dict_lru,
        test_bounded_dict_oldest,
        # Integration
        test_node_rejoin,
        test_suspicion_to_dead,
        test_gossip_dissemination,
        # Performance
        test_piggyback_performance,
        test_gossip_buffer_performance,
    ]
    
    for test_fn in test_functions:
        await test_fn()
    
    return results.summary()


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

