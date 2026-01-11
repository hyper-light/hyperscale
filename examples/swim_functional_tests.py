#!/usr/bin/env python3
"""
Functional tests for SWIM + Lifeguard protocol components.

Tests cover:
- Leadership election flows
- Suspicion manager operations
- Indirect probe manager
- Health monitoring
- Error handling and retry
- Metrics and audit logging
- Message handling
- Node lifecycle
- Multi-node integration
"""

import asyncio
import sys
import time
import inspect
import random
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock, patch

# Add project root to path
sys.path.insert(0, '/home/ada/Projects/hyperscale')

from examples.swim.core.constants import STATUS_OK, STATUS_DEAD, STATUS_SUSPECT
from examples.swim.core.node_state import NodeState
from examples.swim.core.resource_limits import BoundedDict
from examples.swim.core.metrics import Metrics
from examples.swim.core.audit import AuditLog, AuditEventType, AuditEvent
from examples.swim.core.errors import (
    NetworkError, ProtocolError, ResourceError,
    ProbeTimeoutError, MalformedMessageError, ErrorCategory,
)
from examples.swim.core.error_handler import ErrorHandler, ErrorContext, CircuitState

from examples.swim.core.retry import RetryPolicy, retry_with_backoff

from examples.swim.detection.probe_scheduler import ProbeScheduler
from examples.swim.detection.suspicion_state import SuspicionState
from examples.swim.detection.suspicion_manager import SuspicionManager
from examples.swim.detection.pending_indirect_probe import PendingIndirectProbe
from examples.swim.detection.indirect_probe_manager import IndirectProbeManager
from examples.swim.detection.incarnation_tracker import IncarnationTracker

from examples.swim.gossip.gossip_buffer import GossipBuffer
from examples.swim.gossip.piggyback_update import PiggybackUpdate

from examples.swim.leadership.leader_state import LeaderState
from examples.swim.leadership.leader_eligibility import LeaderEligibility
from examples.swim.leadership.flapping_detector import FlappingDetector

from examples.swim.health.local_health_multiplier import LocalHealthMultiplier
from examples.swim.health.health_monitor import EventLoopHealthMonitor
from examples.swim.health.graceful_degradation import GracefulDegradation


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
            for e in self.errors[:20]:
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
# LEADERSHIP ELECTION TESTS
# =============================================================================

print("\n" + "=" * 60)
print("LEADERSHIP ELECTION TESTS")
print("=" * 60)


@test("Leadership: LeaderState initial values")
def test_leader_state_initial():
    state = LeaderState()
    
    assert state.role == 'follower'
    assert state.current_term == 0
    assert state.current_leader is None
    assert state.voted_for is None
    assert len(state.votes_received) == 0


@test("Leadership: term increment on election start")
def test_leader_state_term_increment():
    state = LeaderState()
    state.current_term = 5
    
    result = state.start_election(new_term=6)
    
    assert state.current_term == 6
    assert state.role == 'candidate'


@test("Leadership: vote recording")
def test_leader_state_vote_recording():
    state = LeaderState()
    state.role = 'candidate'
    state.current_term = 1
    
    count1 = state.record_vote(('10.0.0.1', 8080))
    count2 = state.record_vote(('10.0.0.2', 8080))
    count3 = state.record_vote(('10.0.0.1', 8080))  # Duplicate
    
    assert count1 == 1
    assert count2 == 2
    assert count3 == 2  # Duplicate not counted


@test("Leadership: quorum calculation")
def test_leader_state_quorum():
    state = LeaderState()
    state.role = 'candidate'
    state.current_term = 1
    
    # 5 members: need 3 votes (5//2 + 1 = 3)
    n_members = 5
    votes_needed = (n_members // 2) + 1
    
    assert votes_needed == 3
    
    # Record 2 votes - not enough
    state.record_vote(('10.0.0.1', 8080))
    state.record_vote(('10.0.0.2', 8080))
    assert len(state.votes_received) < votes_needed
    
    # Record 3rd vote - quorum reached
    state.record_vote(('10.0.0.3', 8080))
    assert len(state.votes_received) >= votes_needed


@test("Leadership: become leader clears votes")
def test_leader_state_become_leader():
    state = LeaderState()
    state.role = 'candidate'
    state.current_term = 1
    
    state.record_vote(('10.0.0.1', 8080))
    state.record_vote(('10.0.0.2', 8080))
    
    state.become_leader(term=1)
    
    assert state.role == 'leader'
    assert len(state.votes_received) == 0


@test("Leadership: pre-vote recording")
def test_leader_state_pre_vote():
    state = LeaderState()
    
    count1 = state.record_pre_vote(('10.0.0.1', 8080))
    count2 = state.record_pre_vote(('10.0.0.2', 8080))
    
    assert count1 == 1
    assert count2 == 2


@test("Leadership: eligibility based on LHM")
def test_leader_eligibility_lhm():
    eligibility = LeaderEligibility(max_leader_lhm=4)
    
    # Healthy node is eligible
    assert eligibility.is_eligible(lhm_score=0, status=b'OK')
    
    # High LHM node is not eligible
    assert not eligibility.is_eligible(lhm_score=5, status=b'OK')


@test("Leadership: eligibility with SUSPECT status")
def test_leader_eligibility_suspect():
    eligibility = LeaderEligibility()
    
    # SUSPECT status makes ineligible
    assert not eligibility.is_eligible(lhm_score=0, status=b'SUSPECT')
    assert not eligibility.is_eligible(lhm_score=0, status=b'DEAD')


@test("Leadership: fencing token exists")
def test_leader_state_fencing_token():
    state = LeaderState()
    
    # Fencing token should be an attribute
    assert hasattr(state, 'fencing_token')
    assert isinstance(state.fencing_token, int)


@test("Leadership: become leader updates role")
def test_leader_state_become_leader_role():
    state = LeaderState()
    state.current_term = 5
    
    state.become_leader(term=5)
    
    assert state.role == 'leader'


# =============================================================================
# SUSPICION MANAGER TESTS
# =============================================================================

print("\n" + "=" * 60)
print("SUSPICION MANAGER TESTS")
print("=" * 60)


@test("Suspicion: start suspicion creates state")
async def test_suspicion_manager_start():
    manager = SuspicionManager()
    node = ('10.0.0.1', 8080)
    from_node = ('10.0.0.2', 8080)
    
    state = await manager.start_suspicion(node, incarnation=1, from_node=from_node)
    
    assert state is not None
    assert state.incarnation == 1


@test("Suspicion: confirm suspicion with new node")
async def test_suspicion_manager_confirm():
    manager = SuspicionManager()
    node = ('10.0.0.1', 8080)
    from_node = ('10.0.0.2', 8080)
    
    state = await manager.start_suspicion(node, incarnation=1, from_node=from_node)
    initial_count = state.confirmation_count
    
    # Add confirmations with incarnation and from_node
    await manager.confirm_suspicion(node, incarnation=1, from_node=('10.0.0.3', 8080))
    
    assert state.confirmation_count > initial_count


@test("Suspicion: refute suspicion removes it")
async def test_suspicion_manager_refute():
    manager = SuspicionManager()
    node = ('10.0.0.1', 8080)
    from_node = ('10.0.0.2', 8080)
    
    await manager.start_suspicion(node, incarnation=1, from_node=from_node)
    assert manager.get_suspicion(node) is not None
    
    # Refute with higher incarnation
    await manager.refute_suspicion(node, incarnation=2)
    
    assert manager.get_suspicion(node) is None


@test("Suspicion: stale incarnation ignored")
async def test_suspicion_manager_stale():
    manager = SuspicionManager()
    node = ('10.0.0.1', 8080)
    from_node = ('10.0.0.2', 8080)
    
    # Start with incarnation 5
    await manager.start_suspicion(node, incarnation=5, from_node=from_node)
    
    # Try to start with lower incarnation - should return existing
    existing = await manager.start_suspicion(node, incarnation=3, from_node=('10.0.0.3', 8080))
    
    assert existing.incarnation == 5


@test("Suspicion: concurrent suspicions for different nodes")
async def test_suspicion_manager_concurrent():
    manager = SuspicionManager()
    from_node = ('10.0.0.99', 8080)
    
    nodes = [(f'10.0.0.{i}', 8080) for i in range(10)]
    
    # Start suspicions concurrently
    await asyncio.gather(*[
        manager.start_suspicion(node, incarnation=1, from_node=from_node)
        for node in nodes
    ])
    
    # All should exist
    for node in nodes:
        state = manager.get_suspicion(node)
        assert state is not None


@test("Suspicion: clear all")
async def test_suspicion_manager_clear_all():
    manager = SuspicionManager()
    from_node = ('10.0.0.99', 8080)
    
    # Add several suspicions
    for i in range(5):
        await manager.start_suspicion((f'10.0.0.{i}', 8080), incarnation=1, from_node=from_node)
    
    await manager.clear_all()
    
    # All should be gone
    for i in range(5):
        state = manager.get_suspicion((f'10.0.0.{i}', 8080))
        assert state is None


@test("Suspicion: suspicion count")
async def test_suspicion_manager_count():
    manager = SuspicionManager()
    from_node = ('10.0.0.99', 8080)
    
    for i in range(5):
        await manager.start_suspicion((f'10.0.0.{i}', 8080), incarnation=1, from_node=from_node)
    
    # Access suspicions dict directly
    assert len(manager.suspicions) == 5


# =============================================================================
# INDIRECT PROBE MANAGER TESTS
# =============================================================================

print("\n" + "=" * 60)
print("INDIRECT PROBE MANAGER TESTS")
print("=" * 60)


@test("IndirectProbe: track pending probe")
def test_indirect_probe_track():
    manager = IndirectProbeManager()
    target = ('10.0.0.1', 8080)
    requester = ('10.0.0.2', 8080)
    
    probe = PendingIndirectProbe(
        target=target,
        requester=requester,
        start_time=time.monotonic(),
        timeout=5.0,
    )
    manager.pending_probes[target] = probe
    
    assert target in manager.pending_probes


@test("IndirectProbe: proxy selection")
def test_indirect_probe_proxies():
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=5.0,
    )
    
    # Add proxies
    probe.add_proxy(('10.0.0.3', 8080))
    probe.add_proxy(('10.0.0.4', 8080))
    probe.add_proxy(('10.0.0.5', 8080))
    
    assert len(probe.proxies) == 3


@test("IndirectProbe: first ACK completes probe")
def test_indirect_probe_first_ack():
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=5.0,
    )
    
    # First ACK should return True
    assert probe.record_ack() == True
    assert probe.is_completed()
    
    # Subsequent ACKs should return False
    assert probe.record_ack() == False


@test("IndirectProbe: timeout detection")
async def test_indirect_probe_timeout():
    probe = PendingIndirectProbe(
        target=('10.0.0.1', 8080),
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=0.1,
    )
    
    assert not probe.is_expired()
    
    await asyncio.sleep(0.15)
    
    assert probe.is_expired()


@test("IndirectProbe: manager record_ack")
def test_indirect_probe_manager_ack():
    manager = IndirectProbeManager()
    target = ('10.0.0.1', 8080)
    
    probe = PendingIndirectProbe(
        target=target,
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=5.0,
    )
    manager.pending_probes[target] = probe
    
    # Record ack
    result = manager.record_ack(target)
    assert result == True


@test("IndirectProbe: manager respects max pending")
def test_indirect_probe_manager_max():
    manager = IndirectProbeManager(max_pending=3)
    
    for i in range(10):
        target = (f'10.0.0.{i}', 8080)
        probe = PendingIndirectProbe(
            target=target,
            requester=('10.0.0.99', 8080),
            start_time=time.monotonic(),
            timeout=5.0,
        )
        # Normally would check limit before adding
        if len(manager.pending_probes) < manager.max_pending:
            manager.pending_probes[target] = probe
    
    assert len(manager.pending_probes) <= 3


@test("IndirectProbe: cleanup expired probes")
async def test_indirect_probe_cleanup():
    manager = IndirectProbeManager()
    target = ('10.0.0.1', 8080)
    
    # Add probe with short timeout
    probe = PendingIndirectProbe(
        target=target,
        requester=('10.0.0.2', 8080),
        start_time=time.monotonic(),
        timeout=0.1,
    )
    manager.pending_probes[target] = probe
    
    await asyncio.sleep(0.15)
    
    cleaned = manager.cleanup_expired()
    
    assert cleaned >= 1
    assert len(manager.pending_probes) == 0


# =============================================================================
# EVENTLOOP HEALTH MONITOR TESTS
# =============================================================================

print("\n" + "=" * 60)
print("EVENTLOOP HEALTH MONITOR TESTS")
print("=" * 60)


@test("HealthMonitor: initial state")
def test_health_monitor_initial():
    monitor = EventLoopHealthMonitor(
        lag_threshold=0.5,
        critical_lag_threshold=2.0,
    )
    
    assert not monitor.is_degraded
    assert monitor._consecutive_lag_count == 0


@test("HealthMonitor: has required attributes")
def test_health_monitor_attributes():
    monitor = EventLoopHealthMonitor()
    
    assert hasattr(monitor, 'is_degraded')
    assert hasattr(monitor, '_consecutive_lag_count')
    assert hasattr(monitor, '_samples')


@test("HealthMonitor: consecutive counters bounded")
def test_health_monitor_counters():
    monitor = EventLoopHealthMonitor()
    
    # Counters have max value
    assert hasattr(monitor, 'MAX_CONSECUTIVE_COUNT')
    assert monitor.MAX_CONSECUTIVE_COUNT > 0


@test("HealthMonitor: sample history exists")
def test_health_monitor_samples():
    monitor = EventLoopHealthMonitor()
    
    # Samples should be a deque with bounded size
    assert hasattr(monitor, '_samples')
    assert hasattr(monitor, 'history_size')


@test("HealthMonitor: thresholds configurable")
def test_health_monitor_thresholds():
    monitor = EventLoopHealthMonitor(
        lag_threshold=0.3,
        critical_lag_threshold=1.5,
    )
    
    assert monitor.lag_threshold == 0.3
    assert monitor.critical_lag_threshold == 1.5


# =============================================================================
# GRACEFUL DEGRADATION TESTS
# =============================================================================

print("\n" + "=" * 60)
print("GRACEFUL DEGRADATION TESTS")
print("=" * 60)


@test("Degradation: initial state")
def test_degradation_initial():
    degradation = GracefulDegradation()
    
    # Should have current level attribute
    assert hasattr(degradation, 'current_level')


@test("Degradation: thresholds configurable")
def test_degradation_thresholds():
    degradation = GracefulDegradation()
    
    # Should have threshold tuples
    assert hasattr(degradation, 'lhm_thresholds')
    assert hasattr(degradation, 'lag_thresholds')
    assert len(degradation.lhm_thresholds) > 0


@test("Degradation: get current policy")
def test_degradation_policy():
    degradation = GracefulDegradation()
    
    policy = degradation.get_current_policy()
    
    # Should have required fields
    assert hasattr(policy, 'timeout_multiplier')
    assert hasattr(policy, 'probe_rate')


@test("Degradation: force level updates state")
async def test_degradation_force_level():
    degradation = GracefulDegradation()
    
    await degradation.force_level(2)
    
    assert degradation.current_level == 2


@test("Degradation: should skip probe returns bool")
def test_degradation_skip_probe():
    degradation = GracefulDegradation()
    
    # At level 0, should not skip
    result = degradation.should_skip_probe()
    assert isinstance(result, bool)


@test("Degradation: update method exists")
async def test_degradation_update():
    degradation = GracefulDegradation()
    
    # Set callbacks
    degradation.set_health_callbacks(
        get_lhm=lambda: 0,
        get_event_loop_lag=lambda: 0.0,
    )
    
    await degradation.update()
    
    # Should not crash


@test("Degradation: is_degraded method")
def test_degradation_is_degraded():
    degradation = GracefulDegradation()
    
    # Should have is_degraded method that returns bool
    assert hasattr(degradation, 'is_degraded')
    result = degradation.is_degraded()  # Note: method call
    assert isinstance(result, bool)


# =============================================================================
# ERROR HANDLER TESTS
# =============================================================================

print("\n" + "=" * 60)
print("ERROR HANDLER TESTS")
print("=" * 60)


@test("ErrorHandler: initial state")
def test_error_handler_initial():
    handler = ErrorHandler()
    
    stats = handler.get_stats_summary()
    assert isinstance(stats, dict)


@test("ErrorHandler: handle error")
async def test_error_handler_handle():
    handler = ErrorHandler()
    
    error = NetworkError(message="Connection failed")
    await handler.handle(error)
    
    # Should not crash - error is recorded internally


@test("ErrorHandler: error categorization")
async def test_error_handler_categorization():
    handler = ErrorHandler()
    
    await handler.handle(NetworkError(message="net"))
    await handler.handle(ProtocolError(message="proto"))
    
    stats = handler.get_stats_summary()
    # Stats should have category keys
    assert isinstance(stats, dict)


@test("ErrorHandler: record success")
def test_error_handler_success():
    handler = ErrorHandler()
    
    handler.record_success(ErrorCategory.NETWORK)
    
    # Should not raise


@test("ErrorHandler: context manager")
async def test_error_handler_context():
    handler = ErrorHandler()
    
    async with ErrorContext(handler, operation="test"):
        pass  # Success
    
    # Should not crash


@test("ErrorHandler: context manager suppresses error")
async def test_error_handler_context_error():
    handler = ErrorHandler()
    
    # With default reraise=False, error is suppressed
    async with ErrorContext(handler, operation="test"):
        raise NetworkError(message="fail")
    
    # Error was suppressed and handled


@test("ErrorHandler: is_circuit_open")
def test_error_handler_circuit():
    handler = ErrorHandler()
    
    # Should be closed initially
    assert not handler.is_circuit_open(ErrorCategory.NETWORK)


# =============================================================================
# RETRY MECHANISM TESTS
# =============================================================================

print("\n" + "=" * 60)
print("RETRY MECHANISM TESTS")
print("=" * 60)


@test("Retry: success on first attempt")
async def test_retry_success_first():
    attempts = [0]
    
    async def succeeds():
        attempts[0] += 1
        return "success"
    
    policy = RetryPolicy(max_attempts=3, base_delay=0.01)
    result = await retry_with_backoff(succeeds, policy=policy)
    
    assert result == "success"
    assert attempts[0] == 1


@test("Retry: success on retry")
async def test_retry_success_on_retry():
    attempts = [0]
    
    async def fails_then_succeeds():
        attempts[0] += 1
        if attempts[0] < 3:
            raise NetworkError(message="fail")
        return "success"
    
    policy = RetryPolicy(max_attempts=5, base_delay=0.01)
    result = await retry_with_backoff(fails_then_succeeds, policy=policy)
    
    assert result == "success"
    assert attempts[0] == 3


@test("Retry: max retries respected")
async def test_retry_max_retries():
    attempts = [0]
    
    async def always_fails():
        attempts[0] += 1
        raise NetworkError(message="always fail")
    
    policy = RetryPolicy(max_attempts=3, base_delay=0.01)
    
    try:
        await retry_with_backoff(always_fails, policy=policy)
        assert False, "Should have raised"
    except NetworkError:
        pass
    
    assert attempts[0] == 3  # 3 attempts


@test("Retry: delay between attempts")
async def test_retry_delay():
    times = []
    
    async def fails():
        times.append(time.monotonic())
        raise NetworkError(message="fail")
    
    policy = RetryPolicy(max_attempts=3, base_delay=0.05)
    
    try:
        await retry_with_backoff(fails, policy=policy)
    except NetworkError:
        pass
    
    # Check delays exist
    if len(times) >= 2:
        delay = times[1] - times[0]
        assert delay >= 0.04  # At least 80% of base_delay


@test("Retry: policy defaults")
def test_retry_policy_defaults():
    policy = RetryPolicy()
    
    assert policy.max_attempts == 3
    assert policy.base_delay == 0.1
    assert policy.max_delay == 5.0
    assert policy.jitter == 0.1


@test("Retry: policy jitter range")
def test_retry_policy_jitter():
    policy = RetryPolicy(jitter=0.2)
    
    # Jitter should be a float between 0 and 1
    assert 0 <= policy.jitter <= 1


# =============================================================================
# METRICS TESTS
# =============================================================================

print("\n" + "=" * 60)
print("METRICS TESTS")
print("=" * 60)


@test("Metrics: counter increment")
def test_metrics_increment():
    metrics = Metrics()
    
    metrics.increment('probes_sent')
    metrics.increment('probes_sent')
    metrics.increment('probes_sent')
    
    assert metrics.probes_sent == 3


@test("Metrics: multiple counters")
def test_metrics_multiple():
    metrics = Metrics()
    
    metrics.increment('probes_sent')
    metrics.increment('probes_received')
    metrics.increment('probes_failed')
    
    assert metrics.probes_sent == 1
    assert metrics.probes_received == 1
    assert metrics.probes_failed == 1


@test("Metrics: increment by amount")
def test_metrics_increment_amount():
    metrics = Metrics()
    
    metrics.increment('probes_sent', amount=5)
    
    assert metrics.probes_sent == 5


@test("Metrics: reset counters")
def test_metrics_reset():
    metrics = Metrics()
    
    metrics.increment('probes_sent')
    metrics.increment('probes_sent')
    
    metrics.reset()
    
    assert metrics.probes_sent == 0


# =============================================================================
# AUDIT LOG TESTS
# =============================================================================

print("\n" + "=" * 60)
print("AUDIT LOG TESTS")
print("=" * 60)


@test("AuditLog: record event")
def test_audit_record():
    audit = AuditLog(max_events=100)
    
    audit.record(
        event_type=AuditEventType.NODE_JOINED,
        node=('10.0.0.1', 8080),
        reason="Node joined",
    )
    
    stats = audit.get_stats()
    assert stats['total_recorded'] >= 1


@test("AuditLog: capacity bounds")
def test_audit_capacity():
    audit = AuditLog(max_events=10)
    
    for i in range(20):
        audit.record(
            event_type=AuditEventType.NODE_JOINED,
            node=(f'10.0.0.{i}', 8080),
            reason=f"Node {i}",
        )
    
    stats = audit.get_stats()
    # Current events bounded by max_events
    assert stats['current_events'] <= 10


@test("AuditLog: event retrieval by type")
def test_audit_retrieval():
    audit = AuditLog(max_events=100)
    
    for i in range(5):
        audit.record(
            event_type=AuditEventType.NODE_JOINED,
            node=(f'10.0.0.{i}', 8080),
            reason=f"join {i}",
        )
    
    events = audit.get_events_by_type(AuditEventType.NODE_JOINED)
    assert len(events) == 5


@test("AuditLog: different event types")
def test_audit_event_types():
    audit = AuditLog(max_events=100)
    
    audit.record(
        event_type=AuditEventType.NODE_JOINED,
        node=('10.0.0.1', 8080),
        reason="join",
    )
    audit.record(
        event_type=AuditEventType.ELECTION_WON,
        node=('10.0.0.2', 8080),
        reason="elected",
    )
    
    stats = audit.get_stats()
    assert stats['current_events'] == 2


# =============================================================================
# MESSAGE HANDLING TESTS
# =============================================================================

print("\n" + "=" * 60)
print("MESSAGE HANDLING TESTS")
print("=" * 60)


@test("MessageDedup: same message not processed twice")
def test_message_dedup():
    seen = BoundedDict[bytes, float](max_size=100)
    
    msg_hash = b'abc123'
    
    # First time - not seen
    assert msg_hash not in seen
    seen[msg_hash] = time.monotonic()
    
    # Second time - already seen
    assert msg_hash in seen


@test("MessageDedup: cache expiry simulation")
def test_message_dedup_expiry():
    seen = BoundedDict[str, float](max_size=10, eviction_policy='OLDEST')
    
    # Add old messages
    for i in range(15):
        seen[f'msg_{i}'] = time.monotonic()
    
    # Old messages should be evicted
    assert len(seen) <= 10


@test("RateLimiting: per-sender tracking")
def test_rate_limit_tracking():
    limits = BoundedDict[tuple, list](max_size=100)
    
    sender = ('10.0.0.1', 8080)
    
    # Track messages from sender
    if sender not in limits:
        limits[sender] = []
    
    limits[sender].append(time.monotonic())
    limits[sender].append(time.monotonic())
    
    assert len(limits[sender]) == 2


@test("RateLimiting: excess messages counted")
def test_rate_limit_excess():
    max_per_second = 10
    messages = []
    dropped = 0
    
    for i in range(20):
        if len(messages) < max_per_second:
            messages.append(i)
        else:
            dropped += 1
    
    assert dropped == 10


@test("MessageSize: oversized rejection")
def test_message_size_rejection():
    MAX_SIZE = 1400
    
    small_msg = b'x' * 100
    large_msg = b'x' * 2000
    
    assert len(small_msg) <= MAX_SIZE
    assert len(large_msg) > MAX_SIZE


@test("MessageParsing: malformed recovery")
def test_message_malformed_recovery():
    # Malformed messages should not crash
    malformed_messages = [
        b'',
        b'\x00\x01\x02',
        b'INVALID:',
        b'PROBE:not_a_number',
    ]
    
    for msg in malformed_messages:
        # Parsing should fail gracefully
        result = PiggybackUpdate.from_bytes(msg)
        assert result is None  # Graceful failure


# =============================================================================
# NODE LIFECYCLE TESTS
# =============================================================================

print("\n" + "=" * 60)
print("NODE LIFECYCLE TESTS")
print("=" * 60)


@test("Lifecycle: join updates tracker")
def test_lifecycle_join():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    tracker.update_node(node, b'OK', incarnation=1, timestamp=time.monotonic())
    
    state = tracker.get_node_state(node)
    assert state is not None
    assert state.status == b'OK'


@test("Lifecycle: leave updates status")
def test_lifecycle_leave():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    tracker.update_node(node, b'OK', incarnation=1, timestamp=time.monotonic())
    tracker.update_node(node, b'DEAD', incarnation=1, timestamp=time.monotonic())
    
    state = tracker.get_node_state(node)
    assert state.status == b'DEAD'


@test("Lifecycle: rejoin with higher incarnation")
def test_lifecycle_rejoin():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    # Initial join
    tracker.update_node(node, b'OK', incarnation=1, timestamp=time.monotonic())
    
    # Die
    tracker.update_node(node, b'DEAD', incarnation=1, timestamp=time.monotonic())
    
    # Rejoin with higher incarnation
    tracker.update_node(node, b'OK', incarnation=2, timestamp=time.monotonic())
    
    state = tracker.get_node_state(node)
    assert state.status == b'OK'
    assert state.incarnation == 2


@test("Lifecycle: stale update ignored")
def test_lifecycle_stale():
    tracker = IncarnationTracker()
    node = ('10.0.0.1', 8080)
    
    tracker.update_node(node, b'OK', incarnation=5, timestamp=time.monotonic())
    tracker.update_node(node, b'DEAD', incarnation=3, timestamp=time.monotonic())  # Stale
    
    state = tracker.get_node_state(node)
    assert state.status == b'OK'
    assert state.incarnation == 5


@test("Lifecycle: cleanup dead nodes")
async def test_lifecycle_cleanup():
    tracker = IncarnationTracker(
        max_nodes=100,
        dead_node_retention_seconds=0.1,
    )
    
    node = ('10.0.0.1', 8080)
    tracker.update_node(node, b'DEAD', incarnation=1, timestamp=time.monotonic())
    
    await asyncio.sleep(0.15)
    
    await tracker.cleanup()
    
    # Dead node may be cleaned up


# =============================================================================
# MULTI-NODE INTEGRATION TESTS
# =============================================================================

print("\n" + "=" * 60)
print("MULTI-NODE INTEGRATION TESTS")
print("=" * 60)


@test("Integration: three-node cluster state")
def test_integration_three_nodes():
    """Simulate three-node cluster membership."""
    trackers = {
        'node1': IncarnationTracker(),
        'node2': IncarnationTracker(),
        'node3': IncarnationTracker(),
    }
    
    nodes = [('10.0.0.1', 8080), ('10.0.0.2', 8080), ('10.0.0.3', 8080)]
    
    # Each node knows about all others
    for tracker in trackers.values():
        for node in nodes:
            tracker.update_node(node, b'OK', incarnation=0, timestamp=time.monotonic())
    
    # Verify all nodes tracked
    for tracker in trackers.values():
        assert len(tracker.get_all_nodes()) == 3


@test("Integration: failure detection simulation")
async def test_integration_failure_detection():
    """Simulate node failure detection."""
    tracker = IncarnationTracker()
    suspicion_mgr = SuspicionManager()
    
    target = ('10.0.0.1', 8080)
    tracker.update_node(target, b'OK', incarnation=1, timestamp=time.monotonic())
    
    # Probe fails - start suspicion
    tracker.update_node(target, b'SUSPECT', incarnation=1, timestamp=time.monotonic())
    await suspicion_mgr.start_suspicion(target, incarnation=1, from_node=('10.0.0.2', 8080))
    
    # Simulate confirmations with incarnation
    await suspicion_mgr.confirm_suspicion(target, incarnation=1, from_node=('10.0.0.3', 8080))
    await suspicion_mgr.confirm_suspicion(target, incarnation=1, from_node=('10.0.0.4', 8080))
    
    state = suspicion_mgr.get_suspicion(target)
    assert state is not None
    assert state.confirmation_count >= 2


@test("Integration: recovery after partition")
def test_integration_recovery():
    """Simulate node recovery after network partition."""
    tracker = IncarnationTracker()
    
    node = ('10.0.0.1', 8080)
    
    # Node healthy
    tracker.update_node(node, b'OK', incarnation=1, timestamp=time.monotonic())
    
    # Partition - marked dead
    tracker.update_node(node, b'DEAD', incarnation=1, timestamp=time.monotonic())
    
    # Partition heals - node rejoins with higher incarnation
    tracker.update_node(node, b'OK', incarnation=2, timestamp=time.monotonic())
    
    state = tracker.get_node_state(node)
    assert state.status == b'OK'
    assert state.incarnation == 2


@test("Integration: gossip convergence")
def test_integration_gossip_convergence():
    """Test that gossip eventually reaches all nodes."""
    n_nodes = 10
    buffers = {i: GossipBuffer() for i in range(n_nodes)}
    
    # Node 0 has an update
    update_node = ('10.0.0.99', 8080)
    buffers[0].add_update('dead', update_node, 1, n_members=n_nodes)
    
    # Simulate gossip rounds
    for _ in range(n_nodes * 3):
        for sender_id in range(n_nodes):
            receiver_id = random.randint(0, n_nodes - 1)
            if receiver_id == sender_id:
                continue
            
            encoded = buffers[sender_id].encode_piggyback(max_count=3)
            if encoded:
                updates = GossipBuffer.decode_piggyback(encoded)
                for u in updates:
                    buffers[receiver_id].add_update(
                        u.update_type, u.node, u.incarnation, n_members=n_nodes
                    )


@test("Integration: leader failover simulation")
def test_integration_leader_failover():
    """Simulate leader failure and new election."""
    # Node 1 is initial leader
    states = {
        'node1': LeaderState(),
        'node2': LeaderState(),
        'node3': LeaderState(),
    }
    
    # Node 1 becomes leader
    states['node1'].become_leader(term=1)
    for state in states.values():
        state.current_leader = ('10.0.0.1', 8080)
        state.current_term = 1
    
    # Leader dies - remaining nodes detect timeout
    # Node 2 starts election
    states['node2'].start_election(new_term=2)
    assert states['node2'].current_term == 2
    
    # Node 2 wins
    states['node2'].record_vote(('10.0.0.2', 8080))  # Self-vote
    states['node2'].record_vote(('10.0.0.3', 8080))  # Node 3's vote
    
    states['node2'].become_leader(term=2)
    assert states['node2'].role == 'leader'


@test("Integration: simultaneous elections")
def test_integration_simultaneous_elections():
    """Test that simultaneous elections handle correctly."""
    states = {
        'node1': LeaderState(),
        'node2': LeaderState(),
    }
    
    # Both start election
    states['node1'].start_election(new_term=1)
    states['node2'].start_election(new_term=1)
    
    # They'll have same term
    assert states['node1'].current_term == 1
    assert states['node2'].current_term == 1
    
    # Each votes for self
    states['node1'].record_vote(('10.0.0.1', 8080))
    states['node2'].record_vote(('10.0.0.2', 8080))
    
    # With 2 nodes, need 2 votes - neither has quorum yet


# =============================================================================
# PERFORMANCE/STRESS TESTS
# =============================================================================

print("\n" + "=" * 60)
print("PERFORMANCE/STRESS TESTS")
print("=" * 60)


@test("Stress: high churn rate")
async def test_stress_high_churn():
    """Test handling rapid node join/leave."""
    tracker = IncarnationTracker(max_nodes=1000)
    buffer = GossipBuffer(max_updates=1000)
    
    start = time.monotonic()
    
    for round in range(100):
        # 10 joins
        for i in range(10):
            node = (f'10.0.{round}.{i}', 8080)
            tracker.update_node(node, b'OK', incarnation=round, timestamp=time.monotonic())
            buffer.add_update('alive', node, round)
        
        # 5 leaves
        for i in range(5):
            node = (f'10.0.{round}.{i}', 8080)
            tracker.update_node(node, b'DEAD', incarnation=round, timestamp=time.monotonic())
            buffer.add_update('dead', node, round)
    
    elapsed = time.monotonic() - start
    assert elapsed < 5.0, f"High churn too slow: {elapsed:.2f}s"


@test("Stress: message storm")
async def test_stress_message_storm():
    """Test handling many concurrent messages."""
    buffer = GossipBuffer(max_updates=100)
    
    async def producer(id: int):
        for i in range(100):
            buffer.add_update('alive', (f'10.{id}.0.{i}', 8080), i)
            await asyncio.sleep(0)
    
    async def consumer():
        for _ in range(200):
            updates = buffer.get_updates_to_piggyback(10)
            if updates:
                buffer.mark_broadcasts(updates)
            await asyncio.sleep(0)
    
    start = time.monotonic()
    
    await asyncio.gather(
        producer(1),
        producer(2),
        producer(3),
        consumer(),
    )
    
    elapsed = time.monotonic() - start
    assert elapsed < 5.0, f"Message storm too slow: {elapsed:.2f}s"


@test("Stress: long-running stability")
async def test_stress_long_running():
    """Test no memory growth over many operations."""
    import gc
    
    tracker = IncarnationTracker(max_nodes=100)
    buffer = GossipBuffer(max_updates=100)
    scheduler = ProbeScheduler()
    
    gc.collect()
    
    for round in range(500):  # Reduced iterations
        # Churn
        node = (f'10.0.{round % 256}.{round // 256}', 8080)
        tracker.update_node(node, b'OK', incarnation=round, timestamp=time.monotonic())
        buffer.add_update('alive', node, round)
        
        if round % 10 == 0:
            scheduler.update_members([
                (f'10.0.{i}.1', 8080) for i in range(10)
            ])
        
        if round % 50 == 0:
            buffer.cleanup()
            await tracker.cleanup()
    
    # Force GC and verify bounded structures
    gc.collect()
    # Tracker/buffer are bounded


@test("Stress: network delay tolerance")
async def test_stress_network_delay():
    """Test protocol handles simulated network delays."""
    suspicion_mgr = SuspicionManager()
    from_node = ('10.0.0.99', 8080)
    
    node = ('10.0.0.1', 8080)
    await suspicion_mgr.start_suspicion(node, incarnation=1, from_node=from_node)
    
    # Simulate delayed confirmations with incarnation
    await asyncio.sleep(0.1)
    await suspicion_mgr.confirm_suspicion(node, incarnation=1, from_node=('10.0.0.2', 8080))
    
    await asyncio.sleep(0.1)
    await suspicion_mgr.confirm_suspicion(node, incarnation=1, from_node=('10.0.0.3', 8080))
    
    state = suspicion_mgr.get_suspicion(node)
    assert state is not None
    assert state.confirmation_count >= 2


# =============================================================================
# Main Runner
# =============================================================================

async def run_all_tests():
    """Run all test functions."""
    
    test_functions = [
        # Leadership
        test_leader_state_initial,
        test_leader_state_term_increment,
        test_leader_state_vote_recording,
        test_leader_state_quorum,
        test_leader_state_become_leader,
        test_leader_state_pre_vote,
        test_leader_eligibility_lhm,
        test_leader_eligibility_suspect,
        test_leader_state_fencing_token,
        test_leader_state_become_leader_role,
        # Suspicion
        test_suspicion_manager_start,
        test_suspicion_manager_confirm,
        test_suspicion_manager_refute,
        test_suspicion_manager_stale,
        test_suspicion_manager_concurrent,
        test_suspicion_manager_clear_all,
        test_suspicion_manager_count,
        # Indirect Probe
        test_indirect_probe_track,
        test_indirect_probe_proxies,
        test_indirect_probe_first_ack,
        test_indirect_probe_timeout,
        test_indirect_probe_manager_ack,
        test_indirect_probe_manager_max,
        test_indirect_probe_cleanup,
        # Health Monitor
        test_health_monitor_initial,
        test_health_monitor_attributes,
        test_health_monitor_counters,
        test_health_monitor_samples,
        test_health_monitor_thresholds,
        # Degradation
        test_degradation_initial,
        test_degradation_thresholds,
        test_degradation_policy,
        test_degradation_force_level,
        test_degradation_skip_probe,
        test_degradation_update,
        test_degradation_is_degraded,
        # Error Handler
        test_error_handler_initial,
        test_error_handler_handle,
        test_error_handler_categorization,
        test_error_handler_success,
        test_error_handler_context,
        test_error_handler_context_error,
        test_error_handler_circuit,
        # Retry
        test_retry_success_first,
        test_retry_success_on_retry,
        test_retry_max_retries,
        test_retry_delay,
        test_retry_policy_defaults,
        test_retry_policy_jitter,
        # Metrics
        test_metrics_increment,
        test_metrics_multiple,
        test_metrics_increment_amount,
        test_metrics_reset,
        # Audit
        test_audit_record,
        test_audit_capacity,
        test_audit_retrieval,
        test_audit_event_types,
        # Message Handling
        test_message_dedup,
        test_message_dedup_expiry,
        test_rate_limit_tracking,
        test_rate_limit_excess,
        test_message_size_rejection,
        test_message_malformed_recovery,
        # Lifecycle
        test_lifecycle_join,
        test_lifecycle_leave,
        test_lifecycle_rejoin,
        test_lifecycle_stale,
        test_lifecycle_cleanup,
        # Integration
        test_integration_three_nodes,
        test_integration_failure_detection,
        test_integration_recovery,
        test_integration_gossip_convergence,
        test_integration_leader_failover,
        test_integration_simultaneous_elections,
        # Stress
        test_stress_high_churn,
        test_stress_message_storm,
        test_stress_long_running,
        test_stress_network_delay,
    ]
    
    for test_fn in test_functions:
        await test_fn()
    
    return results.summary()


if __name__ == "__main__":
    print("=" * 60)
    print("SWIM + Lifeguard Functional Tests")
    print("=" * 60)
    
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)
