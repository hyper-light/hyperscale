# AD-29 Timeout Path Analysis: Unconfirmed Peer Handling

## Executive Summary

This document analyzes timeout paths in the Hyperscale distributed system and proposes approaches for handling unconfirmed peer timeouts that comply with **AD-29's effectiveness guarantee**: "Only confirmed peers can be suspected/declared dead."

## Current State

### AD-29 Guard Implementation

The system currently has a **centralized guard** in `HealthAwareServer.start_suspicion()`:

```python
# hyperscale/distributed_rewrite/swim/health_aware_server.py:2672-2679
async def start_suspicion(
    self,
    node: tuple[str, int],
    incarnation: int,
    from_node: tuple[str, int],
) -> SuspicionState | None:
    """
    Start suspecting a node or add confirmation to existing suspicion.

    Per AD-29: Only confirmed peers can be suspected. If we've never
    successfully communicated with a peer, we can't meaningfully suspect
    them - they might just not be up yet during cluster formation.
    """
    # AD-29: Guard against suspecting unconfirmed peers
    if not self.is_peer_confirmed(node):
        self._metrics.increment("suspicions_skipped_unconfirmed")
        return None
```

### Timeout Flow Architecture

The timeout mechanism uses a **hierarchical failure detection** system with two layers:

#### 1. Global Layer (Machine-Level Detection)
- **Component**: `TimingWheel` (hierarchical timing wheel)
- **Location**: `hyperscale/distributed_rewrite/swim/detection/timing_wheel.py`
- **Timeout Range**: 5-30 seconds (configurable)
- **Purpose**: Detect if an entire machine/node is down
- **Expiration Handler**: `HierarchicalFailureDetector._handle_global_expiration()`

**Flow**:
```
start_suspicion()
  → hierarchical_detector.suspect_global()
    → timing_wheel.add(node, state, expiration_time)
      → [wheel ticks every 100ms]
        → _handle_global_expiration(node, state)  [TIMEOUT PATH]
          → _on_suspicion_expired(node, incarnation)
```

#### 2. Job Layer (Per-Job Detection)
- **Component**: `JobSuspicionManager` (adaptive polling)
- **Location**: `hyperscale/distributed_rewrite/swim/detection/job_suspicion_manager.py`
- **Timeout Range**: 1-10 seconds (configurable)
- **Purpose**: Detect if a node is unresponsive for a specific job/workflow
- **Expiration Handler**: `HierarchicalFailureDetector._handle_job_expiration()`

### Critical Finding: No Bypass Paths

**Analysis Result**: ✅ **The AD-29 guard is NOT bypassable**

All timeout paths funnel through the guard:

1. **Global Timeouts** → `_handle_global_expiration()` → `_on_suspicion_expired()` → Updates incarnation tracker to "DEAD"
   - But this is ONLY called if suspicion was started via `suspect_global()`
   - `suspect_global()` is ONLY called from `start_suspicion()`
   - `start_suspicion()` has the AD-29 guard

2. **Job Timeouts** → `_handle_job_expiration()` → Updates job-specific state
   - Does NOT mark nodes as globally dead
   - Only affects job-specific routing

3. **Direct State Updates** → None found
   - No direct calls to `incarnation_tracker.update_node(..., "SUSPECT")`
   - No direct calls to `incarnation_tracker.update_node(..., "DEAD")`
   - All state changes go through the hierarchical detector

**Verification**: `grep` search for direct incarnation tracker updates found **zero** bypasses.

## The Problem

Currently, if an **unconfirmed peer** times out:
- The timeout fires in the TimingWheel
- The expiration handler is called
- The node is marked DEAD
- **BUT** the suspicion was never created because the AD-29 guard rejected it

**This creates a logical inconsistency**: We can't have a timeout for a suspicion that was never created.

## Proposed Approaches (AD-29 Compliant)

### Approach 1: Passive Removal (Recommended)

**Concept**: Let unconfirmed peers "age out" passively without declaring them dead.

**Implementation**:
```python
# In HealthAwareServer
async def _cleanup_unconfirmed_peers(self) -> None:
    """
    Periodic cleanup of unconfirmed peers that have timed out.

    Per AD-29: We don't suspect/kill unconfirmed peers, we just remove
    them from the membership list as "never joined."
    """
    now = time.monotonic()
    cutoff = now - self._unconfirmed_peer_timeout  # e.g., 60 seconds

    nodes: Nodes = self._context.read("nodes")
    to_remove: list[tuple[str, int]] = []

    for node in nodes:
        # Check if peer is unconfirmed and old
        if not self.is_peer_confirmed(node):
            first_seen = self._first_seen_times.get(node)
            if first_seen and first_seen < cutoff:
                to_remove.append(node)

    for node in to_remove:
        self._metrics.increment("unconfirmed_peers_removed")
        # Remove from membership without marking as DEAD
        await self._remove_node_from_membership(node)
        self._audit_log.record(
            AuditEventType.UNCONFIRMED_PEER_REMOVED,
            node=node,
            reason="never_confirmed",
        )
```

**Pros**:
- ✅ Simple and clean
- ✅ No risk of false positives
- ✅ Natural behavior: "If you never joined, you're not part of the cluster"
- ✅ No protocol violations

**Cons**:
- ❌ Slow to react to truly dead unconfirmed peers
- ❌ Memory held longer

**When to Use**: Default approach for most scenarios

---

### Approach 2: Confirmation Window with Fast Timeout

**Concept**: Give unconfirmed peers a short window to confirm, then remove them aggressively.

**Implementation**:
```python
# In HealthAwareServer
async def _handle_new_peer_discovery(self, node: tuple[str, int]) -> None:
    """
    When a new peer is discovered (via gossip, bootstrap, etc.),
    set a confirmation deadline.
    """
    self._first_seen_times[node] = time.monotonic()
    self._confirmation_deadlines[node] = time.monotonic() + self._confirmation_window

    # Schedule a fast-track check
    await self._schedule_confirmation_check(node, self._confirmation_window)

async def _schedule_confirmation_check(
    self,
    node: tuple[str, int],
    delay: float,
) -> None:
    """Schedule a check to see if peer confirmed within window."""
    async def check_confirmation():
        await asyncio.sleep(delay)

        # Double-check peer still exists and is unconfirmed
        if not self.is_peer_confirmed(node):
            deadline = self._confirmation_deadlines.get(node)
            if deadline and time.monotonic() >= deadline:
                # Remove unconfirmed peer that missed confirmation window
                await self._remove_node_from_membership(node)
                self._metrics.increment("unconfirmed_peers_timed_out")
                self._audit_log.record(
                    AuditEventType.UNCONFIRMED_PEER_TIMEOUT,
                    node=node,
                    reason="missed_confirmation_window",
                )

    self._task_runner.run(check_confirmation)
```

**Pros**:
- ✅ Faster reaction to unconfirmed peers
- ✅ More aggressive memory management
- ✅ Clear separation: "You have X seconds to confirm or you're out"

**Cons**:
- ❌ More complex (requires deadline tracking)
- ❌ May prematurely remove slow-to-start peers
- ❌ Requires tuning the confirmation window

**When to Use**: High-churn environments where memory is constrained

---

### Approach 3: Proactive Confirmation Attempts

**Concept**: Actively try to confirm unconfirmed peers before removing them.

**Implementation**:
```python
# In HealthAwareServer
async def _attempt_peer_confirmation(self, node: tuple[str, int]) -> bool:
    """
    Actively probe an unconfirmed peer to establish confirmation.

    This is more aggressive than waiting for gossip - we directly
    ping the peer to see if they respond.
    """
    try:
        # Send a ping to the unconfirmed peer
        response = await self._send_ping_for_confirmation(node, timeout=2.0)

        if response:
            # Mark as confirmed
            self._confirmed_peers.add(node)
            self._metrics.increment("peers_confirmed_by_probe")
            self._audit_log.record(
                AuditEventType.PEER_CONFIRMED,
                node=node,
                method="active_probe",
            )
            return True
    except Exception:
        pass

    return False

async def _cleanup_unconfirmed_peers_with_confirmation(self) -> None:
    """
    Cleanup unconfirmed peers, but try to confirm them first.
    """
    now = time.monotonic()
    cutoff = now - self._unconfirmed_peer_timeout

    nodes: Nodes = self._context.read("nodes")
    for node in nodes:
        if not self.is_peer_confirmed(node):
            first_seen = self._first_seen_times.get(node)
            if first_seen and first_seen < cutoff:
                # Try one last time to confirm
                confirmed = await self._attempt_peer_confirmation(node)
                if not confirmed:
                    await self._remove_node_from_membership(node)
                    self._metrics.increment("unconfirmed_peers_removed_after_probe")
```

**Pros**:
- ✅ More robust: Tries to confirm before removing
- ✅ Handles slow-start peers better
- ✅ Reduces false removals

**Cons**:
- ❌ More complex
- ❌ Adds network overhead (probing)
- ❌ May delay cleanup if probes time out

**When to Use**: Scenarios where peer confirmation is critical and you want to minimize false removals

---

### Approach 4: Separate Lifecycle State (Most Robust)

**Concept**: Introduce an explicit "UNCONFIRMED" lifecycle state separate from ALIVE/SUSPECT/DEAD.

**Implementation**:
```python
# In incarnation_tracker.py
class NodeLifecycleState(Enum):
    UNCONFIRMED = b"UNCONFIRMED"  # Discovered but never confirmed
    ALIVE = b"ALIVE"
    SUSPECT = b"SUSPECT"
    DEAD = b"DEAD"

# In HealthAwareServer
async def _handle_new_peer_discovery(self, node: tuple[str, int]) -> None:
    """Mark new peers as UNCONFIRMED initially."""
    self._incarnation_tracker.update_node(
        node,
        b"UNCONFIRMED",
        incarnation=0,
        timestamp=time.monotonic(),
    )

async def mark_peer_confirmed(self, node: tuple[str, int]) -> None:
    """
    Mark a peer as confirmed (successful bidirectional communication).

    Transitions: UNCONFIRMED → ALIVE
    """
    current_state = self._incarnation_tracker.get_node_state(node)
    if current_state == b"UNCONFIRMED":
        self._incarnation_tracker.update_node(
            node,
            b"ALIVE",
            incarnation=self._incarnation_tracker.get_incarnation(node),
            timestamp=time.monotonic(),
        )
        self._confirmed_peers.add(node)
        self._metrics.increment("peers_confirmed")

async def _cleanup_unconfirmed_peers(self) -> None:
    """Remove peers stuck in UNCONFIRMED state."""
    now = time.monotonic()
    cutoff = now - self._unconfirmed_peer_timeout

    # Query incarnation tracker for UNCONFIRMED nodes
    unconfirmed_nodes = self._incarnation_tracker.get_nodes_by_state(b"UNCONFIRMED")

    for node in unconfirmed_nodes:
        last_update = self._incarnation_tracker.get_last_update_time(node)
        if last_update < cutoff:
            # Remove from membership (NOT marked as DEAD)
            await self._remove_node_from_membership(node)
            self._incarnation_tracker.remove_node(node)
            self._metrics.increment("unconfirmed_peers_removed")
```

**State Transition Diagram**:
```
       [Discovery]
           ↓
      UNCONFIRMED ──────[timeout]──────→ [Removed]
           ↓
     [First ACK/Response]
           ↓
         ALIVE ──────[timeout]──────→ SUSPECT ──────[timeout]──────→ DEAD
           ↑                             ↓
           └────────[refutation]─────────┘
```

**Pros**:
- ✅ Most explicit and clear
- ✅ Separate lifecycle tracking for unconfirmed peers
- ✅ Enables richer monitoring/observability
- ✅ No confusion between "never confirmed" and "dead"

**Cons**:
- ❌ Requires changes to `IncarnationTracker`
- ❌ More states to manage
- ❌ Protocol extension (gossip must handle UNCONFIRMED state)

**When to Use**: Long-term robust solution for production systems

---

## Comparison Matrix

| Approach | Complexity | Reaction Speed | Robustness | Memory Efficiency | AD-29 Compliance |
|----------|------------|----------------|------------|-------------------|------------------|
| **1. Passive Removal** | ⭐ Low | ⭐ Slow | ⭐⭐⭐ High | ⭐⭐ Medium | ✅ Full |
| **2. Fast Timeout** | ⭐⭐ Medium | ⭐⭐⭐ Fast | ⭐⭐ Medium | ⭐⭐⭐ High | ✅ Full |
| **3. Active Confirmation** | ⭐⭐⭐ High | ⭐⭐ Medium | ⭐⭐⭐ High | ⭐⭐ Medium | ✅ Full |
| **4. Separate State** | ⭐⭐⭐⭐ Very High | ⭐⭐ Medium | ⭐⭐⭐⭐ Very High | ⭐⭐⭐ High | ✅ Full |

## Recommendations

### For Immediate Implementation
**Use Approach 1 (Passive Removal)**: It's simple, safe, and fully compliant with AD-29. No risk of false positives.

### For High-Churn Environments
**Use Approach 2 (Fast Timeout)**: Faster reaction and better memory efficiency when peers join/leave frequently.

### For Production-Grade Systems (Long Term)
**Use Approach 4 (Separate Lifecycle State)**: Most robust and explicit. Provides the clearest separation of concerns.

### Hybrid Approach (Best of Both Worlds)
Combine Approach 1 and Approach 3:
1. Use passive removal as the default
2. When approaching memory limits, proactively attempt confirmation
3. Remove peers that fail confirmation attempts

## AD-29 Compliance Verification

All proposed approaches maintain AD-29 compliance because:

1. ✅ **No suspicion of unconfirmed peers**: We never call `start_suspicion()` for unconfirmed peers
2. ✅ **No dead marking**: We never transition unconfirmed peers to DEAD state
3. ✅ **Clean removal**: We simply remove them from membership as "never joined"
4. ✅ **No protocol violations**: Removal is local cleanup, not a distributed death declaration

## Implementation Checklist

For any approach:
- [ ] Track first-seen time for all discovered peers
- [ ] Add `_unconfirmed_peer_timeout` configuration parameter
- [ ] Implement periodic cleanup task (runs every 10-30 seconds)
- [ ] Add metrics: `unconfirmed_peers_removed`, `unconfirmed_peers_timed_out`
- [ ] Add audit events: `UNCONFIRMED_PEER_REMOVED`, `UNCONFIRMED_PEER_TIMEOUT`
- [ ] Update tests to verify unconfirmed peers are not suspected
- [ ] Add integration test for unconfirmed peer cleanup
- [ ] Document behavior in operations runbook

## Related Documents

- **AD-29**: Only confirmed peers can be suspected (effectiveness guarantee)
- **AD-26**: Adaptive healthcheck extensions (timeout management)
- **AD-30**: Hierarchical failure detection architecture

## Conclusion

**The current system is safe**: The AD-29 guard is centralized and cannot be bypassed. All timeout paths funnel through `start_suspicion()`, which enforces the confirmation check.

**We should still implement timeout handling for unconfirmed peers** to prevent:
- Memory leaks from accumulated unconfirmed peers
- Confusion about peer lifecycle states
- Unnecessary probing of peers that never joined

**Recommended first step**: Implement Approach 1 (Passive Removal) as it's simple, safe, and provides immediate value without risk.
