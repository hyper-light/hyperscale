# AD-26 Healthcheck Extension Non-Compliance Issues

## Overview

This document tracks critical issues with the AD-26 (Adaptive Healthcheck Extensions) implementation that prevent it from working correctly.

---

## ✅ Issue 1: Protocol Mismatch (FIXED)

**Status**: ✅ Fixed in commit `2dd53517`

**Problem**: `HealthcheckExtensionRequest` requires `estimated_completion` and `active_workflow_count` fields, but the heartbeat piggyback handler constructed it without these fields, causing TypeError at runtime.

**Solution**:
- Added `extension_estimated_completion` and `extension_active_workflow_count` fields to `WorkerHeartbeat`
- Updated manager's `_handle_heartbeat_extension_request()` to pass all required fields

**Files Modified**:
- [distributed.py](hyperscale/distributed_rewrite/models/distributed.py#L558-L559)
- [manager.py](hyperscale/distributed_rewrite/nodes/manager.py#L2021-L2028)

---

## ⚠️ Issue 2: No Deadline Enforcement

**Status**: 🔴 Critical - Not Implemented

**Problem**: Manager stores `_worker_deadlines` and updates them on grant, but deadlines are **not enforced**:
- No adjustment of SWIM suspicion timers
- No delay of eviction when deadline extended
- No trigger of eviction when grace period expires
- Extensions are recorded but ignored

**Current State**:
```python
# Deadlines are stored (manager.py:570)
self._worker_deadlines: dict[str, float] = {}

# Updated on grant (manager.py:2038, 10239)
self._worker_deadlines[worker_id] = response.new_deadline

# ❌ But NEVER checked or enforced anywhere
```

**What's Missing**:

### 2.1 Deadline Monitoring Loop
No background task checks deadlines and triggers actions:
```python
# NEEDED: Add to manager.__init__
async def _deadline_enforcement_loop(self):
    """Check worker deadlines and enforce suspicion/eviction."""
    while self._running:
        now = time.monotonic()

        for worker_id, deadline in self._worker_deadlines.items():
            if now > deadline:
                # Deadline expired without extension
                grace_expiry = deadline + self._grace_period

                if now < grace_expiry:
                    # Within grace period - mark suspected
                    await self._suspect_worker_deadline_expired(worker_id)
                else:
                    # Grace expired - evict worker
                    await self._evict_worker_deadline_expired(worker_id)

        await asyncio.sleep(5.0)  # Check every 5 seconds
```

### 2.2 Integration with SWIM Failure Detector
SWIM timing wheels need to be adjusted when deadlines extended:
```python
# NEEDED: In _handle_heartbeat_extension_request and request_extension
if response.granted:
    self._worker_deadlines[worker_id] = response.new_deadline

    # ❌ Missing: Adjust SWIM timing wheel
    if self._swim_failure_detector:
        await self._swim_failure_detector.extend_deadline(
            worker_addr,
            response.extension_seconds
        )
```

### 2.3 Suspicion/Eviction Handlers
No handlers to act on deadline expiration:
```python
# NEEDED: Add to manager
async def _suspect_worker_deadline_expired(self, worker_id: str):
    """Mark worker suspected when deadline expires."""
    worker = self._worker_pool.get_worker(worker_id)
    if not worker:
        return

    worker_addr = (worker.node.host, worker.node.port)

    # Mark suspected in SWIM
    await self.suspect_node(
        node=worker_addr,
        incarnation=worker.incarnation,
        from_node=(self._host, self._udp_port)
    )

    await self._udp_logger.log(ServerWarning(
        message=f"Worker {worker_id} deadline expired, marked suspected",
        node_host=self._host,
        node_port=self._tcp_port,
        node_id=self._node_id.short,
    ))

async def _evict_worker_deadline_expired(self, worker_id: str):
    """Evict worker when grace period expires after deadline."""
    await self._udp_logger.log(ServerError(
        message=f"Worker {worker_id} grace period expired, evicting",
        node_host=self._host,
        node_port=self._tcp_port,
        node_id=self._node_id.short,
    ))

    # Trigger worker failure handler
    await self._handle_worker_failure(worker_id)

    # Clean up deadline tracking
    self._worker_deadlines.pop(worker_id, None)
```

**Impact**: Extensions are cosmetic - workers aren't given any actual leniency, defeating the entire purpose of AD-26.

**Required Work**:
1. Add deadline monitoring loop to manager
2. Add suspicion/eviction handlers
3. Integrate with SWIM failure detector's timing wheels
4. Add cleanup on worker removal
5. Test deadline enforcement end-to-end

---

## ⚠️ Issue 3: Competing AD-26 Implementations

**Status**: 🟡 High - Architectural Inconsistency

**Problem**: Two separate AD-26 implementations exist that don't cooperate:

### Implementation 1: SWIM Failure Detector Extension
**Location**: [hierarchical_failure_detector.py:357](hyperscale/distributed_rewrite/swim/detection/hierarchical_failure_detector.py#L357)

```python
async def request_extension(
    self,
    node: NodeAddress,
    reason: str,
    current_progress: float,
) -> tuple[bool, float, str | None, bool]:
    """Request extension for suspected node."""
    # Extends timing wheel expiration only when already suspected
    ...
```

**Characteristics**:
- Only works when node **already suspected**
- Directly manipulates SWIM timing wheels
- Integrated with SWIM protocol
- Uses `ExtensionTracker` from health module

### Implementation 2: WorkerHealthManager
**Location**: [worker_health_manager.py](hyperscale/distributed_rewrite/health/worker_health_manager.py)

```python
class WorkerHealthManager:
    """Manages worker health and deadline extensions."""

    def handle_extension_request(
        self,
        request: HealthcheckExtensionRequest,
        current_deadline: float,
    ) -> HealthcheckExtensionResponse:
        """Handle extension request from worker."""
        # Uses ExtensionTracker per worker
        # Updates _worker_deadlines dict
        # No SWIM integration
        ...
```

**Characteristics**:
- Works before suspicion (proactive)
- Stores deadlines in `_worker_deadlines` dict
- No SWIM integration
- Uses same `ExtensionTracker` class

### The Divergence

These implementations can produce inconsistent results:

| Scenario | SWIM Impl | WorkerHealthManager | Result |
|----------|-----------|---------------------|--------|
| Worker requests extension via heartbeat | Not involved | Grants extension, updates `_worker_deadlines` | ❌ Deadline stored but not enforced |
| Worker already suspected, requests extension | Can extend timing wheel | Not involved | ❌ SWIM extended but deadline not tracked |
| Worker deadline expires | Not aware | Deadline expired in dict | ❌ No action taken |

**Impact**: Extension semantics diverge depending on path taken. Workers might get extensions that aren't honored or vice versa.

**Solution**: Choose one authority:

**Option A**: SWIM as Authority (Recommended)
- Remove `_worker_deadlines` dict from manager
- All extension requests go through SWIM failure detector
- SWIM timing wheels are the source of truth
- WorkerHealthManager becomes a facade to SWIM

**Option B**: Manager as Authority
- SWIM failure detector doesn't handle extensions
- Manager's `_worker_deadlines` is source of truth
- Implement deadline enforcement loop (Issue 2)
- Notify SWIM when deadlines change

---

## ⚠️ Issue 4: Progress Semantics for Long-Running Work

**Status**: 🟡 High - Brittle Extension Logic

**Problem**: Worker clamps progress to 0..1 range, but extension grant rule requires "strict increase". For long-running load tests where progress isn't naturally smooth 0..1, extensions become brittle.

**Current Code**:
```python
# worker.py:1550
progress = min(1.0, max(0.0, current_progress))  # Clamp to [0, 1]

# extension_tracker.py (grant logic)
if current_progress <= self._last_progress:
    # ❌ Denied - progress must strictly increase
    return (False, 0.0, "progress not increasing")
```

**Problem Scenarios**:

### Scenario 1: Long-Running Load Test
```
Time    Workflows Completed    Progress    Extension?
0s      0 / 10000              0.00        -
30s     100 / 10000            0.01        ✅ Granted (0.00 -> 0.01)
60s     200 / 10000            0.02        ✅ Granted (0.01 -> 0.02)
90s     300 / 10000            0.03        ✅ Granted (0.02 -> 0.03)
...
3000s   9900 / 10000           0.99        ✅ Granted (0.98 -> 0.99)
3030s   9950 / 10000           0.995       ✅ Granted (0.99 -> 0.995)
3060s   9975 / 10000           0.9975      ✅ Granted (0.995 -> 0.9975)
3090s   9987 / 10000           0.9987      ✅ Granted (0.9975 -> 0.9987)
3120s   9993 / 10000           0.9993      ✅ Granted (0.9987 -> 0.9993)
```

At high progress values, tiny increments become hard to demonstrate with float precision.

### Scenario 2: Workflow with Variable Throughput
```
Time    Completing/sec    Progress    Extension?
0s      10                0.00        -
30s     10                0.10        ✅ Granted (0.00 -> 0.10)
60s     5 (slowdown)      0.15        ✅ Granted (0.10 -> 0.15)
90s     5                 0.20        ✅ Granted (0.15 -> 0.20)
120s    20 (burst)        0.40        ✅ Granted (0.20 -> 0.40)
150s    2 (hiccup)        0.42        ✅ Granted (0.40 -> 0.42)
180s    2                 0.44        ✅ Granted (0.42 -> 0.44)
210s    0 (stuck!)        0.44        ❌ DENIED - no progress
```

Progress metric needs to be strictly increasing every 30s, which is unrealistic for variable workloads.

### Scenario 3: Rounding/Precision Issues
```python
# Workflow: 1000 items, 995 completed
progress_1 = 995 / 1000  # 0.995
progress_2 = 996 / 1000  # 0.996

# After float arithmetic:
progress_1_rounded = round(progress_1, 3)  # 0.995
progress_2_rounded = round(progress_2, 3)  # 0.996

# But what if we round to 2 decimals?
progress_1_rounded = round(progress_1, 2)  # 1.00
progress_2_rounded = round(progress_2, 2)  # 1.00

# ❌ Extension denied - progress appears equal!
```

**Solutions**:

### Option A: Use Absolute Metrics (Recommended)
Instead of 0..1 progress, use absolute completion counts:
```python
# Instead of:
current_progress: float  # 0.0-1.0

# Use:
completed_items: int  # Absolute count
total_items: int      # Total expected

# Extension grant logic becomes:
if completed_items > self._last_completed_items:
    # ✅ Granted - made progress
    self._last_completed_items = completed_items
```

Benefits:
- No precision issues
- Natural for workflows with discrete items
- Easy to demonstrate progress
- Works for long-running tests

### Option B: Epsilon-Based Progress Check
Allow "close enough" progress:
```python
PROGRESS_EPSILON = 0.001  # 0.1% minimum increase

if current_progress > self._last_progress + PROGRESS_EPSILON:
    # ✅ Granted
```

Drawbacks:
- Still has rounding issues at high progress values
- Harder to tune epsilon

### Option C: Time-Based Leniency
Grant extensions if progress increased *recently* (last N seconds):
```python
if current_progress > self._last_progress:
    self._last_progress_time = time.monotonic()
    # ✅ Granted

elif time.monotonic() - self._last_progress_time < 60.0:
    # ✅ Granted - made progress recently (within 60s)
```

**Recommended Implementation**:
1. Add `completed_items` and `total_items` to `HealthcheckExtensionRequest`
2. Update `ExtensionTracker` to use absolute metrics when available
3. Fall back to relative progress (0..1) for backward compatibility
4. Update worker to send both absolute and relative metrics

---

## Summary

| Issue | Severity | Status | Can Extensions Work? |
|-------|----------|--------|---------------------|
| **Issue 1: Protocol Mismatch** | 🔴 Critical | ✅ Fixed | N/A |
| **Issue 2: No Enforcement** | 🔴 Critical | ❌ Not Implemented | ❌ No |
| **Issue 3: Competing Impls** | 🟡 High | ❌ Architectural | ⚠️ Inconsistent |
| **Issue 4: Progress Semantics** | 🟡 High | ❌ Design Flaw | ⚠️ Brittle |

**Net Result**: AD-26 extensions are partially implemented but **not functional**. Issue 2 alone prevents extensions from having any effect. Issues 3 and 4 create additional reliability and consistency problems even if Issue 2 were fixed.

---

## Recommended Fix Order

1. **Fix Issue 2 (Deadline Enforcement)** - Highest priority
   - Implement deadline monitoring loop
   - Add suspicion/eviction handlers
   - Integrate with SWIM timing wheels
   - **Estimated effort**: 1-2 days

2. **Fix Issue 3 (Unify Implementations)** - Required for consistency
   - Choose authority (recommend SWIM)
   - Remove/refactor duplicate logic
   - **Estimated effort**: 1 day

3. **Fix Issue 4 (Progress Semantics)** - Quality improvement
   - Add absolute metrics to protocol
   - Update ExtensionTracker logic
   - **Estimated effort**: 0.5 days

**Total estimated effort**: 2.5-3.5 days

---

## Testing Requirements

After fixes, test these scenarios:

1. **Deadline Enforcement**
   - Worker requests extension → deadline updated → suspicion delayed
   - Worker doesn't request extension → deadline expires → marked suspected
   - Worker in grace period → still suspected but not evicted
   - Grace period expires → worker evicted

2. **Long-Running Work**
   - 10,000 workflow job → worker requests multiple extensions → completes successfully
   - Progress from 0.99 to 0.999 → extension still granted

3. **Variable Throughput**
   - Workflow has throughput spikes/dips → extensions granted based on absolute progress
   - Worker genuinely stuck (no progress) → extensions denied correctly

4. **Integration**
   - Extension granted → SWIM timing wheel updated
   - Worker becomes healthy → deadline tracking cleaned up
   - Worker fails → extensions reset for new incarnation
