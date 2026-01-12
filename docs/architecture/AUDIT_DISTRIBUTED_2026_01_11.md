# Distributed Module Audit - 2026-01-11

## Executive Summary

Comprehensive audit of `hyperscale/distributed` for memory leaks, race conditions, deadlocks, dropped errors, and invalid/hang states.

**Severity Levels:**
- **CRITICAL**: Must fix immediately - causes data loss, crashes, or security issues
- **HIGH**: Should fix soon - causes significant degradation or incorrect behavior  
- **MEDIUM**: Should fix - causes minor issues or technical debt
- **LOW**: Nice to have - code quality improvements

---

## 1. MEMORY LEAKS

### 1.1 [HIGH] Unbounded defaultdict(list) in Manager/Gate State

**Files:**
- `hyperscale/distributed/nodes/manager/state.py:103-104, 119-121`
- `hyperscale/distributed/nodes/gate/state.py:73`
- `hyperscale/distributed/nodes/gate/server.py:394`

**Pattern:**
```python
self._cancellation_pending_workflows: dict[str, set[str]] = defaultdict(set)
self._cancellation_errors: dict[str, list[str]] = defaultdict(list)
self._job_aggregated_results: dict[str, list["WorkflowStats"]] = defaultdict(list)
```

**Issue:** These defaultdicts grow indefinitely. While `clear_job_state()` and `clear_cancellation_state()` exist, they must be called explicitly. If a job fails mid-cancellation or results aren't collected, entries remain forever.

**Fix:** 
1. Add TTL-based cleanup for these collections
2. Bound list sizes (e.g., keep last N errors only)
3. Ensure cleanup is called in all code paths (success, failure, timeout)

---

### 1.2 [MEDIUM] Lock Dictionaries Grow Unboundedly

**Files:**
- `hyperscale/distributed/nodes/manager/state.py:49, 61, 108`
- `hyperscale/distributed/nodes/gate/state.py:44`
- `hyperscale/distributed/nodes/worker/state.py:65, 162, 277`
- `hyperscale/distributed/nodes/gate/models/gate_peer_state.py:80`

**Pattern:**
```python
def get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
    if peer_addr not in self._peer_state_locks:
        self._peer_state_locks[peer_addr] = asyncio.Lock()
    return self._peer_state_locks[peer_addr]
```

**Issue:** Locks are created on-demand but never removed when peers disconnect. Over time with peer churn, thousands of orphaned Lock objects accumulate.

**Fix:** Remove lock entries when the corresponding peer/job/workflow is cleaned up.

---

### 1.3 [MEDIUM] Latency Sample Lists Unbounded

**File:** `hyperscale/distributed/nodes/manager/state.py:135-137`

**Pattern:**
```python
self._gate_latency_samples: list[tuple[float, float]] = []
self._peer_manager_latency_samples: dict[str, list[tuple[float, float]]] = {}
self._worker_latency_samples: dict[str, list[tuple[float, float]]] = {}
```

**Issue:** No cap on sample counts. In long-running deployments, these lists grow indefinitely.

**Fix:** Use a bounded deque or implement rolling window (e.g., keep last 1000 samples or last 5 minutes).

---

### 1.4 [LOW] Recent Events List in HierarchicalFailureDetector

**File:** `hyperscale/distributed/swim/detection/hierarchical_failure_detector.py:740-744`

**Pattern:**
```python
def _record_event(self, event: FailureEvent) -> None:
    self._recent_events.append(event)
    if len(self._recent_events) > self._max_event_history:
        self._recent_events.pop(0)
```

**Issue:** Using `list.pop(0)` is O(n). For a bounded buffer, use `collections.deque(maxlen=N)`.

**Fix:** Replace with `collections.deque(maxlen=self._max_event_history)`.

---

## 2. RACE CONDITIONS

### 2.1 [HIGH] Lock Creation Race in get_*_lock() Methods

**Files:** Multiple state.py files

**Pattern:**
```python
def get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
    if peer_addr not in self._peer_state_locks:
        self._peer_state_locks[peer_addr] = asyncio.Lock()
    return self._peer_state_locks[peer_addr]
```

**Issue:** Two concurrent calls with the same key can both see `key not in dict`, both create locks, and the first one's lock gets overwritten. Callers end up with different lock instances, defeating the purpose.

**Fix:** Use `dict.setdefault()` which is atomic:
```python
def get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
    return self._peer_state_locks.setdefault(peer_addr, asyncio.Lock())
```

---

### 2.2 [MEDIUM] ConnectionPool._evict_one_idle() Called Outside Lock

**File:** `hyperscale/distributed/discovery/pool/connection_pool.py:178-182, 381-415`

**Pattern:**
```python
async with self._get_lock():
    # ... checks ...
    if self._total_connections >= self.config.max_total_connections:
        evicted = await self._evict_one_idle()  # This acquires NO lock internally
```

**Issue:** `_evict_one_idle()` iterates `self._connections` without holding the lock, while being called from within a locked context. The lock is released before the eviction completes.

**Fix:** Either hold lock during eviction or make `_evict_one_idle()` acquire its own lock.

---

### 2.3 [MEDIUM] Task Creation Without Tracking

**Files:** 
- `hyperscale/distributed/swim/detection/hierarchical_failure_detector.py:692-694`
- `hyperscale/distributed/swim/detection/suspicion_manager.py:272-274`

**Pattern:**
```python
task = asyncio.create_task(self._clear_job_suspicions_for_node(node))
self._pending_clear_tasks.add(task)
task.add_done_callback(self._pending_clear_tasks.discard)
```

**Issue:** The `add` and `add_done_callback` are not atomic. If the task completes before `add_done_callback` is registered, the discard callback won't fire and the task reference leaks.

**Fix:** Check if task is already done after adding callback, or use a safer pattern.

---

## 3. DEADLOCKS

### 3.1 [MEDIUM] Potential Lock Ordering Issues

**Files:** Multiple files with multiple locks

**Observation:** Several classes have multiple locks (e.g., `_state_lock`, `_peer_state_locks[addr]`). No documented lock ordering exists.

**Risk:** If code path A acquires lock1 then lock2, and code path B acquires lock2 then lock1, deadlock can occur.

**Fix:** 
1. Document lock ordering in each class
2. Consider using a single coarser lock where fine-grained locking isn't critical
3. Add deadlock detection in debug mode

---

### 3.2 [LOW] Await Inside Lock Context

**File:** `hyperscale/distributed/swim/detection/suspicion_manager.py:161-206`

**Pattern:**
```python
async with self._lock:
    # ... 
    await self._reschedule_timer(existing)  # Awaits while holding lock
    # ...
```

**Issue:** Awaiting while holding a lock can cause issues if the awaited operation needs the same lock or if it takes too long (blocking other operations).

**Status:** In this specific case, `_reschedule_timer` doesn't reacquire `self._lock`, so it's safe. However, this pattern is fragile.

**Recommendation:** Minimize work done under locks, release lock before await when possible.

---

## 4. DROPPED ERRORS

### 4.1 [HIGH] Bare except: pass Patterns

**Files:** 557 matches across 116 files (see grep output)

**Critical Examples:**

```python
# hyperscale/distributed/leases/job_lease.py:282-283
except Exception:
    pass

# hyperscale/distributed/taskex/task_runner.py:396-397
except Exception:
    pass

# hyperscale/distributed/discovery/pool/connection_pool.py:269-270
except Exception:
    pass  # Ignore close errors
```

**Issue:** Silently swallowing exceptions hides bugs and makes debugging nearly impossible. Per AGENTS.md: "We *do not* EVER swallow errors".

**Fix Priority:**
1. **Immediate:** Add logging to all bare `except: pass` blocks
2. **Short-term:** Categorize which are truly expected (e.g., cleanup during shutdown) vs bugs
3. **Long-term:** Convert to specific exception types with proper handling

---

### 4.2 [HIGH] Fire-and-Forget Callbacks Without Error Handling

**File:** `hyperscale/distributed/swim/detection/hierarchical_failure_detector.py:697-701`

**Pattern:**
```python
if self._on_global_death:
    try:
        self._on_global_death(node, state.incarnation)
    except Exception:
        pass
```

**Issue:** Callback errors are silently dropped. If the callback is important (like notifying job manager of node death), silent failure means the system continues with stale state.

**Fix:** At minimum, log the error. Consider whether callback failures should propagate or trigger recovery.

---

### 4.3 [MEDIUM] Circuit Breaker Errors Silently Recorded

**File:** `hyperscale/distributed/nodes/worker/progress.py:118-119, 222-223`

**Pattern:**
```python
except Exception:
    circuit.record_error()
```

**Issue:** All exceptions treated equally. A transient network error and a programming bug both just increment the error counter.

**Fix:** Log the exception, differentiate between expected errors (timeout, connection refused) and unexpected ones.

---

## 5. INVALID/HANG STATES

### 5.1 [HIGH] while True Loops Without Graceful Shutdown Check

**Files:**
- `hyperscale/distributed/jobs/worker_pool.py:456`
- `hyperscale/distributed/nodes/gate/server.py:2607`

**Need to verify:** Do these loops check a shutdown flag? If not, they could prevent clean shutdown.

---

### 5.2 [HIGH] Missing Timeout on asyncio.Event.wait()

**Files:** Multiple (need to audit)

**Pattern:**
```python
await completion_event.wait()  # No timeout
```

**Issue:** If the event is never set (due to a bug or network partition), the waiter hangs forever.

**Fix:** Always use `asyncio.wait_for(event.wait(), timeout=X)` with appropriate timeout.

---

### 5.3 [MEDIUM] Task Cancellation May Leave State Inconsistent

**File:** `hyperscale/distributed/swim/detection/suspicion_manager.py:276-289`

**Pattern:**
```python
async def _cancel_timer(self, state: SuspicionState) -> None:
    if state.node in self._timer_tokens and self._task_runner:
        token = self._timer_tokens.pop(state.node, None)
        if token:
            try:
                await self._task_runner.cancel(token)
            except Exception as e:
                self._log_warning(f"Failed to cancel timer via TaskRunner: {e}")
    state.cancel_timer()
```

**Issue:** If `_task_runner.cancel()` raises, the timer token is already popped but the task may still be running. The `state.cancel_timer()` at the end is good but only catches the fallback task case.

**Fix:** Use try/finally to ensure state is consistent regardless of cancellation success.

---

### 5.4 [MEDIUM] Orphaned asyncio.create_task() Calls

**Files:** 47 matches across 19 files

**Good Pattern (with tracking):**
```python
self._cleanup_task = asyncio.create_task(cleanup_loop())
```

**Problematic Pattern (orphaned):**
```python
asyncio.create_task(some_fire_and_forget_operation())
```

**Issue:** Per AGENTS.md: "We *never* create asyncio orphaned tasks or futures. Use the TaskRunner instead."

**Audit needed:** Review each of the 47 `asyncio.create_task` calls to ensure they're tracked and cleaned up.

---

## 6. RECOMMENDATIONS BY PRIORITY

### Immediate (CRITICAL/HIGH)

1. **Add logging to all bare `except: pass` blocks** - This is blocking debugging
2. **Fix lock creation race conditions** with `setdefault()`
3. **Audit all `asyncio.create_task` calls** for proper tracking
4. **Add TTL cleanup for defaultdict collections** in state classes
5. **Add timeouts to all `Event.wait()` calls**

### Short-term (MEDIUM)

6. Clean up orphaned lock entries when peers/jobs are removed
7. Bound latency sample lists
8. Fix ConnectionPool eviction race
9. Document lock ordering in multi-lock classes
10. Use deque for bounded event history

### Long-term (LOW)

11. Convert bare exceptions to specific types with proper handling
12. Add structured error categories for circuit breakers
13. Add deadlock detection in debug mode

---

## Appendix: Files Requiring Most Attention

1. `hyperscale/distributed/nodes/manager/state.py` - Multiple memory leak patterns
2. `hyperscale/distributed/nodes/gate/state.py` - Same patterns
3. `hyperscale/distributed/discovery/pool/connection_pool.py` - Race conditions
4. `hyperscale/distributed/swim/detection/suspicion_manager.py` - Complex async state
5. `hyperscale/distributed/taskex/task_runner.py` - Error handling
6. `hyperscale/distributed/leases/job_lease.py` - Dropped errors
