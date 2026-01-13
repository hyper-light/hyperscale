# Issues Identified from Scenario Tracing

This document tracks bugs, missing implementations, race conditions, and other issues
discovered during systematic tracing of SCENARIOS.md test scenarios through the codebase.

---

## Session 1 Fixes (COMPLETED)

| ID | Severity | Category | Location | Status |
|----|----------|----------|----------|--------|
| F1 | CRITICAL | Missing Method | windowed_stats_collector.py | FIXED |
| F2 | CRITICAL | Missing Method | windowed_stats_collector.py | FIXED |
| F3 | CRITICAL | Missing Method | windowed_stats_collector.py | FIXED |
| F4 | MEDIUM | Race Condition | stats_coordinator.py | FIXED |
| F5 | MEDIUM | Race Condition | crdt.py | DOCUMENTED |
| F6 | MEDIUM | Race Condition | windowed_stats_collector.py | DOCUMENTED |
| F7 | LOW | Blocking Call | tcp_windowed_stats.py | FIXED |
| F8 | LOW | Observability | gate/server.py | OPTIONAL |
| F9 | LOW | Race Condition | gate/server.py | FIXED |

---

## Session 2: Comprehensive Scenario Tracing (40+ Scenarios)

### CATEGORY A: Manager Registration & Discovery Issues

#### A1: No Stale Manager Cleanup (CRITICAL - Memory Leak)
**Location**: `gate/server.py:3058-3072` (`_discovery_maintenance_loop`)
**Issue**: Loop only decays discovery failures but never removes stale managers from:
- `_datacenter_manager_status`
- `_manager_last_status`
- `_manager_health`
- `_manager_negotiated_caps`
- `_manager_backpressure`

**Impact**: Dictionaries grow unbounded with dead manager entries.
**Status**: FIXED - `_discovery_maintenance_loop` now cleans up all stale manager state (300s threshold) from all relevant dicts

#### A2: Concurrent Manager Registration Race (CRITICAL)
**Location**: `gate/handlers/tcp_manager.py:131-134`
**Issue**: Manager status updates have no synchronization with cleanup loop.
**Impact**: Data corruption, incorrect health states.
**Status**: TODO

#### A3: Synthetic Heartbeat Not Cleaned (MEDIUM)
**Location**: `gate/handlers/tcp_manager.py:444-459`
**Issue**: Synthetic heartbeats from peer broadcasts never cleaned if real heartbeat never arrives.
**Status**: TODO

---

### CATEGORY B: Job Dispatch & Routing Issues

#### B1: DispatchTimeTracker Memory Leak (CRITICAL)
**Location**: `routing/dispatch_time_tracker.py:15-42`
**Issue**: `_dispatch_times` dict has no cleanup. Failed/timed-out jobs leave entries forever.
**Impact**: Unbounded memory growth.
**Status**: FIXED - Added `cleanup_stale_entries()` method with 600s threshold, called from discovery_maintenance_loop

#### B2: ObservedLatencyTracker Memory Leak (CRITICAL)
**Location**: `routing/observed_latency_tracker.py:24`
**Issue**: `_latencies` dict accumulates state for every DC ever seen, no cleanup.
**Status**: FIXED - Added `cleanup_stale_entries()` method with 600s threshold, called from discovery_maintenance_loop

#### B3: DispatchTimeTracker Race Condition (HIGH)
**Location**: `routing/dispatch_time_tracker.py`
**Issue**: No asyncio.Lock protecting `_dispatch_times` dict from concurrent access.
**Status**: FIXED - asyncio.Lock added at line 18, used in all methods

#### B4: ObservedLatencyTracker Race Condition (HIGH)
**Location**: `routing/observed_latency_tracker.py`
**Issue**: No asyncio.Lock protecting `_latencies` dict.
**Status**: FIXED - asyncio.Lock added at line 26, used in all methods

#### B5: Missing Cleanup Calls in GateServer (HIGH)
**Location**: `gate/server.py:450-458, 3007-3008`
**Issue**: Cleanup methods exist but never called:
- `_job_forwarding_tracker.cleanup_stale_peers()`
- `_state_manager.cleanup_stale_states()`
- Periodic cleanup of dispatch/latency trackers
**Status**: TODO

#### B6: Silent Exception in Dispatch Coordinator (MEDIUM)
**Location**: `gate/dispatch_coordinator.py:164`
**Issue**: Exception silently swallowed, sets empty workflow set.
**Status**: TODO

#### B7: Incomplete GateJobTimeoutTracker.stop() (MEDIUM)
**Location**: `jobs/gates/gate_job_timeout_tracker.py:142`
**Issue**: `_tracked_jobs` dict never cleared on shutdown.
**Status**: TODO

---

### CATEGORY C: Health Detection & Circuit Breaker Issues

#### C1: Missing xack Handler in GateServer (CRITICAL)
**Location**: `gate/server.py` (missing override of `_handle_xack_response`)
**Issue**: GateServer never processes xack responses, so:
- `_on_dc_latency()` callback never triggered
- Cross-DC correlation detector never receives latency signals
- Partition detection broken
**Status**: FIXED - `_handle_xack_response` implemented at line 1057-1085, passes ack to FederatedHealthMonitor which invokes `_on_dc_latency` callback

#### C2: No Circuit Breaker Success Recording (CRITICAL)
**Location**: `gate/server.py:1939, 2516`
**Issue**: Only `record_failure()` called, never `record_success()`.
**Impact**: Circuits get stuck OPEN forever, healthy managers excluded.
**Status**: FIXED - `record_success(manager_addr)` called on manager heartbeat at line 2422

#### C3: Missing Partition Callback Invocation (HIGH)
**Location**: `datacenters/cross_dc_correlation.py`
**Issue**: Callbacks registered but never invoked from detector.
**Status**: TODO

#### C4: Circuit Breaker Race Condition (MEDIUM)
**Location**: `health/circuit_breaker_manager.py:50-81`
**Issue**: No synchronization between `get_circuit()` and `is_circuit_open()`.
**Status**: TODO

#### C5: Memory Leak in Extension Trackers (MEDIUM)
**Location**: `swim/detection/hierarchical_failure_detector.py:191`
**Issue**: `_extension_trackers` dict grows unbounded.
**Status**: TODO

#### C6: Missing Incarnation Tracking in Circuit Breaker (MEDIUM)
**Location**: `health/circuit_breaker_manager.py`
**Issue**: Circuit doesn't reset when manager restarts with new incarnation.
**Status**: TODO

---

### CATEGORY D: Overload & Backpressure Issues

#### D1: Rate Limiter Cleanup Race Condition (CRITICAL)
**Location**: `reliability/rate_limiting.py:634-655`
**Issue**: `cleanup_inactive_clients()` not thread-safe, can race with request handling.
**Status**: TODO

#### D2: Rate Limiter Memory Leak (HIGH)
**Location**: `reliability/rate_limiting.py:419, 641-653`
**Issue**: `max_tracked_clients` config exists but not enforced.
**Impact**: Ephemeral clients accumulate unbounded.
**Status**: TODO

#### D3: Backpressure Propagation Race (HIGH)
**Location**: `gate/server.py:2401-2427`
**Issue**: `_manager_backpressure` dict updated without lock.
**Status**: TODO

#### D4: Invalid Threshold Handling (MEDIUM)
**Location**: `reliability/overload.py:283-298`
**Issue**: No validation that thresholds are in ascending order.
**Status**: TODO

#### D5: Capacity Aggregator Unbounded Growth (MEDIUM)
**Location**: `capacity/capacity_aggregator.py:56-66`
**Issue**: `_manager_heartbeats` dict has no size limit.
**Status**: TODO

#### D6: Hysteresis State Not Reset (LOW)
**Location**: `reliability/overload.py:444-454`
**Issue**: `_pending_state_count` not reset in `reset()`.
**Status**: TODO

---

### CATEGORY E: Worker Registration & Core Allocation Issues

#### E1: Missing _worker_job_last_progress Cleanup (CRITICAL - Memory Leak)
**Location**: `manager/registry.py:81-98`
**Issue**: `unregister_worker()` doesn't clean `_worker_job_last_progress`.
**Impact**: O(workers × jobs) entries never freed.
**Status**: FIXED - `unregister_worker()` now cleans up all worker job progress entries (lines 101-105)

#### E2: Missing _worker_latency_samples Cleanup (HIGH)
**Location**: `manager/registry.py:81-98`
**Issue**: `_worker_latency_samples` not cleaned on unregister.
**Impact**: 1000-entry deque per worker never freed.
**Status**: FIXED - `unregister_worker()` now cleans up worker latency samples (line 99)

#### E3: TOCTOU Race in Core Allocation (CRITICAL)
**Location**: `jobs/worker_pool.py:487-546`
**Issue**: Worker can die between selection and reservation, causing silent dispatch failures.
**Status**: TODO

#### E4: Event Race in wait_for_cores() (HIGH - Deadlock Risk)
**Location**: `jobs/worker_pool.py:674-704`
**Issue**: Event race can cause 30s timeout even when cores available.
**Status**: TODO

#### E5: Missing _worker_health_states Dict (HIGH - Runtime Crash)
**Location**: `manager/registry.py:147`
**Issue**: Code references `_worker_health_states` but it's never initialized.
**Impact**: AttributeError at runtime.
**Status**: FIXED - Dict initialized at ManagerState line 89, cleanup in unregister_worker and remove_worker_state

#### E6: Dispatch Semaphore Cleanup Issue (MEDIUM)
**Location**: `manager/registry.py:96`
**Issue**: Semaphore deleted while dispatch may be in progress.
**Status**: TODO

---

### CATEGORY F: Workflow Dispatch & Execution Issues

#### F10: Missing Dispatch Failure Cleanup (CRITICAL)
**Location**: `manager/dispatch.py:121-159`
**Issue**: No cleanup of allocated resources if dispatch fails.
**Impact**: Workflows silently lost, fence tokens leak.
**Status**: FIXED - Added `_dispatch_failure_count` to ManagerState, logging for all failure paths, circuit breaker error recording

#### F11: Dispatch vs Cancellation Race (CRITICAL)
**Location**: `jobs/workflow_dispatcher.py:528-694`
**Issue**: TOCTOU race - workflow can be dispatched after cancellation.
**Status**: FIXED - Added `_cancelling_jobs` set, cancellation checks at multiple points in dispatch flow

#### F12: Active Workflows Memory Leak (HIGH)
**Location**: `worker/workflow_executor.py:310-327`
**Issue**: Incomplete cleanup - `_workflow_cancel_events`, `_workflow_tokens`, `_workflow_id_to_name`, `_workflow_cores_completed` never removed.
**Impact**: ~4KB leaked per workflow.
**Status**: FIXED - Added cleanup of all workflow state in `remove_active_workflow()`

#### F13: Fence Token TOCTOU Race (HIGH)
**Location**: `worker/handlers/tcp_dispatch.py:80-89`
**Issue**: Fence token check-and-update not atomic.
**Impact**: At-most-once guarantee broken.
**Status**: FIXED - Added atomic `update_workflow_fence_token()` method with lock in WorkerState

#### F14: Result Sending No Fallback (HIGH)
**Location**: `worker/progress.py:283-393`
**Issue**: If all managers unavailable, result silently dropped, no retry.
**Status**: FIXED - Added `PendingResult` with bounded deque (max 1000), exponential backoff retry (5s base, max 60s, 10 retries, 300s TTL)

#### F15: Orphan Detection Incomplete (MEDIUM)
**Location**: `worker/background_loops.py:164-226`
**Issue**: Only handles grace period expiry, no timeout for stuck RUNNING workflows.
**Status**: FIXED - Added `get_stuck_workflows()` method, timeout tracking, integrated into orphan check loop

---

## Priority Order for Fixes

### Immediate (Will cause crashes or data loss):
1. E5: Missing _worker_health_states dict (AttributeError)
2. C1: Missing xack handler (partition detection broken)
3. C2: No circuit breaker success recording (managers locked out)

### Critical (Memory leaks, will cause OOM):
4. A1: No stale manager cleanup
5. B1: DispatchTimeTracker memory leak
6. B2: ObservedLatencyTracker memory leak
7. E1: Missing _worker_job_last_progress cleanup
8. F12: Active workflows memory leak

### High (Race conditions, silent failures):
9. E3: TOCTOU in core allocation
10. E4: Event race in wait_for_cores
11. F10: Missing dispatch failure cleanup
12. F11: Dispatch vs cancellation race
13. D1: Rate limiter cleanup race
14. B3/B4: Tracker race conditions

### Medium (Should fix but not urgent):
15. All remaining items

---

## Total Issues Found: 35+

| Category | Critical | High | Medium | Low |
|----------|----------|------|--------|-----|
| Manager Registration (A) | 2 | 0 | 1 | 0 |
| Job Dispatch/Routing (B) | 2 | 3 | 2 | 0 |
| Health/Circuit Breaker (C) | 2 | 1 | 3 | 0 |
| Overload/Backpressure (D) | 1 | 2 | 2 | 1 |
| Worker/Core Allocation (E) | 2 | 3 | 1 | 0 |
| Workflow Dispatch (F) | 2 | 4 | 1 | 0 |
| **Total** | **11** | **13** | **10** | **1** |
