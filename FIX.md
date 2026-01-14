# FIX.md (Exhaustive Rescan)

Last updated: 2026-01-14
Scope: In-depth re-scan of `SCENARIOS.md` against current implementation with verified code references.

This document lists **current** findings only. Verified fixed items are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 4 | 🔴 Needs Fix |
| **Medium Priority** | 5 | 🟡 Should Fix |
| **Low Priority** | 2 | 🟢 Can Wait |

---

## 1. High Priority Issues

### 1.1 Job Final Result Forwarding Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2111-2121 | Forward errors return `b"forwarded"` with no logging |

**Why this matters:** Final job results can be silently dropped when a peer gate fails to forward to client callbacks (SCENARIOS 9/10).

**Fix (actionable):**
- Log the exception before returning `b"forwarded"` with job_id and callback address.
- Optionally enqueue retry via `_deliver_client_update` instead of returning immediately.

### 1.2 Missing Cleanup for Job Progress Tracking

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/state.py` | 366-373 | `cleanup_job_progress_tracking()` exists |
| `distributed/nodes/gate/server.py` | 4908-4939 | `_cleanup_single_job` never calls cleanup |

**Why this matters:** Per-job progress tracking entries (`_job_progress_sequences`, `_job_progress_seen`) accumulate indefinitely (SCENARIOS 1.2/8.3).

**Fix (actionable):**
- Call `self._modular_state.cleanup_job_progress_tracking(job_id)` during `_cleanup_single_job`.

### 1.3 Missing Cleanup for Job Update State

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/state.py` | 550-554 | `cleanup_job_update_state()` exists |
| `distributed/nodes/gate/server.py` | 4908-4939 | `_cleanup_single_job` never calls cleanup |

**Why this matters:** Update sequences/history and client positions persist after completion, causing unbounded memory growth (SCENARIOS 1.2/8.3).

**Fix (actionable):**
- Call `await self._modular_state.cleanup_job_update_state(job_id)` during `_cleanup_single_job`.

### 1.4 Timing Wheel Advance Loop Swallows Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/timing_wheel.py` | 411-423 | `_advance_loop` catches `Exception` and `pass`es |

**Why this matters:** Timing wheel drives global failure detection. Silent failures can stall suspicion expiry (SCENARIOS 3.1/11.1).

**Fix (actionable):**
- Log exceptions with loop context and continue advancing.
- Consider backoff if repeated failures occur.

---

## 2. Medium Priority Issues

### 2.1 Missing Cleanup for Cancellation State

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/state.py` | 449-452 | `cleanup_cancellation()` exists |
| `distributed/nodes/gate/server.py` | 4908-4939 | `_cleanup_single_job` never calls cleanup |

**Why this matters:** Cancellation events and error lists remain in memory after job completion (SCENARIOS 13.4/20.3).

**Fix (actionable):**
- Call `self._modular_state.cleanup_cancellation(job_id)` during `_cleanup_single_job`.

### 2.2 Windowed Stats Push Returns Early Without Cleanup When Callback Missing

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/stats_coordinator.py` | 438-439 | Missing callback returns without cleanup or logging |

**Why this matters:** Aggregated stats accumulate for jobs without callbacks (SCENARIOS 8.3).

**Fix (actionable):**
- Log missing callback and call `cleanup_job_windows(job_id)` before returning.

### 2.3 Spillover Evaluation Uses Hardcoded RTT Values

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/dispatch_coordinator.py` | 612-619 | `primary_rtt_ms=10.0`, fallback `rtt_ms=50.0` hardcoded |

**Why this matters:** Spillover decisions use fixed RTTs instead of measured latency, skewing routing (SCENARIOS 6.2).

**Fix (actionable):**
- Use observed or predicted RTTs from latency trackers rather than constants.

### 2.4 Federated Health Probe Error Callback Failures Are Silent

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/federated_health_monitor.py` | 372-381, 416-423 | `on_probe_error` failures are swallowed |

**Why this matters:** Cross-DC probe errors can be suppressed if the callback fails, obscuring partitions (SCENARIOS 24.1/24.3).

**Fix (actionable):**
- Add fallback logging when `on_probe_error` raises.

### 2.5 Job Suspicion Expiration Error Callback Failures Are Silent

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/job_suspicion_manager.py` | 324-337 | `on_error` failure is swallowed |

**Why this matters:** Job-level death notifications can be lost if `on_error` raises (SCENARIOS 6.1/11.1).

**Fix (actionable):**
- Add fallback logging when `on_error` raises.

---

## 3. Low Priority Issues

### 3.1 Cancellation Response Parse Fallback Lacks Diagnostics

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2516-2522 | `JobCancelResponse` parse failure ignored before fallback |

**Why this matters:** Malformed cancellation responses lose error context during fallback to `CancelAck`.

**Fix (actionable):**
- Add debug logging for the parse failure before fallback.

### 3.2 Dispatch Time Tracker Remove Job Not Used

| File | Lines | Issue |
|------|-------|-------|
| `distributed/routing/dispatch_time_tracker.py` | 56-60 | `remove_job()` exists but is unused |
| `distributed/nodes/gate/server.py` | 4908-4939 | `_cleanup_single_job` doesn’t call `remove_job()` |

**Why this matters:** Per-job dispatch latency entries persist until stale cleanup, delaying memory reclamation (SCENARIOS 1.2).

**Fix (actionable):**
- Call `await self._dispatch_time_tracker.remove_job(job_id)` during `_cleanup_single_job`.

---

## Notes (Verified Fixes)

The following previously reported issues are confirmed fixed in current code:
- Progress ordering uses per-update sequence, not fence token (`distributed/nodes/gate/state.py`).
- Federated health checks ack timeouts (`distributed/swim/health/federated_health_monitor.py`).
- Client push handlers log exceptions before returning `b"error"`.
- Worker progress flush and ACK parsing now log failures.
