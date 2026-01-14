# FIX.md (Intensive Deep Check)

Last updated: 2026-01-14
Scope: Intensive deep scan of `SCENARIOS.md` vs current implementation, with verified code references.

This document contains **current** findings only. Previously fixed items are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 2 | 🔴 Needs Fix |
| **Medium Priority** | 5 | 🟡 Should Fix |
| **Low Priority** | 2 | 🟢 Can Wait |

---

## 1. High Priority Issues

### 1.1 Job Final Result Forwarding Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2111-2121 | Forwarded final result errors return `b"forwarded"` with no logging |

**Why this matters:** Final job results can be silently dropped when a peer gate fails to forward results to the client callback, violating result delivery scenarios (SCENARIOS 9/10).

**Fix (actionable):**
- Log the exception before returning `b"forwarded"` with job_id and callback address.
- Optionally enqueue retry via `_deliver_client_update` rather than returning immediately.

### 1.2 Job Routing State Cleanup Missing

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 4908-4939 | `_cleanup_single_job` never calls job router cleanup |
| `distributed/routing/gate_job_router.py` | 334-336 | `cleanup_job_state()` exists but is unused |

**Why this matters:** Per-job routing state accumulates indefinitely, violating cleanup requirements and SCENARIOS 1.2/1.4.

**Fix (actionable):**
- Call `self._job_router.cleanup_job_state(job_id)` in `_cleanup_single_job` after job completion.

---

## 2. Medium Priority Issues

### 2.1 Worker Discovery Maintenance Loop Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/discovery.py` | 54-70 | Discovery maintenance loop ignores exceptions |

**Why this matters:** DNS discovery, failure decay, and cache cleanup can silently stop, leading to stale membership and missed recovery (SCENARIOS 2.2/24.1).

**Fix (actionable):**
- Log exceptions with loop context (dns_names, failure_decay_interval) and continue with backoff.

### 2.2 Worker Cancellation Poll Loop Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/cancellation.py` | 227-241 | Per-workflow poll exceptions are ignored |

**Why this matters:** Cancellation fallback can silently fail, leaving workflows running after manager cancellation (SCENARIOS 13.4/20.3).

**Fix (actionable):**
- Log exceptions with workflow_id and manager_addr.

### 2.3 Client Job Status Polling Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/client/tracking.py` | 187-210 | Status polling errors silently ignored |

**Why this matters:** Client-side status can stall without visibility, hiding remote failures or protocol mismatches (SCENARIOS 8/9).

**Fix (actionable):**
- Log poll exceptions with job_id and gate address; consider retry backoff.

### 2.4 Windowed Stats Push Returns Early Without Cleanup When Callback Missing

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/stats_coordinator.py` | 438-439 | Missing callback returns without cleanup or logging |

**Why this matters:** Windowed stats for jobs without callbacks can accumulate, leading to memory growth and stale stats (SCENARIOS 8.3).

**Fix (actionable):**
- Log missing callback and call `cleanup_job_windows(job_id)` before returning.

### 2.5 Spillover Evaluation Uses Hardcoded RTT Values

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/dispatch_coordinator.py` | 612-619 | `primary_rtt_ms=10.0` and `rtt_ms=50.0` hardcoded |

**Why this matters:** Spillover decisions are made using fixed RTTs instead of measured latency, skewing routing decisions (SCENARIOS 6.2).

**Fix (actionable):**
- Use observed or predicted RTTs from the coordinate/latency trackers instead of constants.

---

## 3. Low Priority Issues

### 3.1 Cancellation Response Parse Fallback Lacks Diagnostics

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2516-2522 | `JobCancelResponse` parse failure is silently ignored before fallback |

**Why this matters:** Malformed cancellation responses lose error context during fallback to `CancelAck`.

**Fix (actionable):**
- Add debug logging for the parse failure before fallback.

### 3.2 Dispatch Time Tracker Remove Job Not Used

| File | Lines | Issue |
|------|-------|-------|
| `distributed/routing/dispatch_time_tracker.py` | 56-60 | `remove_job()` exists but is unused |
| `distributed/nodes/gate/server.py` | 4908-4939 | `_cleanup_single_job` doesn’t call `remove_job()` |

**Why this matters:** Per-job dispatch latency entries persist until staleness cleanup, delaying memory reclamation (SCENARIOS 1.2).

**Fix (actionable):**
- Call `await self._dispatch_time_tracker.remove_job(job_id)` during `_cleanup_single_job`.

---

## Notes (Verified Fixes)

The following previously reported issues are confirmed fixed in current code:
- Federated health probe loop reports errors via `on_probe_error` and checks ack timeouts.
- Worker progress flush and ACK parsing now log failures.
- Client push handlers log exceptions before returning `b"error"`.
- Hierarchical failure detector and job suspicion manager route errors via `on_error` callbacks.
- Lease expiry and cross-DC correlation callbacks surface errors via on-error handlers.
