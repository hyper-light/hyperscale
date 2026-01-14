# FIX.md (In-Depth Rescan)

Last updated: 2026-01-14
Scope: Full in-depth rescan of `SCENARIOS.md` vs current implementation, including verification of previously reported fixes.

This document contains **current** findings only. Previously fixed items are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 1 | 🔴 Needs Fix |
| **Medium Priority** | 3 | 🟡 Should Fix |
| **Low Priority** | 1 | 🟢 Can Wait |

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

---

## 2. Medium Priority Issues

### 2.1 Worker Discovery Maintenance Loop Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/discovery.py` | 54-70 | Discovery maintenance loop ignores exceptions |

**Why this matters:** DNS discovery, failure decay, and cache cleanup can silently stop, leading to stale membership and missed recovery (SCENARIOS 2.2/24.1).

**Fix (actionable):**
- Log exceptions with loop context (dns_names, failure_decay_interval).
- Continue loop after logging; consider backoff on repeated failures.

### 2.2 Worker Cancellation Poll Loop Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/cancellation.py` | 227-241 | Per-workflow poll exceptions are ignored |

**Why this matters:** Cancellation fallback can silently fail, leaving workflows running after manager cancellation (SCENARIOS 13.4/20.3).

**Fix (actionable):**
- Log exceptions with workflow_id and manager_addr.
- Preserve current behavior but surface errors for diagnosis.

### 2.3 Client Job Status Polling Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/client/tracking.py` | 187-210 | Status polling errors silently ignored |

**Why this matters:** Client-side status can stall without visibility, hiding remote failures or protocol mismatches (SCENARIOS 8/9).

**Fix (actionable):**
- Log poll exceptions with job_id and gate address.
- Optionally add retry backoff and increment a poll failure counter.

---

## 3. Low Priority Issues

### 3.1 Cancellation Response Parse Fallback Lacks Diagnostics

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2516-2522 | `JobCancelResponse` parse failure is silently ignored before fallback |

**Why this matters:** When cancellation responses are malformed, we lose the error context while falling back to `CancelAck` parsing.

**Fix (actionable):**
- Add debug logging for the parse failure before falling back to `CancelAck`.

---

## Notes (Verified Fixes)

The following previously reported issues are confirmed fixed in current code:
- Federated health probe loop now reports errors via `on_probe_error` and checks ack timeouts.
- Worker progress flush and ACK parsing now log failures.
- Client push handlers now log exceptions before returning `b"error"`.
- Hierarchical failure detector and job suspicion manager now route errors via `on_error` callbacks.
- Lease expiry and cross-DC correlation callbacks now surface errors via on-error handlers.
