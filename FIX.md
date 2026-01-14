# FIX.md (Intensive Deep Check)

Last updated: 2026-01-14
Scope: Intensive deep scan of `SCENARIOS.md` vs current implementation, with verified code references.

This document contains **current** findings only. Previously fixed items are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 1 | 🔴 Needs Fix (blocked - requires architectural wiring) |
| **Medium Priority** | 1 | 🟡 Needs Fix (blocked - requires architectural wiring) |
| **Low Priority** | 0 | 🟢 All Fixed |

---

## 1. Blocked Issues (Require Architectural Wiring)

### 1.1 Job Routing State Cleanup Missing - BLOCKED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 4918-4950 | `_cleanup_single_job` never calls job router cleanup |
| `distributed/routing/gate_job_router.py` | 334-336 | `cleanup_job_state()` exists but is unused |

**Why this matters:** Per-job routing state accumulates indefinitely, violating cleanup requirements and SCENARIOS 1.2/1.4.

**Why blocked:** `GateJobRouter` is defined but never instantiated in `GateServer`. Requires:
1. Add `_job_router: GateJobRouter` field to server
2. Initialize in `__init__`
3. Wire up routing calls
4. Then call `self._job_router.cleanup_job_state(job_id)` in `_cleanup_single_job`

### 1.2 Spillover Evaluation Uses Hardcoded RTT Values - BLOCKED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/dispatch_coordinator.py` | 612-619 | `primary_rtt_ms=10.0` and `rtt_ms=50.0` hardcoded |

**Why this matters:** Spillover decisions are made using fixed RTTs instead of measured latency, skewing routing decisions (SCENARIOS 6.2).

**Why blocked:** `GateDispatchCoordinator` doesn't have access to latency/coordinate trackers. Requires:
1. Add latency tracker parameter to coordinator `__init__`
2. Wire tracker from server to coordinator
3. Replace hardcoded values with measured RTTs

---

## 2. Completed Fixes (This Session)

### 2.1 Dispatch Time Tracker Remove Job - FIXED
- **File**: `distributed/nodes/gate/server.py`
- **Fix**: Added `await self._dispatch_time_tracker.remove_job(job_id)` to `_cleanup_single_job`
- **Lines**: After line 4949

---

## 3. Previously Verified Fixes

The following issues were already fixed in the codebase:

### High Priority
- **1.1 Job Final Result Forwarding**: Uses `_record_and_send_client_update` with retry support (lines 2106-2114)

### Medium Priority
- **2.1 Worker Discovery Maintenance Loop**: Logs exceptions with context (lines 71-87)
- **2.2 Worker Cancellation Poll Loop**: Logs per-workflow and outer loop exceptions (lines 240-253, 276-286)
- **2.3 Client Job Status Polling**: Logs poll exceptions with job_id (lines 210-218)
- **2.4 Windowed Stats Missing Callback**: Logs and calls cleanup (lines 438-448)

### Low Priority
- **3.1 Cancellation Response Parse Fallback**: Logs parse failure before fallback (lines 2515-2526)

---

## Notes (Legacy Verified Fixes)

The following previously reported issues are confirmed fixed in current code:
- Federated health probe loop reports errors via `on_probe_error` and checks ack timeouts.
- Worker progress flush and ACK parsing now log failures.
- Client push handlers log exceptions before returning `b"error"`.
- Hierarchical failure detector and job suspicion manager route errors via `on_error` callbacks.
- Lease expiry and cross-DC correlation callbacks surface errors via on-error handlers.
