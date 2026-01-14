# FIX.md (Intensive Deep Check)

Last updated: 2026-01-14
Scope: Intensive deep scan of `SCENARIOS.md` vs current implementation, with verified code references.

This document contains **current** findings only. Previously fixed items are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed or N/A |
| **Medium Priority** | 0 | 🟢 All Fixed |
| **Low Priority** | 0 | 🟢 All Fixed |

---

## 1. Not Applicable Issues

### 1.1 Job Routing State Cleanup - N/A

| File | Lines | Issue |
|------|-------|-------|
| `distributed/routing/gate_job_router.py` | 334-336 | `cleanup_job_state()` exists but is unused |

**Status:** Not Applicable

**Why N/A:** `GateJobRouter` is a complete routing system (AD-36) that was designed but **never integrated** into `GateServer`. Since the router is not instantiated, there is no routing state being tracked and therefore nothing to clean up. The cleanup call would be needed only if/when `GateJobRouter` is integrated.

**Future work:** If `GateJobRouter` is integrated, add `self._job_router.cleanup_job_state(job_id)` to `_cleanup_single_job`.

---

## 2. Completed Fixes (This Session)

### 2.1 Spillover Evaluation Hardcoded RTT - FIXED
- **File**: `distributed/nodes/gate/dispatch_coordinator.py`
- **Fix**: Added `observed_latency_tracker` parameter and `_get_observed_rtt_ms()` helper method
- **Changes**:
  - Added `ObservedLatencyTracker` import and parameter to `__init__`
  - Created `_get_observed_rtt_ms(datacenter_id, default_rtt_ms, min_confidence=0.3)` method
  - Replaced hardcoded `rtt_ms = 50.0` with tracker lookup (fallback to 50.0)
  - Replaced hardcoded `primary_rtt_ms=10.0` with tracker lookup (fallback to 10.0)
  - Wired `observed_latency_tracker` from `GateServer` to coordinator

### 2.2 Dispatch Time Tracker Remove Job - FIXED
- **File**: `distributed/nodes/gate/server.py`
- **Fix**: Added `await self._dispatch_time_tracker.remove_job(job_id)` to `_cleanup_single_job`

---

## 3. Previously Verified Fixes

The following issues were already fixed in the codebase:

### High Priority
- **Job Final Result Forwarding**: Uses `_record_and_send_client_update` with retry support (lines 2106-2114)

### Medium Priority
- **Worker Discovery Maintenance Loop**: Logs exceptions with context (lines 71-87)
- **Worker Cancellation Poll Loop**: Logs per-workflow and outer loop exceptions (lines 240-253, 276-286)
- **Client Job Status Polling**: Logs poll exceptions with job_id (lines 210-218)
- **Windowed Stats Missing Callback**: Logs and calls cleanup (lines 438-448)

### Low Priority
- **Cancellation Response Parse Fallback**: Logs parse failure before fallback (lines 2515-2526)

---

## Notes (Legacy Verified Fixes)

The following previously reported issues are confirmed fixed in current code:
- Federated health probe loop reports errors via `on_probe_error` and checks ack timeouts.
- Worker progress flush and ACK parsing now log failures.
- Client push handlers log exceptions before returning `b"error"`.
- Hierarchical failure detector and job suspicion manager route errors via `on_error` callbacks.
- Lease expiry and cross-DC correlation callbacks surface errors via on-error handlers.
