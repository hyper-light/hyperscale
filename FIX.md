# FIX.md (Exhaustive Rescan)

Last updated: 2026-01-14
Scope: In-depth re-scan of `SCENARIOS.md` against current implementation with verified code references.

This document lists **current** findings only. Verified fixed items are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed |
| **Medium Priority** | 0 | 🟢 All Fixed |
| **Low Priority** | 0 | 🟢 All Fixed |

---

## 1. High Priority Issues - ALL FIXED

### 1.1 Job Final Result Forwarding Swallows Errors - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2125-2137 | Forward errors now logged via `log_failure=True` |

**Status:** FIXED - `_record_and_send_client_update` called with `log_failure=True` ensures delivery failures are logged.

### 1.2 Missing Cleanup for Job Progress Tracking - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/state.py` | 366-373 | `cleanup_job_progress_tracking()` exists |
| `distributed/nodes/gate/server.py` | 5022 | Now called in `_cleanup_single_job` |

**Status:** FIXED - Added `self._modular_state.cleanup_job_progress_tracking(job_id)` to cleanup.

### 1.3 Missing Cleanup for Job Update State - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/state.py` | 550-554 | `cleanup_job_update_state()` exists |
| `distributed/nodes/gate/server.py` | 5023 | Now called in `_cleanup_single_job` |

**Status:** FIXED - Added `await self._modular_state.cleanup_job_update_state(job_id)` to cleanup.

### 1.4 Timing Wheel Advance Loop Swallows Exceptions - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/timing_wheel.py` | 423-433 | Now logs via `_on_error` callback or stderr |

**Status:** FIXED - Added `on_error` callback parameter and fallback stderr logging.

---

## 2. Medium Priority Issues - ALL FIXED

### 2.1 Missing Cleanup for Cancellation State - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/state.py` | 449-452 | `cleanup_cancellation()` exists |
| `distributed/nodes/gate/server.py` | 5024 | Now called in `_cleanup_single_job` |

**Status:** FIXED - Added `self._modular_state.cleanup_cancellation(job_id)` to cleanup.

### 2.2 Windowed Stats Push Returns Early Without Cleanup - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/stats_coordinator.py` | 438-448 | Now logs and calls cleanup |

**Status:** FIXED - Already had logging and `cleanup_job_windows(job_id)` call.

### 2.3 Spillover Evaluation Uses Hardcoded RTT Values - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/dispatch_coordinator.py` | 612-619 | Now uses `_get_observed_rtt_ms()` |

**Status:** FIXED - Uses `ObservedLatencyTracker` for actual RTT measurements.

### 2.4 Federated Health Probe Error Callback Failures Are Silent - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/federated_health_monitor.py` | 372-381, 416-423 | Now has fallback stderr logging |

**Status:** FIXED - Added fallback `print(..., file=sys.stderr)` when `on_probe_error` callback fails.

### 2.5 Job Suspicion Expiration Error Callback Failures Are Silent - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/job_suspicion_manager.py` | 324-337 | Now has fallback stderr logging |

**Status:** FIXED - Added fallback `print(..., file=sys.stderr)` when `on_error` callback fails.

---

## 3. Low Priority Issues - ALL FIXED

### 3.1 Cancellation Response Parse Fallback Lacks Diagnostics - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/gate/server.py` | 2535-2545 | Now logs parse failure before fallback |

**Status:** FIXED - Already had debug logging for parse failures.

### 3.2 Dispatch Time Tracker Remove Job Not Used - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/routing/dispatch_time_tracker.py` | 56-60 | `remove_job()` now called |
| `distributed/nodes/gate/server.py` | 5020 | Called in `_cleanup_single_job` |

**Status:** FIXED - Added `await self._dispatch_time_tracker.remove_job(job_id)` to cleanup.

---

## Notes (Verified Fixes)

The following previously reported issues are confirmed fixed in current code:
- Progress ordering uses per-update sequence, not fence token (`distributed/nodes/gate/state.py`).
- Federated health checks ack timeouts (`distributed/swim/health/federated_health_monitor.py`).
- Client push handlers log exceptions before returning `b"error"`.
- Worker progress flush and ACK parsing now log failures.
- Job routing state cleanup via `GateJobRouter.cleanup_job_state()` (AD-51).
- Dispatch failure tracking for cooldown penalty (AD-51).
- Coordinate updates wired via Vivaldi callbacks (AD-51).
- BlendedLatencyScorer integrated into candidate enrichment (AD-51).
