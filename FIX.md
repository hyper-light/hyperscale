# FIX.md (Verified Re-Examination)

Last updated: 2026-01-14
Scope: Re-examined current code paths for scenario gaps and stale findings.

This file reflects **verified, current** issues only. Previously reported items that are now fixed are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed |
| **Medium Priority** | 0 | 🟢 All Fixed |
| **Low Priority** | 0 | 🟢 All Fixed |

---

## All Issues Fixed

### 1.1 Out-of-Band Health Receive Loop Silently Swallows Exceptions - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/out_of_band_health_channel.py` | 320-328 | Now logs errors to stderr |

**Status:** FIXED - Added `print(..., file=sys.stderr)` logging before continuing loop.

### 1.2 Federated Health Probe Error Callback Failures Are Silent - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/federated_health_monitor.py` | 380-387, 428-434 | Now has fallback stderr logging |

**Status:** FIXED - Added fallback `print(..., file=sys.stderr)` when `on_probe_error` callback fails.

### 1.3 Job Suspicion Expiration Error Callback Failures Are Silent - FIXED

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/job_suspicion_manager.py` | 336-343 | Now has fallback stderr logging |

**Status:** FIXED - Added fallback `print(..., file=sys.stderr)` when `on_error` callback fails.

---

## Notes (Verified Fixes)

The following previous findings are confirmed fixed in current code:
- Job cleanup now removes routing, dispatch timing, progress/update, and cancellation state (`distributed/nodes/gate/server.py:5019-5025`).
- Job final result forwarding logs failures via `_record_and_send_client_update` with `log_failure=True` (`distributed/nodes/gate/server.py:2125-2133`).
- Windowed stats push logs missing callbacks and cleans windows (`distributed/nodes/gate/stats_coordinator.py:438-448`).
- Timing wheel advance loop logs errors via `_on_error` callback or stderr fallback (`distributed/swim/detection/timing_wheel.py:423-435`).
- Spillover evaluation uses observed RTT from `ObservedLatencyTracker` instead of hardcoded values (`distributed/nodes/gate/dispatch_coordinator.py`).
- Dispatch time tracker `remove_job()` called in cleanup (`distributed/nodes/gate/server.py:5020`).
- Cancellation response parse fallback has debug logging (`distributed/nodes/gate/server.py:2535-2545`).
- Job routing state cleanup via `GateJobRouter.cleanup_job_state()` (AD-51).
- Dispatch failure tracking for cooldown penalty in all failure paths (AD-51).
- Coordinate updates wired via Vivaldi callbacks (AD-51).
- BlendedLatencyScorer integrated into candidate enrichment (AD-51).
