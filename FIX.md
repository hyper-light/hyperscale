# FIX.md (Verified Re-Examination)

Last updated: 2026-01-14
Scope: Re-examined current code paths for scenario gaps and stale findings.

This file reflects **verified, current** issues only. Previously reported items that are now fixed are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed or N/A |
| **Medium Priority** | 3 | 🟡 Should Fix |
| **Low Priority** | 0 | 🟢 Can Wait |

---

## 1. Medium Priority Issues

### 1.1 Out-of-Band Health Receive Loop Silently Swallows Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/out_of_band_health_channel.py` | 321-324 | `_receive_loop` catches `Exception` and continues without logging |

**Why this matters:** OOB probes are used for high-priority health signals. Silent failures make probe loss or socket errors invisible (SCENARIOS 3.7/6.1).

**Fix (actionable):**
- Log exceptions (rate-limited) with socket info and message type.
- Continue loop after logging.

### 1.2 Federated Health Probe Error Callback Failures Are Silent

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/federated_health_monitor.py` | 372-381, 416-423 | `on_probe_error` exceptions are swallowed |

**Why this matters:** Cross-DC probe errors can be suppressed if the callback itself fails, obscuring partitions (SCENARIOS 24.1/24.3).

**Fix (actionable):**
- Add fallback logging when `on_probe_error` raises.
- Include affected datacenters and probe interval in the log.

### 1.3 Job Suspicion Expiration Error Callback Failures Are Silent

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/job_suspicion_manager.py` | 324-337 | `on_error` failure is swallowed |

**Why this matters:** Job-level failure notifications can be lost if the error callback raises, delaying recovery (SCENARIOS 6.1/11.1).

**Fix (actionable):**
- Add fallback logging when `on_error` raises.

---

## Notes (Verified Fixes)

The following previous findings are confirmed fixed in current code:
- Job cleanup now removes routing, dispatch timing, progress/update, and cancellation state (`distributed/nodes/gate/server.py:4988`).
- Job final result forwarding logs failures via `_record_and_send_client_update` (`distributed/nodes/gate/server.py:2119`).
- Windowed stats push logs missing callbacks and cleans windows (`distributed/nodes/gate/stats_coordinator.py:438`).
- Timing wheel advance loop logs errors via `_on_error` or stderr (`distributed/swim/detection/timing_wheel.py:423`).
