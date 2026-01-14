# FIX.md (Verified Re-Examination)

Last updated: 2026-01-14
Scope: Re-examined current code paths for scenario gaps and stale findings.

This file reflects **verified, current** issues only. Previously reported items that are now fixed are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed or N/A |
| **Medium Priority** | 1 | 🟡 Should Fix |
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

---

## Notes (Verified Fixes)

The following previous findings are confirmed fixed in current code:
- Federated health probe error callback failures now print to stderr on callback failure (`distributed/swim/health/federated_health_monitor.py:372`).
- Job suspicion expiration error callback failures now print to stderr on callback failure (`distributed/swim/detection/job_suspicion_manager.py:324`).
- Job cleanup removes routing, dispatch timing, progress/update, and cancellation state (`distributed/nodes/gate/server.py:4988`).
- Windowed stats push logs missing callbacks and cleans windows (`distributed/nodes/gate/stats_coordinator.py:438`).
- Timing wheel advance loop logs errors via `_on_error` or stderr (`distributed/swim/detection/timing_wheel.py:423`).
