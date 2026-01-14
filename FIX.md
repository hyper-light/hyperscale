# FIX.md (Deep Trace Verification)

Last updated: 2026-01-14
Scope: Fresh, full trace of `SCENARIOS.md` against current code paths with edge‑case review.

This file lists **current verified issues only**. Items confirmed fixed are listed in Notes.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed or N/A |
| **Medium Priority** | 2 | 🟡 Should Fix |
| **Low Priority** | 0 | 🟢 Can Wait |

---

## 1. Medium Priority Issues

### 1.1 Out-of-Band Health Receive Loop Silently Swallows Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/out_of_band_health_channel.py` | 321-324 | `_receive_loop` catches `Exception` and continues without logging |

**Why this matters:** OOB probes are used for high‑priority health signals. Silent failures hide socket errors or parsing faults (SCENARIOS 3.7/6.1).

**Fix (actionable):**
- Log exceptions (rate‑limited) with socket info and message type.
- Continue loop after logging.

### 1.2 Reporter Result Push Handler Swallows Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/client/handlers/tcp_reporter_result.py` | 75-76 | Handler returns `b"error"` without logging on parse/handler failure |

**Why this matters:** Reporter completion signals can fail silently, leaving clients unaware of reporter outcomes (SCENARIOS 12.3/35.3).

**Fix (actionable):**
- Log the exception with job_id (if parse succeeded) or raw payload length.
- Keep best‑effort return value but add visibility.

---

## Notes (Verified Fixes)

The following previous findings are confirmed fixed in current code:
- Job cleanup removes routing, dispatch timing, progress/update, and cancellation state (`distributed/nodes/gate/server.py:4988`).
- Job final result forwarding logs failures via `_record_and_send_client_update` (`distributed/nodes/gate/server.py:2125`).
- Windowed stats push logs missing callbacks and cleans windows (`distributed/nodes/gate/stats_coordinator.py:438`).
- Timing wheel advance loop logs errors via `_on_error` or stderr (`distributed/swim/detection/timing_wheel.py:423`).
- Federated health probe error callbacks log to stderr on callback failure (`distributed/swim/health/federated_health_monitor.py:372`).
- Job suspicion expiration error callback failures log to stderr (`distributed/swim/detection/job_suspicion_manager.py:324`).
