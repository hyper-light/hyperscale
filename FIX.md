# FIX.md (Fresh Deep Trace)

Last updated: 2026-01-14
Scope: Full re-trace of `SCENARIOS.md` against current code paths (no cached findings).

This file lists **current verified issues only**. All items below were confirmed by direct code reads.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed or N/A |
| **Medium Priority** | 3 | 🟡 Should Fix |
| **Low Priority** | 1 | 🟢 Can Wait |

---

## 1. Medium Priority Issues

### 1.1 Role Validation Falls Back to Defaults on Parse Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/discovery/security/role_validator.py` | 332-415 | Certificate parse failures return default claims instead of rejecting |

**Why this matters:** Scenario 41.23 requires cluster/env mismatch rejection and role validation. On parse failures, defaults are returned (cluster/env/role), which can allow invalid identities to pass role checks.

**Fix (actionable):**
- Add a strict mode that raises on parse errors or missing required claims.
- Log parse failures and reject connection when cluster/env/role cannot be validated.

### 1.2 Cross‑DC Correlation Callback Errors Can Be Silently Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/datacenters/cross_dc_correlation.py` | 1176-1185, 1205-1216 | `on_callback_error` failure is swallowed without fallback logging |

**Why this matters:** Scenario 24.3/41.20 relies on partition detection callbacks. If callback errors and the error handler also fails, there is no visibility.

**Fix (actionable):**
- Add fallback logging (stderr or logger) when `_on_callback_error` raises.
- Include affected datacenters and timestamp in the fallback log.

### 1.3 Lease Cleanup Error Handling Can Be Silently Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/leases/job_lease.py` | 281-302 | `on_error` callback failure is swallowed without fallback logging |

**Why this matters:** Scenario 14.1 (lease expiry) and 37.2 (cleanup) need reliable observability. If the error callback fails, lease cleanup issues become invisible.

**Fix (actionable):**
- Add fallback logging when `_on_error` raises (stderr or logger).
- Include job_id/owner and cleanup loop context.

---

## 2. Low Priority Issues

### 2.1 Local Reporter Submission Swallows Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/client/reporting.py` | 79-90 | Reporter submission exceptions are swallowed with no logging |

**Why this matters:** Scenario 35.3/39.1 expects observability during reporter failures. Silent failures make it impossible to diagnose reporter outages.

**Fix (actionable):**
- Log reporter failures with reporter type and job/workflow context.
- Keep best‑effort behavior (do not fail the job).

---

## Notes (Verified Fixes)

The following previous findings are confirmed fixed in current code:
- Job cleanup removes routing, dispatch timing, progress/update, and cancellation state (`distributed/nodes/gate/server.py:4988`).
- Job final result forwarding logs failures via `_record_and_send_client_update` (`distributed/nodes/gate/server.py:2125`).
- Windowed stats push logs missing callbacks and cleans windows (`distributed/nodes/gate/stats_coordinator.py:438`).
- Timing wheel advance loop logs errors via `_on_error` or stderr (`distributed/swim/detection/timing_wheel.py:423`).
- OOB health receive loop logs errors via `_log_error` (`distributed/swim/health/out_of_band_health_channel.py:346`).
- Reporter result push handler logs parse/handler errors (`distributed/nodes/client/handlers/tcp_reporter_result.py:75`).
