# FIX.md (Fresh Deep Trace)

Last updated: 2026-01-14
Scope: Full re-trace of `SCENARIOS.md` against current code paths (no cached findings).

This file lists **current verified issues only**. All items below were confirmed by direct code reads.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 None found |
| **Medium Priority** | 2 | 🟡 Should Fix |
| **Low Priority** | 0 | 🟢 None found |

---

## 1. Medium Priority Issues

### 1.1 mTLS Strict Mode Doesn’t Enforce Cert Parse Failures

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/manager/handlers/tcp_worker_registration.py` | 113-122 | `extract_claims_from_cert()` called without `strict=True` even when `mtls_strict_mode` is enabled |
| `distributed/nodes/gate/handlers/tcp_manager.py` | 256-265 | Same issue for manager registration at gate |
| `distributed/nodes/manager/server.py` | 3044-3052 | Same issue in `_validate_mtls_claims()` |

**Why this matters:** Scenario 41.23 requires rejecting invalid or mismatched certificates. When `mtls_strict_mode` is enabled but `strict=True` is not passed, parse failures fall back to defaults and can pass validation.

**Fix (actionable):**
- Pass `strict=self._config.mtls_strict_mode` to `RoleValidator.extract_claims_from_cert()` in all call sites.
- If strict mode is enabled, treat parse errors as validation failures.

### 1.2 Timeout Tracker Accepts Stale Progress Reports

| File | Lines | Issue |
|------|-------|-------|
| `distributed/jobs/gates/gate_job_timeout_tracker.py` | 175-205 | `record_progress()` stores `report.fence_token` but never validates it against existing per-DC fence token |

**Why this matters:** Scenario 11.1 (timeout detection) can be skewed by stale progress reports from old managers, delaying timeout decisions after leadership transfer.

**Fix (actionable):**
- Reject `JobProgressReport` and `JobTimeoutReport` entries with `fence_token` older than `dc_fence_tokens[datacenter]`.
- Only update `dc_last_progress` when the fence token is current.

---

## Notes (Verified Behaviors)

- Federated health handles first‑probe ACK timeouts using `last_probe_sent`: `distributed/swim/health/federated_health_monitor.py:472`.
- Probe error callbacks include fallback logging on handler failure: `distributed/swim/health/federated_health_monitor.py:447`.
- Cross‑DC correlation callbacks include fallback logging when error handlers fail: `distributed/datacenters/cross_dc_correlation.py:1176`.
- Lease cleanup loop includes fallback logging when error handlers fail: `distributed/leases/job_lease.py:281`.
- Local reporter submission logs failures (best‑effort): `distributed/nodes/client/reporting.py:83`.
- OOB health receive loop logs exceptions with socket context: `distributed/swim/health/out_of_band_health_channel.py:320`.
