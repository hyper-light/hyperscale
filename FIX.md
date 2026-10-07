# FIX.md (Fresh Deep Trace)

Last updated: 2026-01-14 · **Status revised 2026-10-06:** both issues below are fixed; the current list of open items is `docs/REMAINING_LEDGER.md`.
Scope: Full re-trace of `SCENARIOS.md` against current code paths (no cached findings).

This file listed the issues verified on 2026-01-14. Both are now closed; the sections below keep the original finding and record the fix.

This is a dated trace, not a running bug list: fixes made after it (Phase 1 / Phase 9, ASSESSMENT.md delta A1-A7 and B) are recorded with their commits and tests in `docs/REMAINING_LEDGER.md` and `docs/REMAINING_WORK_PLAN.md`, and are deliberately not copied here, where a second copy would go stale.

---

## Summary

| Severity | Count (2026-01-14) | Status (2026-10-06) |
|----------|--------------------|---------------------|
| **High Priority** | 0 | Not re-traced here; later high-severity defects (e.g. ASSESSMENT.md delta A1-A7) were found and fixed after this file was written |
| **Medium Priority** | 2 | 🟢 Both fixed (§1.1, §1.2) |
| **Low Priority** | 0 | — |

---

## 1. Medium Priority Issues (closed)

### 1.1 mTLS Strict Mode Doesn’t Enforce Cert Parse Failures — FIXED

Original finding (2026-01-14): `extract_claims_from_cert()` was called without `strict=True` at the manager's worker-registration handler, the gate's manager-registration handler and the manager's `_validate_mtls_claims()`, so with `mtls_strict_mode` enabled a certificate parse failure fell back to defaults and could pass validation (Scenario 41.23).

Fix: `RoleValidator.extract_peer_claims(cert_der)` (`distributed/discovery/security/role_validator.py:294-323`) parses with `strict=self.strict_mode`, the validator's own config-wired flag, so no call site has to thread it. Callers: `ManagerServer._validate_peer_certificate_claims` (`distributed/nodes/manager/server.py`) and the gate's `_reject_certificate_claims` (`distributed/nodes/gate/handlers/tcp_manager.py`) (the manager's worker-registration handler file no longer exists). Test: `tests/unit/distributed/discovery/test_mtls_strict_claims.py`.

### 1.2 Timeout Tracker Accepts Stale Progress Reports — FIXED

Original finding (2026-01-14): `GateJobTimeoutTracker.record_progress()` stored `report.fence_token` without checking it against the datacenter's current fence, so stale reports from an old manager could delay timeout decisions (Scenario 11.1).

Fix: `record_progress`, `record_timeout` and `record_leader_transfer` first call `_admitted_report_info` (`distributed/jobs/gates/gate_job_timeout_tracker.py:184-198`), which drops a report from a datacenter the job no longer targets and rejects one whose fence token is superseded (`_reject_superseded_report`, `:139`) before `dc_last_progress` or any other state is written (`:200-215`). Test: `tests/unit/distributed/jobs/test_gate_job_timeout_tracker_fencing.py`.

---

## Notes (Verified Behaviors)

Line numbers re-verified 2026-10-07; references that drift with every edit cite the symbol instead.

- Federated health handles first‑probe ACK timeouts using `last_probe_sent`: `distributed/swim/health/federated_health_monitor.py:517,662`.
- Probe error callbacks fall back to logging when no callback is set or it fails: `FederatedHealthMonitor._report_datacenter_probe_error` (`distributed/swim/health/federated_health_monitor.py`).
- Cross‑DC correlation callbacks route failures to `on_callback_error`, and a failing handler goes to stderr: `distributed/datacenters/cross_dc_correlation_detector.py:1050-1070`.
- Lease cleanup: the job lease cleanup is a TaskRunner loop (`distributed/leases/job_lease_manager.py:189-195`, started by `GateServer._start_background_loops`); its error callback and expiry hook were removed on 2026-10-06 along with lease import/export (every lease is the local gate's own).
- Local reporter submission logs failures (best‑effort, deadline-bounded): `distributed/nodes/client/reporting.py:109-120`.
- OOB health receive loop logs exceptions with socket context: `distributed/swim/health/out_of_band_health_channel.py:361-373`.
