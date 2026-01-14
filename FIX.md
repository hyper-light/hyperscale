# FIX.md (Fresh Deep Trace)

Last updated: 2026-01-14
Scope: Full re-trace of `SCENARIOS.md` against current code paths (no cached findings).

This file lists **current verified issues only**. This pass found no unresolved scenario gaps.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 None found |
| **Medium Priority** | 0 | 🟢 None found |
| **Low Priority** | 0 | 🟢 None found |

---

## Notes (Verified Behaviors)

- Federated health handles first‑probe ACK timeouts using `last_probe_sent` as reference: `distributed/swim/health/federated_health_monitor.py:472`.
- Probe error callbacks include fallback logging to stderr on handler failure: `distributed/swim/health/federated_health_monitor.py:447`.
- Cross‑DC correlation callbacks include fallback logging when error handlers fail: `distributed/datacenters/cross_dc_correlation.py:1176`.
- Lease cleanup loop includes fallback logging when error handlers fail: `distributed/leases/job_lease.py:281`.
- Local reporter submission logs failures (best‑effort behavior): `distributed/nodes/client/reporting.py:83`.
- OOB health receive loop logs exceptions with socket context: `distributed/swim/health/out_of_band_health_channel.py:320`.
- Role validator supports strict parsing and raises on missing/invalid claims when `strict=True`: `distributed/discovery/security/role_validator.py:332`.
