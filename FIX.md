# FIX.md (Current Scenario Rescan)

Last updated: 2026-01-14
Scope: Full re-scan of `SCENARIOS.md` against current implementation.

This file reflects **current** findings only. Previously reported items that have been fixed or moved
have been removed to prevent confusion.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 2 | 🔴 Needs Fix |
| **Medium Priority** | 4 | 🟡 Should Fix |
| **Low Priority** | 0 | 🟢 Can Wait |

---

## 1. High Priority Issues

### 1.1 Worker Background Loops Swallow Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/background_loops.py` | 170-173, 250-253, 285-288, 354-357 | `except Exception: pass` hides failures in reap/orphan/discovery/progress loops |

**Why this matters:** These loops are critical to recovery, orphan handling, and backpressure. Silent failures violate soak/chaos scenarios and the “never swallow errors” rule.

**Fix (actionable):**
- Replace `except Exception: pass` with `await logger.log(...)` (or `task_runner_run(logger.log, ...)`) including loop name and context.
- Add basic throttling (e.g., exponential backoff or log sampling) to avoid spam during repeated failures.

### 1.2 Federated Health Probe Loop Hides Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/federated_health_monitor.py` | 369-373 | Probe loop catches `Exception` and only sleeps, no logging |

**Why this matters:** Cross-DC health is foundational for routing and failover. Silent probe errors create false health and delay detection during partitions.

**Fix (actionable):**
- Log the exception with context (datacenter list and probe interval).
- Optionally increment a failure counter and trigger backoff if repeated errors occur.

---

## 2. Medium Priority Issues

### 2.1 Worker Overload Polling Suppresses Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/backpressure.py` | 97-100 | Overload polling loop suppresses unexpected exceptions |

**Why this matters:** If resource sampling breaks, overload detection becomes stale without any visibility.

**Fix (actionable):**
- Log exceptions with CPU/memory getter context.
- Continue loop after logging to keep sampling alive.

### 2.2 Progress ACK Parsing Errors Are Silent

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/progress.py` | 554-555 | ACK parse failures ignored without logging |
| `distributed/nodes/worker/handlers/tcp_progress.py` | 65-67 | ACK parse failures ignored without logging |

**Why this matters:** If managers send malformed ACKs, backpressure and leader updates won’t apply and the issue is invisible.

**Fix (actionable):**
- Log parse errors at debug level when payload is not legacy `b"ok"`.
- Keep backward compatibility by skipping logs for the legacy response.

### 2.3 Lease Expiry Callback Errors Are Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/leases/job_lease.py` | 276-283 | Exceptions from `on_lease_expired` are swallowed |

**Why this matters:** If a gate fails to process lease expiry, orphan handling and reassignment can silently fail.

**Fix (actionable):**
- Log exceptions with lease/job identifiers and continue cleanup.
- Consider isolating per-lease failure without skipping remaining expiries.

### 2.4 Cross-DC Correlation Callback Errors Are Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/datacenters/cross_dc_correlation.py` | 1167-1172, 1189-1195 | Partition healed/detected callbacks swallow exceptions |

**Why this matters:** Partition detection events are key for gating eviction and routing. Silent callback failures can suppress alerts or policy changes.

**Fix (actionable):**
- Log callback failures with the affected datacenter list and severity.
- Keep callbacks isolated so one failure doesn’t block others.

---

## Notes

- The re-scan confirms that previously reported issues around federated ACK timeouts, progress ordering, target DC completion, quorum sizing, and manager leadership loss handling are now resolved in the current codebase.
- This document intentionally focuses on current, actionable gaps with direct scenario impact.
