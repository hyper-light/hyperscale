# FIX.md (Exhaustive Rescan)

Last updated: 2026-01-14
Scope: Full rescan of `SCENARIOS.md` vs current implementation.

This document contains **current** findings only. Items previously fixed or moved have been removed.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 5 | 🔴 Needs Fix |
| **Medium Priority** | 4 | 🟡 Should Fix |
| **Low Priority** | 0 | 🟢 Can Wait |

---

## 1. High Priority Issues

### 1.1 Federated Health Probe Loop Swallows Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/health/federated_health_monitor.py` | 369-373 | Probe loop catches `Exception` and only sleeps (no logging) |

**Why this matters:** Cross-DC health drives routing and failover. Silent failures hide probe loop crashes during partitions (SCENARIOS 3.5/24).

**Fix (actionable):**
- Log the exception with datacenter list and probe interval.
- Use bounded backoff for repeated failures.

### 1.2 Worker Progress Flush Errors Are Silently Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/execution.py` | 163-167 | `send_progress` failures are swallowed in `flush_progress_buffer` |
| `distributed/nodes/worker/execution.py` | 188-223 | `run_progress_flush_loop` swallows exceptions without logging |

**Why this matters:** Progress updates are the primary signal for job liveness and timeouts (SCENARIOS 7.2/11.1). Silent drops break progress ordering and timeout detection.

**Fix (actionable):**
- Log send failures with workflow/job identifiers and manager address.
- On repeated failures, trigger leader refresh or circuit open.

### 1.3 Worker Progress ACK Parsing Fails Without Visibility

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/progress.py` | 522-555 | `WorkflowProgressAck` parse exceptions swallowed |

**Why this matters:** ACKs carry backpressure and leader updates. Silent parsing failures leave workers stuck on stale routing or backpressure state (SCENARIOS 5.1/7.2).

**Fix (actionable):**
- Log parse failures at debug level with payload size and workflow_id.
- Keep legacy compatibility but detect `b"ok"` explicitly.

### 1.4 Client Push Handlers Hide Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/client/handlers/tcp_job_status_push.py` | 82-83, 149-150 | Exceptions return `b"error"` with no logging |
| `distributed/nodes/client/handlers/tcp_windowed_stats.py` | 78-79 | Exceptions return `b"error"` with no logging |
| `distributed/nodes/client/handlers/tcp_workflow_result.py` | 118-119 | Exceptions return `b"error"` with no logging |

**Why this matters:** Client callbacks are the only visibility into results/stats. Silent failures break progress/result delivery (SCENARIOS 8–10).

**Fix (actionable):**
- Log exception details before returning `b"error"`.
- Include job_id/workflow_id where available.

### 1.5 Failure Detection Callbacks Swallow Exceptions

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/hierarchical_failure_detector.py` | 701-705, 729-734 | `_on_global_death` and `_on_job_death` callbacks swallow errors |
| `distributed/swim/detection/hierarchical_failure_detector.py` | 760-767 | Reconciliation loop swallows exceptions without logging |

**Why this matters:** These callbacks drive dead-node and job-death reactions. If they fail silently, failover and timeout logic never runs (SCENARIOS 3.6/11.1).

**Fix (actionable):**
- Log callback exceptions with node/job identifiers.
- Log reconciliation failures with cycle and current counters.

---

## 2. Medium Priority Issues

### 2.1 Job Suspicion Expiration Callback Swallows Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/swim/detection/job_suspicion_manager.py` | 321-328 | `_on_expired` callback errors swallowed |

**Why this matters:** Job-level death declarations can fail silently, leaving stuck workflows (SCENARIOS 11.1/13.4).

**Fix (actionable):**
- Log callback exceptions with job_id/node/incarnation.

### 2.2 Worker TCP Progress Handler Ignores Parse Errors

| File | Lines | Issue |
|------|-------|-------|
| `distributed/nodes/worker/handlers/tcp_progress.py` | 65-67 | ACK parse errors ignored (legacy ok only) |

**Why this matters:** Same impact as 1.3; handler should at least log non-legacy parse failures.

**Fix (actionable):**
- If data is not legacy `b"ok"`, log parse errors at debug level.

### 2.3 Lease Expiry Callback Errors Are Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/leases/job_lease.py` | 276-283 | `on_lease_expired` callback exceptions swallowed |

**Why this matters:** Expired leases trigger orphan handling and reassignment. Silent failures leave jobs in limbo (SCENARIOS 13.4/14.2).

**Fix (actionable):**
- Log exceptions with lease/job identifiers and continue processing remaining leases.

### 2.4 Cross-DC Correlation Callback Errors Are Dropped

| File | Lines | Issue |
|------|-------|-------|
| `distributed/datacenters/cross_dc_correlation.py` | 1167-1172, 1189-1195 | Partition healed/detected callbacks swallow exceptions |

**Why this matters:** Correlation events control eviction and routing decisions. Silent failures suppress alerts and response (SCENARIOS 41.20/42.2).

**Fix (actionable):**
- Log callback failures with affected DC list and timestamps.
- Keep callback isolation so one failure doesn’t block others.

---

## Notes

- Previously reported issues around federated ACK timeouts, progress ordering, target DC completion, quorum sizing, and manager leadership loss handling are confirmed resolved in the current codebase.
- This report focuses on **current** scenario-impacting gaps with exact file references.
