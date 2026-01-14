# FIX.md (Exhaustive Rescan)

Last updated: 2026-01-14
Scope: Full rescan of `SCENARIOS.md` vs current implementation.

This document contains **current** findings only. Items previously fixed or moved have been removed.

---

## Summary

| Severity | Count | Status |
|----------|-------|--------|
| **High Priority** | 0 | 🟢 All Fixed |
| **Medium Priority** | 0 | 🟢 All Fixed |
| **Low Priority** | 0 | 🟢 None |

---

## Completed Fixes

### 1. High Priority (All Fixed)

#### 1.1 Federated Health Probe Loop - FIXED
- **File**: `distributed/swim/health/federated_health_monitor.py`
- **Fix**: Added `on_probe_error` callback for probe loop and individual probe exceptions
- **Changes**: Exception handlers invoke callback with error message and affected datacenters

#### 1.2 Worker Progress Flush Errors - FIXED
- **File**: `distributed/nodes/worker/execution.py`
- **Fix**: Added logging for progress flush failures and loop errors
- **Changes**: Uses ServerDebug for per-workflow failures, ServerWarning for loop errors

#### 1.3 Worker Progress ACK Parsing - FIXED
- **File**: `distributed/nodes/worker/progress.py`
- **Fix**: Added logging for ACK parse failures when not legacy `b"ok"` payload
- **Changes**: Added `task_runner_run` parameter for sync method logging

#### 1.4 Client Push Handlers - FIXED
- **Files**:
  - `distributed/nodes/client/handlers/tcp_job_status_push.py`
  - `distributed/nodes/client/handlers/tcp_windowed_stats.py`
  - `distributed/nodes/client/handlers/tcp_workflow_result.py`
- **Fix**: Added logging before returning `b"error"` on exception
- **Changes**: All handlers now log exception details with ServerWarning

#### 1.5 Failure Detection Callbacks - FIXED
- **File**: `distributed/swim/detection/hierarchical_failure_detector.py`
- **Fix**: Added `on_error` callback for callback failures and reconciliation errors
- **Changes**: 
  - `_on_global_death` callback errors now reported
  - `_on_job_death` callback errors now reported
  - Reconciliation loop errors now reported with cycle count

### 2. Medium Priority (All Fixed)

#### 2.1 Job Suspicion Expiration - FIXED
- **File**: `distributed/swim/detection/job_suspicion_manager.py`
- **Fix**: Added `on_error` callback for `on_expired` callback failures
- **Changes**: Reports job_id and node on callback failure

#### 2.2 Worker TCP Progress Handler - FIXED
- **File**: `distributed/nodes/worker/handlers/tcp_progress.py`
- **Fix**: Added logging for non-legacy ACK parse failures
- **Changes**: Uses task_runner to log ServerDebug via server reference

#### 2.3 Lease Expiry Callback - FIXED
- **File**: `distributed/leases/job_lease.py`
- **Fix**: Added `on_error` callback for lease expiry and cleanup loop errors
- **Changes**: Reports job_id on callback failures, loop context on cleanup errors

#### 2.4 Cross-DC Correlation Callbacks - FIXED
- **File**: `distributed/datacenters/cross_dc_correlation.py`
- **Fix**: Added `_on_callback_error` for partition healed/detected callback failures
- **Changes**: Reports event type, affected datacenter list, and exception

---

## Notes

- Previously reported issues around federated ACK timeouts, progress ordering, target DC completion, quorum sizing, and manager leadership loss handling are confirmed resolved in the current codebase.
- All exception swallowing issues have been addressed with proper logging or callbacks.
- Classes without direct logger access now expose error callbacks that callers can wire to their logging infrastructure.
