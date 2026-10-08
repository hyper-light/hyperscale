---
ad_number: 8
name: Cores Completed for Faster Provisioning
description: Workers report their versioned free cores in progress updates so managers re-provision before a workflow finishes
---

# AD-8: Cores Completed for Faster Provisioning

**Decision**: Workers report their current free cores, stamped with the allocator's availability version, in every progress update and result; managers apply the newest report and re-provision immediately.

**Rationale**:
- Don't wait for entire workflow to complete before provisioning
- Enables pipelining of workflow execution
- Better utilization of worker capacity

**Implementation**:
- `WorkflowProgress.worker_available_cores` and `worker_cores_version` (`models/workflow_progress.py:64-66`).
  `cores_completed` is still carried, but it feeds AD-26 progress signals, not core accounting.
- Manager's `_update_worker_cores_from_workflow_progress()` (`nodes/manager/server.py:8944`) calls
  `WorkerPool.update_worker_cores_from_progress()` (`jobs/worker_pool.py:1216`) and signals the dispatcher
  when cores became available.
- Reservations are kept per dispatch; a report clears its own dispatch's reservation and every one allocated
  at or before the applied version; a report older than the last applied version never overwrites it
  (`jobs/worker_pool.py:1288-1312`).
- This replaced manager-side arithmetic on `cores_completed` (2026-10-05, REMAINING_WORK_PLAN Phase 3 "G-8"):
  the old path let every report wipe all reservations and double-booked cores. Tested by
  `tests/unit/simulation/sim/test_worker_core_accounting.py` (40 VOPR seeds).
