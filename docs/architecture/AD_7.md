---
ad_number: 7
name: Worker Manager Failover
description: Workers detect manager failure via SWIM and automatically failover to backup managers
---

# AD-7: Worker Manager Failover

**Decision**: Workers detect manager failure via SWIM and automatically failover to backup managers.

**Rationale**:
- Workers must continue operating during manager transitions
- Active workflows shouldn't be lost on manager failure
- New manager needs to know about in-flight work

**Implementation**:
- Worker registers `WorkerHealthIntegration.on_node_dead` as its `on_node_dead` callback
  (`nodes/worker/server.py:485`, `nodes/worker/health.py:63`), which runs
  `_handle_manager_failure_async` (`nodes/worker/server.py:2144`) on the TaskRunner.
- On manager death: invalidate the cached TCP transport to it, mark it unhealthy, select a new
  primary manager if it was the primary (`select_new_primary_manager`), and mark every workflow
  whose job leader was that manager orphaned (held for the orphan grace period, not cancelled).
- The new manager learns in-flight work by **pull**, not push: on becoming leader it runs
  `sync_state_from_workers` and `sync_full_state_from_manager_peers` (`nodes/manager/server.py:2098-2101`,
  `nodes/manager/sync.py:100`). The original push-after-failover (`_report_active_workflows_to_manager()`)
  never ran and was deleted 2026-10-05 (REMAINING_WORK_PLAN Phase 3 "AD-7").
