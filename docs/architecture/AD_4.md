---
ad_number: 4
name: Workers Are Source of Truth
description: Workers maintain authoritative state for their workflows, managers rebuild state from workers on leader election
---

# AD-4: Workers Are Source of Truth

**Decision**: Workers maintain authoritative state for their workflows. Managers rebuild state from workers on leader election.

**Rationale**:
- Workers have the actual running processes
- Eliminates single point of failure for state
- New leader can recover without distributed log

**Implementation**:
- `_on_manager_become_leader()` triggers `_sync_state_from_workers()`
- Workers respond with `WorkerStateSnapshot` containing `active_workflows`
- Manager rebuilds `_workflow_assignments` from worker responses
