---
ad_number: 8
name: Cores Completed for Faster Provisioning
description: Workers report cores_completed in progress updates for optimistic provisioning
---

# AD-8: Cores Completed for Faster Provisioning

**Decision**: Workers report `cores_completed` in progress updates; managers optimistically update available cores.

**Rationale**:
- Don't wait for entire workflow to complete before provisioning
- Enables pipelining of workflow execution
- Better utilization of worker capacity

**Implementation**:
- `WorkflowProgress.cores_completed` field
- Manager's `_update_worker_cores_from_progress()` calculates freed cores
- Optimistic update may be superseded by next heartbeat (acceptable)
