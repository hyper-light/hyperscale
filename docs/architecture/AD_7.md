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
- Worker registers `_handle_manager_failure` as `on_node_dead` callback
- On manager death: clear current manager, try alternatives
- On successful failover: call `_report_active_workflows_to_manager()`
