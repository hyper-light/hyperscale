---
ad_number: 6
name: Manager Peer Failure Detection
description: Managers track peer liveness and quorum availability separately
---

# AD-6: Manager Peer Failure Detection

**Decision**: Managers track peer liveness and quorum availability separately.

**Rationale**:
- Need to know if quorum operations will succeed
- Leadership re-election is automatic via lease expiry
- Logging quorum status aids debugging

**Implementation**:
- `_manager_udp_to_tcp`: Maps UDP addresses to TCP addresses
- `_active_manager_peers`: Set of currently live peers
- `_on_node_dead()` checks both workers AND manager peers
- `_handle_manager_peer_failure()` updates active set
