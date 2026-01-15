---
ad_number: 3
name: Quorum Uses Configured Cluster Size
description: Quorum calculation uses the configured cluster size, not the active member count
---

# AD-3: Quorum Uses Configured Cluster Size

**Decision**: Quorum calculation uses the **configured** cluster size, not the **active** member count.

**Rationale**:
- Prevents split-brain in network partitions
- A partition with 1 of 3 managers won't think it has quorum
- Standard Raft/Paxos behavior

**Implementation**:
```python
def _quorum_size(self) -> int:
    """Uses CONFIGURED peer count."""
    total_managers = len(self._manager_peers) + 1  # Include self
    return (total_managers // 2) + 1

def _has_quorum_available(self) -> bool:
    """Uses ACTIVE peer count for monitoring only."""
    active_count = len(self._active_manager_peers) + 1
    return active_count >= self._quorum_size()
```
