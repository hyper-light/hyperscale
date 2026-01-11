"""
TCP handlers for gate state synchronization.

Handles:
- GateStateSyncRequest: State sync between peer gates

Dependencies:
- Gate state
- Job manager
- State version tracking

TODO: Extract from gate.py:
- receive_gate_state_sync_request() (lines 6043-6080)
"""

from typing import Protocol


class SyncDependencies(Protocol):
    """Protocol defining dependencies for sync handlers."""

    def get_state_snapshot(self):
        """Get current gate state snapshot for sync."""
        ...

    def apply_state_snapshot(self, snapshot, source_version: int) -> bool:
        """Apply received state snapshot. Returns True if applied."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["SyncDependencies"]
