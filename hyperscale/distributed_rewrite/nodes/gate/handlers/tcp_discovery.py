"""
TCP handlers for discovery and query operations.

Handles:
- PingRequest: Health check ping
- RegisterCallback: Register progress callback
- WorkflowQueryRequest: Query workflow status
- DatacenterListRequest: List available datacenters

Dependencies:
- Datacenter health manager
- Progress callbacks
- Job manager
- Discovery service

TODO: Extract from gate.py:
- ping() (lines 7106-7176)
- register_callback() (lines 7251-7366)
- workflow_query() (lines 7437-7490)
- datacenter_list() (around line 7400)
"""

from typing import Protocol


class DiscoveryDependencies(Protocol):
    """Protocol defining dependencies for discovery handlers."""

    def get_available_datacenters(self) -> list[str]:
        """Get list of available datacenters."""
        ...

    def register_progress_callback(
        self, job_id: str, callback_addr: tuple[str, int]
    ) -> None:
        """Register callback for job progress updates."""
        ...

    def query_workflow_status(self, job_id: str, workflow_id: str):
        """Query status of a specific workflow."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["DiscoveryDependencies"]
