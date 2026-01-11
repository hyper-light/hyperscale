"""
TCP handlers for manager status and registration.

Handles:
- ManagerHeartbeat: Status updates from datacenter managers
- ManagerRegistrationRequest: Manager joining the cluster
- ManagerDiscoveryBroadcast: Manager discovery announcements

Dependencies:
- Datacenter health manager (AD-16)
- Manager health tracking (AD-19)
- Registration state tracking (AD-27)
- Protocol negotiation (AD-25)
- Discovery service (AD-28)
- Role validation

TODO: Extract from gate.py:
- manager_status_update() (lines 4610-4662)
- manager_register() (lines 4663-4918)
- manager_discovery() (lines 4919-5010)
"""

from typing import Protocol


class ManagerStatusDependencies(Protocol):
    """Protocol defining dependencies for manager status handlers."""

    def get_dc_registration_state(self, datacenter_id: str):
        """Get registration state for a datacenter."""
        ...

    def update_manager_health(
        self, datacenter_id: str, manager_addr: tuple[str, int], heartbeat
    ) -> None:
        """Update manager health state from heartbeat."""
        ...

    def handle_manager_backpressure(
        self, manager_addr: tuple[str, int], level, delay_ms: int
    ) -> None:
        """Handle backpressure signal from manager."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["ManagerStatusDependencies"]
