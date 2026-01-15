"""
Datacenter health state tracking.

Tracks datacenter manager health, registration, and backpressure.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.models import (
    ManagerHeartbeat,
    DatacenterRegistrationState,
)
from hyperscale.distributed.health import (
    ManagerHealthState,
    ManagerHealthConfig,
)
from hyperscale.distributed.reliability import BackpressureLevel


@dataclass(slots=True)
class ManagerTracking:
    """Tracks a single manager's state."""

    address: tuple[str, int]
    datacenter_id: str
    last_heartbeat: ManagerHeartbeat | None = None
    last_status_time: float = 0.0
    health_state: ManagerHealthState | None = None
    backpressure_level: BackpressureLevel = BackpressureLevel.NONE


@dataclass(slots=True)
class DCHealthState:
    """
    State container for datacenter health tracking.

    Tracks:
    - Datacenter manager addresses (TCP and UDP)
    - Per-DC registration states (AD-27)
    - Manager heartbeats and status timestamps
    - Manager health states (AD-19 three-signal model)
    - Backpressure levels from managers (AD-37)
    """

    # Datacenter -> manager TCP addresses
    datacenter_managers: dict[str, list[tuple[str, int]]] = field(default_factory=dict)

    # Datacenter -> manager UDP addresses (for SWIM)
    datacenter_managers_udp: dict[str, list[tuple[str, int]]] = field(default_factory=dict)

    # Per-DC registration state (AD-27)
    registration_states: dict[str, DatacenterRegistrationState] = field(default_factory=dict)

    # Per-DC manager status (dc_id -> {manager_addr -> heartbeat})
    manager_status: dict[str, dict[tuple[str, int], ManagerHeartbeat]] = field(default_factory=dict)

    # Per-manager last status timestamp
    manager_last_status: dict[tuple[str, int], float] = field(default_factory=dict)

    # Per-manager health state ((dc_id, manager_addr) -> health state)
    manager_health: dict[tuple[str, tuple[str, int]], ManagerHealthState] = field(default_factory=dict)

    # Health configuration for managers
    health_config: ManagerHealthConfig = field(default_factory=ManagerHealthConfig)

    # Per-manager backpressure level (AD-37)
    manager_backpressure: dict[tuple[str, int], BackpressureLevel] = field(default_factory=dict)

    # Current max backpressure delay (milliseconds)
    backpressure_delay_ms: int = 0

    # Per-DC aggregated backpressure level
    dc_backpressure: dict[str, BackpressureLevel] = field(default_factory=dict)

    def update_manager_status(
        self,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        heartbeat: ManagerHeartbeat,
        timestamp: float,
    ) -> None:
        """Update manager status with new heartbeat."""
        if datacenter_id not in self.manager_status:
            self.manager_status[datacenter_id] = {}
        self.manager_status[datacenter_id][manager_addr] = heartbeat
        self.manager_last_status[manager_addr] = timestamp

    def get_dc_backpressure_level(self, datacenter_id: str) -> BackpressureLevel:
        """Get the backpressure level for a datacenter."""
        return self.dc_backpressure.get(datacenter_id, BackpressureLevel.NONE)

    def update_dc_backpressure(self, datacenter_id: str) -> None:
        """Recalculate DC backpressure from its managers."""
        managers = self.datacenter_managers.get(datacenter_id, [])
        max_level = BackpressureLevel.NONE
        for manager_addr in managers:
            level = self.manager_backpressure.get(manager_addr, BackpressureLevel.NONE)
            if level.value > max_level.value:
                max_level = level
        self.dc_backpressure[datacenter_id] = max_level
