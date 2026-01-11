"""
Gate peer state tracking.

Tracks peer gate connections, health, and discovery state.
"""

import asyncio
from dataclasses import dataclass, field

from hyperscale.distributed_rewrite.models import (
    GateHeartbeat,
    GateInfo,
)
from hyperscale.distributed_rewrite.health import (
    GateHealthState,
    GateHealthConfig,
    LatencyTracker,
)


@dataclass(slots=True)
class GatePeerTracking:
    """Tracks a single gate peer's state."""

    udp_addr: tuple[str, int]
    tcp_addr: tuple[str, int]
    epoch: int = 0
    is_active: bool = False
    heartbeat: GateHeartbeat | None = None
    health_state: GateHealthState | None = None


@dataclass(slots=True)
class GatePeerState:
    """
    State container for gate peer tracking.

    Tracks:
    - Configured gate peer addresses (TCP and UDP)
    - Active gate peers that have sent heartbeats
    - Per-peer locks for concurrent state updates
    - Per-peer epoch for stale operation detection
    - Gate peer info from heartbeats
    - Known gates discovered via gossip
    - Gate peer health states (AD-19)
    - Latency tracking for degradation detection
    """

    # Configured peers (from initialization)
    gate_peers_tcp: list[tuple[str, int]] = field(default_factory=list)
    gate_peers_udp: list[tuple[str, int]] = field(default_factory=list)

    # Mapping from UDP to TCP addresses
    udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = field(default_factory=dict)

    # Active peers that have sent heartbeats (AD-29)
    active_peers: set[tuple[str, int]] = field(default_factory=set)

    # Per-peer locks for concurrent state updates
    peer_locks: dict[tuple[str, int], asyncio.Lock] = field(default_factory=dict)

    # Per-peer epoch for detecting stale operations
    peer_epochs: dict[tuple[str, int], int] = field(default_factory=dict)

    # Gate peer info from heartbeats (UDP addr -> heartbeat)
    peer_info: dict[tuple[str, int], GateHeartbeat] = field(default_factory=dict)

    # Known gates discovered via gossip (gate_id -> GateInfo)
    known_gates: dict[str, GateInfo] = field(default_factory=dict)

    # Gate peer health states (gate_id -> health state)
    peer_health: dict[str, GateHealthState] = field(default_factory=dict)

    # Health configuration for peer gates
    health_config: GateHealthConfig = field(default_factory=GateHealthConfig)

    def get_or_create_peer_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        """Get or create a lock for the given peer address."""
        if peer_addr not in self.peer_locks:
            self.peer_locks[peer_addr] = asyncio.Lock()
        return self.peer_locks[peer_addr]

    def increment_epoch(self, peer_addr: tuple[str, int]) -> int:
        """Increment and return the epoch for a peer address."""
        current_epoch = self.peer_epochs.get(peer_addr, 0)
        new_epoch = current_epoch + 1
        self.peer_epochs[peer_addr] = new_epoch
        return new_epoch

    def get_epoch(self, peer_addr: tuple[str, int]) -> int:
        """Get the current epoch for a peer address."""
        return self.peer_epochs.get(peer_addr, 0)
