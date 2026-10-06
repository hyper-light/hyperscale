"""
State Embedder Protocol and Implementations.

This module provides a composition-based approach for embedding application
state (heartbeats) in SWIM UDP messages, enabling Serf-style passive state
dissemination.

The StateEmbedder protocol is injected into HealthAwareServer, allowing different
node types (Worker, Manager, Gate) to provide their own state without
requiring inheritance-based overrides.

Phase 6.1 Enhancement: StateEmbedders now also provide HealthPiggyback objects
for the HealthGossipBuffer, enabling O(log n) health state dissemination
alongside membership gossip.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Protocol, Callable
from hyperscale.distributed.models import WorkerHeartbeat, ManagerHeartbeat, GateHeartbeat
from hyperscale.distributed.models.coordinates import NetworkCoordinate
from hyperscale.distributed.health.tracker import HealthPiggyback
from typing import cast
from hyperscale.distributed.runtime import Clock, RealClock

from .state_embedder_shared import _DEFAULT_CLOCK
from .state_embedder_shared import _PROBE_RTT_CACHE_MAX_SIZE
from .gate_state_embedder import GateStateEmbedder
from .manager_state_embedder import ManagerStateEmbedder
from .null_state_embedder import NullStateEmbedder
from .worker_state_embedder import WorkerStateEmbedder


class StateEmbedder(Protocol):
    """
    Protocol for embedding and processing state in SWIM messages.

    Implementations provide:
    - get_state(): Returns serialized state to embed in outgoing messages
    - process_state(): Handles state received from other nodes
    - get_health_piggyback(): Returns HealthPiggyback for gossip buffer (Phase 6.1)
    """

    def get_state(self) -> bytes | None:
        """
        Get serialized state to embed in SWIM probe responses.

        Returns:
            Serialized state bytes, or None if no state to embed.
        """
        ...

    async def process_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Process embedded state received from another node.

        Args:
            state_data: Serialized state bytes from the remote node.
            source_addr: The (host, port) of the node that sent the state.
        """
        ...

    def get_health_piggyback(self) -> HealthPiggyback | None:
        """
        Get HealthPiggyback for the HealthGossipBuffer (Phase 6.1).

        This returns a compact health representation for O(log n) gossip
        dissemination. Unlike get_state() which embeds full heartbeats in
        ACK messages, this provides minimal health info for gossip on all
        SWIM messages.

        Returns:
            HealthPiggyback with current health state, or None if unavailable.
        """
        ...

    def record_probe_rtt(self, source_addr: tuple[str, int], rtt_ms: float) -> None: ...

_REHOMED = (
    NullStateEmbedder,
    WorkerStateEmbedder,
    ManagerStateEmbedder,
    GateStateEmbedder,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
