"""
Adapter that wraps HealthAwareServer to implement ServerInterface.

This adapter translates between the ServerInterface protocol expected by
handlers and the actual HealthAwareServer implementation.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from hyperscale.distributed.swim.health_aware_server import (
        HealthAwareServer,
    )


class ServerAdapter:
    """
    Adapts HealthAwareServer to ServerInterface protocol.

    This is a thin wrapper that delegates all calls to the server.
    It implements the ServerInterface protocol required by message handlers.
    """

    def __init__(self, server: "HealthAwareServer") -> None:
        """
        Initialize adapter.

        Args:
            server: The HealthAwareServer to wrap.
        """
        self._server = server

    # === Identity ===

    @property
    def udp_addr_slug(self) -> bytes:
        """Get this server's UDP address slug."""
        return self._server._udp_addr_slug

    def get_self_udp_addr(self) -> tuple[str, int]:
        """Get this server's UDP address as tuple."""
        return self._server._get_self_udp_addr()

    def udp_target_is_self(self, target: tuple[str, int]) -> bool:
        """Check if target address is this server."""
        return self._server.udp_target_is_self(target)

    # === State Access ===

    def read_nodes(self) -> dict[tuple[str, int], Any]:
        """Return node states from IncarnationTracker (AD-46)."""
        return self._server._incarnation_tracker.node_states

    async def get_current_timeout(self) -> float:
        return await self._server._context.read("current_timeout")

    def get_other_nodes(
        self, exclude: tuple[str, int] | None = None
    ) -> list[tuple[str, int]]:
        """Get list of other nodes in membership."""
        return self._server.get_other_nodes(exclude)

    # === Peer Confirmation (AD-29) ===

    def confirm_peer(self, peer: tuple[str, int]) -> bool:
        """Mark a peer as confirmed."""
        return self._server.confirm_peer(peer)

    def is_peer_confirmed(self, peer: tuple[str, int]) -> bool:
        """Check if a peer has been confirmed."""
        return self._server.is_peer_confirmed(peer)

    # === Node State ===

    async def update_node_state(
        self,
        node: tuple[str, int],
        status: bytes,
        incarnation: int,
        timestamp: float,
    ) -> None:
        await self._server.update_node_state(node, status, incarnation, timestamp)

    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: bytes,
    ) -> bool:
        """Check if a message is fresh based on incarnation."""
        return self._server.is_message_fresh(node, incarnation, status)

    # === Failure Detection ===

    async def increase_failure_detector(self, reason: str) -> None:
        """Increase LHM score."""
        await self._server.increase_failure_detector(reason)

    async def decrease_failure_detector(self, reason: str) -> None:
        """Decrease LHM score."""
        await self._server.decrease_failure_detector(reason)

    def get_lhm_adjusted_timeout(
        self,
        base_timeout: float,
        target_node_id: str | None = None,
    ) -> float:
        """Get timeout adjusted for current LHM."""
        return self._server.get_lhm_adjusted_timeout(base_timeout, target_node_id)

    # === Suspicion ===

    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """Start suspicion for a node."""
        result = await self._server.start_suspicion(node, incarnation, from_node)
        return result is not None

    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """Refute suspicion with higher incarnation."""
        return await self._server.refute_suspicion(node, incarnation)

    async def broadcast_refutation(self) -> int:
        """Broadcast alive message with incremented incarnation."""
        return await self._server.broadcast_refutation()

    async def broadcast_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Broadcast suspicion to cluster."""
        await self._server.broadcast_suspicion(node, incarnation)

    # === Communication ===

    async def send(
        self,
        target: tuple[str, int],
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | None:
        """Send UDP message to target."""
        return await self._server.send(target, data, timeout=timeout)

    async def send_if_ok(
        self,
        target: tuple[str, int],
        data: bytes,
    ) -> bytes | None:
        """Send to target if they are in OK state."""
        return await self._server.send_if_ok(target, data)

    # === Response Building ===

    def build_ack_with_state(self) -> bytes:
        """Build ack response with embedded state."""
        return self._server._build_ack_with_state()

    def build_ack_with_state_for_addr(self, addr_slug: bytes) -> bytes:
        """Build ack response for specific address."""
        return self._server._build_ack_with_state_for_addr(addr_slug)

    def get_embedded_state(self) -> bytes | None:
        """Get state to embed in messages."""
        return self._server._get_embedded_state()

    # === Error Handling ===

    async def handle_error(self, error: Exception) -> None:
        """Handle a SWIM protocol error."""
        await self._server.handle_error(error)

    # === Metrics ===

    def increment_metric(self, name: str, value: int = 1) -> None:
        """Increment a metric counter."""
        self._server._metrics.increment(name, value)

    # === Component Access ===

    @property
    def leader_election(self) -> Any:
        """Get leader election component."""
        return self._server._leader_election

    @property
    def hierarchical_detector(self) -> Any:
        """Get hierarchical failure detector."""
        return self._server._hierarchical_detector

    @property
    def task_runner(self) -> Any:
        """Get task runner."""
        return self._server._task_runner

    @property
    def probe_scheduler(self) -> Any:
        """Get probe scheduler."""
        return self._server._probe_scheduler

    @property
    def incarnation_tracker(self) -> Any:
        """Get incarnation tracker."""
        return self._server._incarnation_tracker

    @property
    def audit_log(self) -> Any:
        """Get audit log."""
        return self._server._audit_log

    @property
    def indirect_probe_manager(self) -> Any:
        """Get indirect probe manager."""
        return self._server._indirect_probe_manager

    @property
    def pending_probe_acks(self) -> dict[tuple[str, int], Any]:
        """Get pending probe ack futures."""
        return self._server._pending_probe_acks

    # === Validation ===

    async def validate_target(
        self,
        target: tuple[str, int] | None,
        message_type: bytes,
        source_addr: tuple[str, int],
    ) -> bool:
        """Validate that target is usable."""
        return await self._server._validate_target(target, message_type, source_addr)

    # === Message Parsing ===

    async def parse_incarnation_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        """Parse incarnation number from message safely."""
        return await self._server._parse_incarnation_safe(message, source_addr)

    async def parse_term_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        """Parse term number from message safely."""
        return await self._server._parse_term_safe(message, source_addr)

    async def parse_leadership_claim(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, int]:
        """Parse leadership claim (term, candidate_lhm)."""
        return await self._server._parse_leadership_claim(message, source_addr)

    async def parse_pre_vote_response(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, bool]:
        """Parse pre-vote response (term, granted)."""
        return await self._server._parse_pre_vote_response(message, source_addr)

    # === Indirect Probing ===

    async def handle_indirect_probe_response(
        self, target: tuple[str, int], is_alive: bool
    ) -> None:
        """Handle response from indirect probe."""
        await self._server.handle_indirect_probe_response(target, is_alive)

    async def send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        """Send probe and wait for ack."""
        return await self._server._send_probe_and_wait(target)

    # === Gossip ===

    async def safe_queue_put(
        self,
        queue: Any,
        item: tuple[int, bytes],
        node: tuple[str, int],
    ) -> bool:
        """Deprecated (AD-46): Use incarnation_tracker.update_node() instead."""
        return True

    async def clear_stale_state(self, node: tuple[str, int]) -> None:
        """Clear stale state for a node."""
        await self._server._clear_stale_state(node)

    def update_probe_scheduler_membership(self) -> None:
        """Update probe scheduler with current membership."""
        self._server.update_probe_scheduler_membership()

    # === Context Management ===

    async def context_with_value(self, target: tuple[str, int]) -> Any:
        return await self._server._context.with_value(target)

    async def write_context(self, key: Any, value: Any) -> None:
        await self._server._context.write(key, value)

    # === Leadership Broadcasting ===

    def broadcast_leadership_message(self, message: bytes) -> None:
        """Broadcast a leadership message to all nodes."""
        self._server._broadcast_leadership_message(message)

    async def send_to_addr(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float | None = None,
    ) -> bool:
        """Send message to address."""
        return await self._server._send_to_addr(target, message, timeout)

    # === Gather Operations ===

    async def gather_with_errors(
        self,
        coros: list[Any],
        operation: str,
        timeout: float,
    ) -> tuple[list[Any], list[Exception]]:
        """Gather coroutines with error collection."""
        return await self._server._gather_with_errors(
            coros, operation=operation, timeout=timeout
        )

    # === Cross-Cluster Operations ===

    async def build_xprobe_response(
        self,
        source_addr: tuple[str, int],
        probe_data: bytes,
    ) -> bytes | None:
        """
        Build response to cross-cluster probe.

        Delegates to server's _build_xprobe_response which is overridden
        in subclasses (ManagerServer, GateServer) for specific behavior.
        """
        return await self._server._build_xprobe_response(source_addr, probe_data)

    async def handle_xack_response(
        self,
        source_addr: tuple[str, int],
        ack_data: bytes,
    ) -> None:
        """
        Handle cross-cluster acknowledgment.

        Delegates to server's _handle_xack_response which is overridden
        in subclasses (ManagerServer, GateServer) for specific behavior.
        """
        await self._server._handle_xack_response(source_addr, ack_data)
