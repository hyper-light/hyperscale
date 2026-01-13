"""
Protocol defining the server interface required by message handlers.

Handlers depend on this protocol rather than HealthAwareServer directly,
enabling testability and decoupling.
"""

from typing import Protocol, runtime_checkable, Any


@runtime_checkable
class ServerInterface(Protocol):
    """
    Protocol for server operations required by message handlers.

    Handlers receive a ServerInterface rather than the full HealthAwareServer,
    making dependencies explicit and enabling mocking for tests.
    """

    # === Identity ===

    @property
    def udp_addr_slug(self) -> bytes:
        """Get this server's UDP address slug (e.g., b'127.0.0.1:9000')."""
        ...

    def get_self_udp_addr(self) -> tuple[str, int]:
        """Get this server's UDP address as tuple."""
        ...

    def udp_target_is_self(self, target: tuple[str, int]) -> bool:
        """Check if target address is this server."""
        ...

    # === State Access ===

    def read_nodes(self) -> dict[tuple[str, int], Any]:
        """Read the nodes dictionary from context."""
        ...

    async def get_current_timeout(self) -> float:
        """Get the current base timeout value."""
        ...

    def get_other_nodes(
        self, exclude: tuple[str, int] | None = None
    ) -> list[tuple[str, int]]:
        """Get list of other nodes in membership."""
        ...

    # === Peer Confirmation (AD-29) ===

    def confirm_peer(self, peer: tuple[str, int]) -> bool:
        """Mark a peer as confirmed after successful communication."""
        ...

    def is_peer_confirmed(self, peer: tuple[str, int]) -> bool:
        """Check if a peer has been confirmed."""
        ...

    # === Node State ===

    async def update_node_state(
        self,
        node: tuple[str, int],
        status: bytes,
        incarnation: int,
        timestamp: float,
    ) -> None: ...

    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: bytes,
    ) -> bool:
        """Check if a message is fresh based on incarnation."""
        ...

    # === Failure Detection ===

    async def increase_failure_detector(self, reason: str) -> None:
        """Increase LHM score (failure event)."""
        ...

    async def decrease_failure_detector(self, reason: str) -> None:
        """Decrease LHM score (success event)."""
        ...

    def get_lhm_adjusted_timeout(
        self,
        base_timeout: float,
        target_node_id: str | None = None,
    ) -> float:
        """Get timeout adjusted for current LHM."""
        ...

    # === Suspicion ===

    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """Start suspicion for a node."""
        ...

    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """Refute suspicion with higher incarnation."""
        ...

    async def broadcast_refutation(self) -> int:
        """Broadcast alive message with incremented incarnation."""
        ...

    async def broadcast_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Broadcast suspicion to cluster."""
        ...

    # === Communication ===

    async def send(
        self,
        target: tuple[str, int],
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | None:
        """Send UDP message to target."""
        ...

    async def send_if_ok(
        self,
        target: tuple[str, int],
        data: bytes,
    ) -> bytes | None:
        """Send to target if they are in OK state."""
        ...

    # === Response Building ===

    def build_ack_with_state(self) -> bytes:
        """Build ack response with embedded state."""
        ...

    def build_ack_with_state_for_addr(self, addr_slug: bytes) -> bytes:
        """Build ack response for specific address."""
        ...

    def get_embedded_state(self) -> bytes | None:
        """Get state to embed in messages."""
        ...

    # === Error Handling ===

    async def handle_error(self, error: Exception) -> None:
        """Handle a SWIM protocol error."""
        ...

    # === Metrics ===

    def increment_metric(self, name: str, value: int = 1) -> None:
        """Increment a metric counter."""
        ...

    # === Component Access ===

    @property
    def leader_election(self) -> Any:
        """Get leader election component."""
        ...

    @property
    def hierarchical_detector(self) -> Any:
        """Get hierarchical failure detector."""
        ...

    @property
    def task_runner(self) -> Any:
        """Get task runner for background operations."""
        ...

    @property
    def probe_scheduler(self) -> Any:
        """Get probe scheduler."""
        ...

    @property
    def incarnation_tracker(self) -> Any:
        """Get incarnation tracker."""
        ...

    @property
    def audit_log(self) -> Any:
        """Get audit log."""
        ...

    @property
    def indirect_probe_manager(self) -> Any:
        """Get indirect probe manager."""
        ...

    @property
    def pending_probe_acks(self) -> dict[tuple[str, int], Any]:
        """Get pending probe ack futures."""
        ...

    # === Validation ===

    async def validate_target(
        self,
        target: tuple[str, int] | None,
        message_type: bytes,
        source_addr: tuple[str, int],
    ) -> bool:
        """Validate that target is usable."""
        ...

    # === Message Parsing ===

    async def parse_incarnation_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        """Parse incarnation number from message safely."""
        ...

    async def parse_term_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        """Parse term number from message safely."""
        ...

    async def parse_leadership_claim(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, int]:
        """Parse leadership claim (term, candidate_lhm)."""
        ...

    async def parse_pre_vote_response(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, bool]:
        """Parse pre-vote response (term, granted)."""
        ...

    # === Indirect Probing ===

    async def handle_indirect_probe_response(
        self, target: tuple[str, int], is_alive: bool
    ) -> None:
        """Handle response from indirect probe."""
        ...

    async def send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        """Send probe and wait for ack."""
        ...

    # === Gossip ===

    async def safe_queue_put(
        self,
        queue: Any,
        item: tuple[int, bytes],
        node: tuple[str, int],
    ) -> bool:
        """Safely put item in node's queue."""
        ...

    async def clear_stale_state(self, node: tuple[str, int]) -> None:
        """Clear stale state for a node."""
        ...

    def update_probe_scheduler_membership(self) -> None:
        """Update probe scheduler with current membership."""
        ...

    # === Context Management ===

    async def context_with_value(self, target: tuple[str, int]) -> Any:
        """Get async context manager for target-scoped operations."""
        ...

    async def write_context(self, key: Any, value: Any) -> None:
        """Write value to context."""
        ...

    # === Leadership Broadcasting ===

    async def broadcast_leadership_message(self, message: bytes) -> None:
        """Broadcast a leadership message to all nodes."""
        ...

    async def send_to_addr(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float | None = None,
    ) -> bool:
        """Send message to address."""
        ...

    # === Gather Operations ===

    async def gather_with_errors(
        self,
        coros: list[Any],
        operation: str,
        timeout: float,
    ) -> tuple[list[Any], list[Exception]]:
        """Gather coroutines with error collection."""
        ...

    # === Cross-Cluster Operations ===

    async def build_xprobe_response(
        self,
        source_addr: tuple[str, int],
        probe_data: bytes,
    ) -> bytes | None:
        """
        Build response to cross-cluster probe.

        Subclasses (ManagerServer, GateServer) override for specific behavior.

        Args:
            source_addr: Address that sent the probe.
            probe_data: Pickled CrossClusterProbe data.

        Returns:
            Pickled CrossClusterAck or None to send xnack.
        """
        ...

    async def handle_xack_response(
        self,
        source_addr: tuple[str, int],
        ack_data: bytes,
    ) -> None:
        """
        Handle cross-cluster acknowledgment.

        Subclasses (ManagerServer, GateServer) override for specific behavior.

        Args:
            source_addr: Address that sent the ack.
            ack_data: Pickled CrossClusterAck data.
        """
        ...
