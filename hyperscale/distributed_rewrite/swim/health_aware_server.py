"""
Health-Aware Server implementation with SWIM + Lifeguard protocol.

This is the main server class that integrates all SWIM protocol
components with Lifeguard enhancements for failure detection,
leader election, and application state embedding.

This server provides:
- SWIM protocol for failure detection (probes, indirect probes, suspicion)
- Lifeguard enhancements (LHM, incarnation numbers, refutation)
- Leader election with split-brain prevention
- Serf-style state embedding in SWIM messages
- Graceful degradation under load
"""

import asyncio
import random
import time
from base64 import b64decode, b64encode
from typing import Callable

from hyperscale.distributed_rewrite.server import udp
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)
from hyperscale.distributed_rewrite.swim.coordinates import CoordinateTracker
from hyperscale.distributed_rewrite.models.coordinates import NetworkCoordinate
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerDebug, ServerWarning

# Core types and utilities
from .core.types import Status, Nodes, Ctx, UpdateType, Message
from .core.node_id import NodeId, NodeAddress
from .core.errors import (
    SwimError,
    ErrorCategory,
    ErrorSeverity,
    NetworkError,
    ProbeTimeoutError,
    IndirectProbeTimeoutError,
    ProtocolError,
    MalformedMessageError,
    UnexpectedError,
    QueueFullError,
    StaleMessageError,
    ConnectionRefusedError as SwimConnectionRefusedError,
    ResourceError,
    TaskOverloadError,
    NotEligibleError,
)
from .core.error_handler import ErrorHandler, ErrorContext
from .core.resource_limits import BoundedDict
from .core.metrics import Metrics
from .core.audit import AuditLog, AuditEventType
from .core.retry import (
    retry_with_result,
    PROBE_RETRY_POLICY,
    ELECTION_RETRY_POLICY,
)

# Health monitoring
from .health.local_health_multiplier import LocalHealthMultiplier
from .health.health_monitor import EventLoopHealthMonitor
from .health.graceful_degradation import GracefulDegradation, DegradationLevel
from .health.peer_health_awareness import PeerHealthAwareness, PeerHealthAwarenessConfig

# Failure detection
from .detection.incarnation_tracker import IncarnationTracker, MessageFreshness
from .detection.suspicion_state import SuspicionState

# SuspicionManager replaced by HierarchicalFailureDetector (AD-30)
from .detection.indirect_probe_manager import IndirectProbeManager
from .detection.probe_scheduler import ProbeScheduler
from .detection.hierarchical_failure_detector import (
    HierarchicalFailureDetector,
    HierarchicalConfig,
    NodeStatus,
)

# Gossip
from .gossip.gossip_buffer import GossipBuffer, MAX_UDP_PAYLOAD
from .gossip.health_gossip_buffer import HealthGossipBuffer, HealthGossipBufferConfig

# Leadership
from .leadership.local_leader_election import LocalLeaderElection

# State embedding (Serf-style)
from .core.state_embedder import StateEmbedder, NullStateEmbedder

# Message handling (handler-based architecture)
from .message_handling import (
    MessageDispatcher,
    ServerAdapter,
    register_default_handlers,
)

# Protocol version for SWIM (AD-25)
# Used to detect incompatible nodes during join
from hyperscale.distributed_rewrite.protocol.version import CURRENT_PROTOCOL_VERSION

# SWIM protocol version prefix (included in join messages)
# Format: "v{major}.{minor}" - allows detection of incompatible nodes
SWIM_VERSION_PREFIX = (
    f"v{CURRENT_PROTOCOL_VERSION.major}.{CURRENT_PROTOCOL_VERSION.minor}".encode()
)


class HealthAwareServer(MercurySyncBaseServer[Ctx]):
    """
    Health-Aware Server with SWIM + Lifeguard Protocol and Leadership Election.

    This server implements the SWIM failure detection protocol with
    Lifeguard enhancements including:
    - Local Health Multiplier (LHM) for adaptive timeouts
    - Incarnation numbers for message ordering
    - Suspicion subprotocol with confirmation-based timeouts
    - Indirect probing via proxy nodes
    - Refutation with incarnation increment
    - Message piggybacking for efficient gossip
    - Round-robin probe scheduling
    - Hierarchical lease-based leadership with LHM eligibility
    - Pre-voting for split-brain prevention
    - Term-based resolution and fencing tokens
    """

    def __init__(
        self,
        *args,
        dc_id: str = "default",
        priority: int = 50,
        # Node role for role-aware failure detection (AD-35 Task 12.4.2)
        node_role: str | None = None,
        # State embedding (Serf-style heartbeat in SWIM messages)
        state_embedder: StateEmbedder | None = None,
        # Message deduplication settings
        dedup_cache_size: int = 2000,  # Default 2K messages (was 10K - excessive)
        dedup_window: float = 30.0,  # Seconds to consider duplicate
        # Rate limiting settings
        rate_limit_cache_size: int = 500,  # Track at most 500 senders
        rate_limit_tokens: int = 100,  # Max tokens per sender
        rate_limit_refill: float = 10.0,  # Tokens per second
        # Refutation rate limiting - prevents incarnation exhaustion attacks
        refutation_rate_limit_tokens: int = 5,  # Max refutations per window
        refutation_rate_limit_window: float = 10.0,  # Window duration in seconds
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Generate unique node identity
        self._node_id = NodeId.generate(datacenter=dc_id, priority=priority)

        # Store node role for role-aware failure detection (AD-35 Task 12.4.2)
        self._node_role: str = node_role or "worker"  # Default to worker if not specified

        # State embedder for Serf-style heartbeat embedding
        self._state_embedder: StateEmbedder = state_embedder or NullStateEmbedder()

        # Initialize SWIM components
        self._local_health = LocalHealthMultiplier()
        self._incarnation_tracker = IncarnationTracker()
        self._indirect_probe_manager = IndirectProbeManager()

        # Direct probe ACK tracking - key is target addr, value is Future set when ACK received
        self._pending_probe_acks: dict[tuple[str, int], asyncio.Future[bool]] = {}
        self._pending_probe_start: dict[tuple[str, int], float] = {}

        self._coordinate_tracker = CoordinateTracker()

        # Role-aware confirmation manager for unconfirmed peers (AD-35 Task 12.5.6)
        # Initialized after CoordinateTracker so it can use Vivaldi-based timeouts
        from hyperscale.distributed_rewrite.swim.roles.confirmation_manager import (
            RoleAwareConfirmationManager,
        )
        from hyperscale.distributed_rewrite.models.distributed import NodeRole

        self._confirmation_manager = RoleAwareConfirmationManager(
            coordinator_tracker=self._coordinate_tracker,
            send_ping=self._send_confirmation_ping,
            get_lhm_multiplier=lambda: self._local_health.get_multiplier(),
            on_peer_confirmed=self._on_confirmation_manager_peer_confirmed,
            on_peer_removed=self._on_confirmation_manager_peer_removed,
        )

        # Peer role tracking for role-aware confirmation (AD-35 Task 12.4.2)
        # Maps peer address to role. Default to WORKER if unknown (gossip pending)
        self._peer_roles: dict[tuple[str, int], NodeRole] = {}

        self._gossip_buffer = GossipBuffer()
        self._gossip_buffer.set_overflow_callback(self._on_gossip_overflow)
        self._probe_scheduler = ProbeScheduler()

        # Health gossip buffer for O(log n) health state dissemination (Phase 6.1)
        self._health_gossip_buffer = HealthGossipBuffer(
            config=HealthGossipBufferConfig(),
        )

        # Peer health awareness for adapting to peer load (Phase 6.2)
        self._peer_health_awareness = PeerHealthAwareness(
            config=PeerHealthAwarenessConfig(),
        )
        # Connect health gossip to peer awareness
        self._health_gossip_buffer.set_health_update_callback(
            self._peer_health_awareness.on_health_update
        )

        # Hierarchical failure detector for multi-layer detection (AD-30)
        # - Global layer: Machine-level liveness (via timing wheel)
        # - Job layer: Per-job responsiveness (via adaptive polling)
        # Uses polling instead of cancel/reschedule to avoid timer starvation
        self._hierarchical_detector = HierarchicalFailureDetector(
            on_global_death=self._on_suspicion_expired,
            get_n_members=self._get_member_count,
            get_lhm_multiplier=self._get_lhm_multiplier,
        )

        # Initialize leader election with configurable parameters from Env
        from hyperscale.distributed_rewrite.swim.leadership.leader_state import (
            LeaderState,
        )
        from hyperscale.distributed_rewrite.swim.leadership.leader_eligibility import (
            LeaderEligibility,
        )

        # Get leader election config from Env if available
        env = kwargs.get("env")
        if env and hasattr(env, "get_leader_election_config"):
            leader_config = env.get_leader_election_config()
            self._leader_election = LocalLeaderElection(
                dc_id=dc_id,
                heartbeat_interval=leader_config["heartbeat_interval"],
                election_timeout_base=leader_config["election_timeout_base"],
                election_timeout_jitter=leader_config["election_timeout_jitter"],
                pre_vote_timeout=leader_config["pre_vote_timeout"],
                state=LeaderState(lease_duration=leader_config["lease_duration"]),
                eligibility=LeaderEligibility(
                    max_leader_lhm=leader_config["max_leader_lhm"]
                ),
            )
        else:
            self._leader_election = LocalLeaderElection(dc_id=dc_id)

        # Message deduplication - track recently seen messages to prevent duplicates
        self._seen_messages: BoundedDict[int, float] = BoundedDict(
            max_size=dedup_cache_size,
            eviction_policy="LRA",  # Least Recently Added - old messages first
        )
        self._dedup_window: float = dedup_window
        self._dedup_stats = {"duplicates": 0, "unique": 0}

        # Rate limiting - per-sender token bucket to prevent resource exhaustion
        self._rate_limits: BoundedDict[tuple[str, int], dict] = BoundedDict(
            max_size=rate_limit_cache_size,
            eviction_policy="LRA",
        )
        self._rate_limit_tokens: int = rate_limit_tokens
        self._rate_limit_refill: float = rate_limit_refill
        self._rate_limit_stats = {"accepted": 0, "rejected": 0}

        # Refutation rate limiting - prevent incarnation exhaustion attacks
        # Configurable via init params or Env settings
        self._refutation_rate_limit_tokens: int = refutation_rate_limit_tokens
        self._refutation_rate_limit_window: float = refutation_rate_limit_window
        self._last_refutation_time: float = 0.0
        self._refutation_count_in_window: int = 0

        # Initialize error handler (logger set up after server starts)
        self._error_handler: ErrorHandler | None = None

        # Metrics collection
        self._metrics = Metrics()

        # Audit log for membership and leadership changes
        self._audit_log = AuditLog(max_events=1000)

        # Event loop health monitor (proactive CPU saturation detection)
        self._health_monitor = EventLoopHealthMonitor()

        # Graceful degradation (load shedding under pressure)
        self._degradation = GracefulDegradation()

        # Cleanup configuration
        self._cleanup_interval: float = 30.0  # Seconds between cleanup runs
        self._cleanup_task: asyncio.Task | None = None

        # Leadership event callbacks (for composition)
        # External components can register callbacks without overriding methods
        self._on_become_leader_callbacks: list[Callable[[], None]] = []
        self._on_lose_leadership_callbacks: list[Callable[[], None]] = []
        self._on_leader_change_callbacks: list[
            Callable[[tuple[str, int] | None], None]
        ] = []

        # Node status change callbacks (for composition)
        # Called when a node's status changes (e.g., becomes DEAD or rejoins)
        self._on_node_dead_callbacks: list[Callable[[tuple[str, int]], None]] = []
        self._on_node_join_callbacks: list[Callable[[tuple[str, int]], None]] = []

        # Peer confirmation tracking (AD-29: Protocol-Level Peer Confirmation)
        # Failure detection only applies to peers we've successfully communicated with.
        # This prevents false positives during cluster initialization.
        self._confirmed_peers: set[tuple[str, int]] = (
            set()
        )  # Successfully reached at least once
        self._unconfirmed_peers: set[tuple[str, int]] = (
            set()
        )  # Known but not yet reached
        self._unconfirmed_peer_added_at: dict[
            tuple[str, int], float
        ] = {}  # For stale detection
        self._peer_confirmation_callbacks: list[Callable[[tuple[str, int]], None]] = []

        # Hierarchical detector callbacks already set in __init__
        # Debug: track port for logging
        self._hierarchical_detector._node_port = self._udp_port

        # Message dispatcher for handler-based message processing
        # ServerAdapter wraps this server to implement ServerInterface protocol
        self._server_adapter = ServerAdapter(self)
        self._message_dispatcher = MessageDispatcher(self._server_adapter)
        register_default_handlers(self._message_dispatcher, self._server_adapter)

    @property
    def node_id(self) -> NodeId:
        """Get this server's unique node identifier."""
        return self._node_id

    @property
    def node_role(self) -> str:
        """Get this server's node role (AD-35 Task 12.4.4)."""
        return self._node_role

    def get_node_address(self) -> NodeAddress:
        """Get the full node address (ID + network location)."""
        host, port = self._get_self_udp_addr()
        return NodeAddress(node_id=self._node_id, host=host, port=port)

    def get_coordinate(self) -> NetworkCoordinate:
        return self._coordinate_tracker.get_coordinate()

    def update_coordinate_from_peer(
        self, peer_id: str, peer_coordinate: NetworkCoordinate, rtt_ms: float
    ) -> None:
        self._coordinate_tracker.update_peer_coordinate(
            peer_id, peer_coordinate, rtt_ms
        )

    def estimate_rtt_ms(self, peer_coordinate: NetworkCoordinate) -> float:
        return self._coordinate_tracker.estimate_rtt_ms(peer_coordinate)

    # =========================================================================
    # Leadership Event Registration (Composition Pattern)
    # =========================================================================

    def register_on_become_leader(self, callback: Callable[[], None]) -> None:
        """
        Register a callback to be invoked when this node becomes leader.

        Use this instead of overriding _on_become_leader to compose behavior.
        Callbacks are invoked in registration order after the base handling.

        Args:
            callback: Function to call when this node becomes leader.
        """
        self._on_become_leader_callbacks.append(callback)

    def register_on_lose_leadership(self, callback: Callable[[], None]) -> None:
        """
        Register a callback to be invoked when this node loses leadership.

        Args:
            callback: Function to call when leadership is lost.
        """
        self._on_lose_leadership_callbacks.append(callback)

    def register_on_leader_change(
        self,
        callback: Callable[[tuple[str, int] | None], None],
    ) -> None:
        """
        Register a callback to be invoked when the cluster leader changes.

        Args:
            callback: Function receiving the new leader address (or None).
        """
        self._on_leader_change_callbacks.append(callback)

    def register_on_node_dead(
        self,
        callback: Callable[[tuple[str, int]], None],
    ) -> None:
        """
        Register a callback to be invoked when a node is marked as DEAD.

        Use this to handle worker/peer failures without overriding methods.

        Args:
            callback: Function receiving the dead node's address.
        """
        self._on_node_dead_callbacks.append(callback)

    def register_on_node_join(
        self,
        callback: Callable[[tuple[str, int]], None],
    ) -> None:
        """
        Register a callback to be invoked when a node joins or rejoins the cluster.

        Use this to handle worker/peer recovery without overriding methods.

        Args:
            callback: Function receiving the joining node's address.
        """
        self._on_node_join_callbacks.append(callback)

    def register_on_peer_confirmed(
        self,
        callback: Callable[[tuple[str, int]], None],
    ) -> None:
        """
        Register a callback to be invoked when a peer is confirmed.

        Confirmation occurs on the first successful communication with a peer.
        Use this to add peers to active tracking only after confirmation.

        Args:
            callback: Function receiving the confirmed peer's address.
        """
        self._peer_confirmation_callbacks.append(callback)

    # =========================================================================
    # Peer Confirmation (AD-29)
    # =========================================================================

    def add_unconfirmed_peer(
        self, peer: tuple[str, int], role: str | None = None
    ) -> None:
        """
        Add a peer from configuration as unconfirmed (AD-29 & AD-35 compliant).

        Unconfirmed peers are probed but failure detection does NOT apply
        until we successfully communicate with them at least once.

        This updates both the local tracking sets AND the incarnation tracker
        to maintain a formal UNCONFIRMED state in the state machine.

        Args:
            peer: The UDP address of the peer to track.
            role: Optional role hint (gate/manager/worker). Defaults to worker.
        """
        if peer == self._get_self_udp_addr():
            return  # Don't track self

        if peer in self._confirmed_peers:
            return  # Already confirmed, no action needed

        # Check incarnation tracker - don't demote confirmed nodes
        if self._incarnation_tracker.is_node_confirmed(peer):
            return

        if peer not in self._unconfirmed_peers:
            self._unconfirmed_peers.add(peer)
            self._unconfirmed_peer_added_at[peer] = time.monotonic()
            # AD-29: Add to incarnation tracker with formal UNCONFIRMED state
            self._incarnation_tracker.add_unconfirmed_node(peer)

            # AD-35 Task 12.5.6: Track with RoleAwareConfirmationManager
            from hyperscale.distributed_rewrite.models.distributed import NodeRole

            # Store peer role (default to WORKER if unknown)
            if role:
                try:
                    self._peer_roles[peer] = NodeRole(role.lower())
                except ValueError:
                    self._peer_roles[peer] = NodeRole.WORKER
            else:
                self._peer_roles[peer] = NodeRole.WORKER

            # Generate peer_id from address
            peer_id = f"{peer[0]}:{peer[1]}"

            # Track with confirmation manager (async operation - run in background)
            self._task_runner.run(
                self._confirmation_manager.track_unconfirmed_peer,
                peer_id,
                peer,
                self._peer_roles[peer],
            )

    def confirm_peer(self, peer: tuple[str, int], incarnation: int = 0) -> bool:
        """
        Mark a peer as confirmed after successful communication (AD-29 compliant).

        This transitions the peer from UNCONFIRMED to OK state in both the
        local tracking and the formal incarnation tracker state machine,
        enabling failure detection for this peer.

        Args:
            peer: The UDP address of the peer to confirm.
            incarnation: The peer's incarnation number from the confirming message.

        Returns:
            True if peer was newly confirmed, False if already confirmed.
        """
        if peer == self._get_self_udp_addr():
            return False  # Don't confirm self

        if peer in self._confirmed_peers:
            return False  # Already confirmed

        # Transition from unconfirmed to confirmed
        self._unconfirmed_peers.discard(peer)
        self._unconfirmed_peer_added_at.pop(peer, None)
        self._confirmed_peers.add(peer)

        # AD-29: Update incarnation tracker with formal state transition
        # This transitions UNCONFIRMED → OK in the state machine
        self._incarnation_tracker.confirm_node(peer, incarnation)

        # AD-35 Task 12.5.6: Notify RoleAwareConfirmationManager
        peer_id = f"{peer[0]}:{peer[1]}"
        self._task_runner.run(self._confirmation_manager.confirm_peer, peer_id)

        # Invoke confirmation callbacks
        for callback in self._peer_confirmation_callbacks:
            try:
                callback(peer)
            except Exception as e:
                self._task_runner.run(
                    self.handle_exception, e, "on_peer_confirmed_callback"
                )

        return True

    def is_peer_confirmed(self, peer: tuple[str, int]) -> bool:
        """
        Check if a peer has been confirmed (AD-29 compliant).

        Checks both local tracking set and formal incarnation tracker state.
        """
        # Check local set first (fast path)
        if peer in self._confirmed_peers:
            return True
        # Fall back to incarnation tracker for formal state
        return self._incarnation_tracker.is_node_confirmed(peer)

    def is_peer_unconfirmed(self, peer: tuple[str, int]) -> bool:
        """
        Check if a peer is known but unconfirmed (AD-29 compliant).

        Checks both local tracking set and formal incarnation tracker state.
        """
        if peer in self._unconfirmed_peers:
            return True
        return self._incarnation_tracker.is_node_unconfirmed(peer)

    def get_confirmed_peers(self) -> set[tuple[str, int]]:
        """Get the set of confirmed peers."""
        return self._confirmed_peers.copy()

    def get_unconfirmed_peers(self) -> set[tuple[str, int]]:
        """Get the set of unconfirmed peers."""
        return self._unconfirmed_peers.copy()

    def can_suspect_peer(self, peer: tuple[str, int]) -> bool:
        """
        Check if a peer can be suspected (AD-29 Task 12.3.4).

        Per AD-29: Only confirmed peers can transition to SUSPECT.
        UNCONFIRMED peers cannot be suspected.

        Returns:
            True if peer can be suspected
        """
        return self._incarnation_tracker.can_suspect_node(peer)

    async def _send_confirmation_ping(
        self, peer_id: str, peer_address: tuple[str, int]
    ) -> bool:
        """
        Send a confirmation ping to an unconfirmed peer (AD-35 Task 12.5.4).

        Used by RoleAwareConfirmationManager for proactive confirmation.

        Args:
            peer_id: Peer node ID
            peer_address: Peer UDP address

        Returns:
            True if ping was sent successfully, False otherwise
        """
        try:
            # Send a direct probe (which will include gossip updates)
            await self._send_probe(peer_address)
            return True
        except Exception as send_error:
            await self._logger.log(
                ServerDebug(
                    message=f"Confirmation ping to {peer_id} failed: {send_error}",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.full,
                )
            )
            return False

    async def _on_confirmation_manager_peer_confirmed(self, peer_id: str) -> None:
        """
        Callback when RoleAwareConfirmationManager confirms a peer (AD-35 Task 12.5.6).

        Args:
            peer_id: Peer node ID that was confirmed
        """
        await self._logger.log(
            ServerDebug(
                message=f"RoleAwareConfirmationManager confirmed peer {peer_id}",
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.full,
            )
        )

    async def _on_confirmation_manager_peer_removed(
        self, peer_id: str, reason: str
    ) -> None:
        """
        Callback when RoleAwareConfirmationManager removes a peer (AD-35 Task 12.5.6).

        Args:
            peer_id: Peer node ID that was removed
            reason: Reason for removal
        """
        await self._logger.log(
            ServerDebug(
                message=f"RoleAwareConfirmationManager removed peer {peer_id}: {reason}",
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.full,
            )
        )

    def remove_peer_tracking(self, peer: tuple[str, int]) -> None:
        """
        Remove a peer from all confirmation tracking (AD-29 Task 12.3.6).

        Use when a peer is intentionally removed from the cluster.
        Also removes from incarnation tracker state machine.
        """
        self._confirmed_peers.discard(peer)
        self._unconfirmed_peers.discard(peer)
        self._unconfirmed_peer_added_at.pop(peer, None)
        # AD-29: Also remove from formal state machine
        self._incarnation_tracker.remove_node(peer)

    # =========================================================================
    # Hierarchical Failure Detection
    # =========================================================================

    def init_hierarchical_detector(
        self,
        config: HierarchicalConfig | None = None,
        on_global_death: Callable[[tuple[str, int], int], None] | None = None,
        on_job_death: Callable[[str, tuple[str, int], int], None] | None = None,
        get_job_n_members: Callable[[str], int] | None = None,
    ) -> HierarchicalFailureDetector:
        """
        Initialize the hierarchical failure detector for multi-layer detection.

        This is optional - subclasses that need job-layer detection should call
        this during their initialization.

        Args:
            config: Configuration for hierarchical detection.
            on_global_death: Callback when node is declared dead at global level.
            on_job_death: Callback when node is declared dead for specific job.
            get_job_n_members: Callback to get member count for a job.

        Returns:
            The initialized HierarchicalFailureDetector.
        """
        self._hierarchical_detector = HierarchicalFailureDetector(
            config=config,
            on_global_death=on_global_death,
            on_job_death=on_job_death,
            get_n_members=self._get_member_count,
            get_job_n_members=get_job_n_members,
            get_lhm_multiplier=self._get_lhm_multiplier,
        )
        return self._hierarchical_detector

    async def start_hierarchical_detector(self) -> None:
        """Start the hierarchical failure detector if initialized."""
        if self._hierarchical_detector:
            await self._hierarchical_detector.start()

    async def stop_hierarchical_detector(self) -> None:
        """Stop the hierarchical failure detector if running."""
        if self._hierarchical_detector:
            await self._hierarchical_detector.stop()

    def get_hierarchical_detector(self) -> HierarchicalFailureDetector | None:
        """Get the hierarchical failure detector if initialized."""
        return self._hierarchical_detector

    async def suspect_node_global(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """
        Start or update a global (machine-level) suspicion.

        Convenience method that delegates to the hierarchical detector.

        Returns False if detector not initialized.
        """
        if not self._hierarchical_detector:
            return False
        return await self._hierarchical_detector.suspect_global(
            node, incarnation, from_node
        )

    async def suspect_node_for_job(
        self,
        job_id: str,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """
        Start or update a job-specific suspicion.

        Convenience method that delegates to the hierarchical detector.

        Returns False if detector not initialized.
        """
        if not self._hierarchical_detector:
            return False
        return await self._hierarchical_detector.suspect_job(
            job_id, node, incarnation, from_node
        )

    async def is_node_alive_global(self, node: tuple[str, int]) -> bool:
        """
        Check if a node is alive at the global (machine) level.

        Returns True if detector not initialized (fail-open).
        """
        if not self._hierarchical_detector:
            return True
        return await self._hierarchical_detector.is_alive_global(node)

    def is_node_alive_for_job(self, job_id: str, node: tuple[str, int]) -> bool:
        """
        Check if a node is alive for a specific job.

        Returns True if detector not initialized (fail-open).
        """
        if not self._hierarchical_detector:
            return True
        return self._hierarchical_detector.is_alive_for_job(job_id, node)

    async def clear_job_suspicions(self, job_id: str) -> int:
        """
        Clear all suspicions for a completed job.

        Returns 0 if detector not initialized.
        """
        if not self._hierarchical_detector:
            return 0
        return await self._hierarchical_detector.clear_job(job_id)

    async def get_node_hierarchical_status(
        self,
        node: tuple[str, int],
    ) -> NodeStatus | None:
        """
        Get comprehensive status of a node.

        Returns None if detector not initialized.
        """
        if not self._hierarchical_detector:
            return None
        return await self._hierarchical_detector.get_node_status(node)

    def _get_lhm_multiplier(self) -> float:
        """Get the current LHM timeout multiplier."""
        return self._local_health.get_multiplier()

    def _setup_error_handler(self) -> None:
        """Initialize error handler after server is started."""
        self._error_handler = ErrorHandler(
            logger=self._udp_logger,
            increment_lhm=self.increase_failure_detector,
            node_id=self._node_id.short,
        )

        # Register recovery actions
        self._error_handler.register_recovery(
            ErrorCategory.NETWORK,
            self._recover_from_network_errors,
        )

    async def _recover_from_network_errors(self) -> None:
        """Recovery action for network errors - reset connections."""
        # Log recovery attempt
        if self._error_handler:
            self._error_handler.record_success(ErrorCategory.NETWORK)

    async def handle_error(self, error: SwimError) -> None:
        """Handle a SWIM protocol error."""
        # Track error by category
        if error.category == ErrorCategory.NETWORK:
            self._metrics.increment("network_errors")
        elif error.category == ErrorCategory.PROTOCOL:
            self._metrics.increment("protocol_errors")
        elif error.category == ErrorCategory.RESOURCE:
            self._metrics.increment("resource_errors")

        if self._error_handler:
            await self._error_handler.handle(error)

    async def handle_exception(self, exc: BaseException, operation: str) -> None:
        """Handle a raw exception, converting to SwimError."""
        if self._error_handler:
            await self._error_handler.handle_exception(exc, operation)

    def is_network_circuit_open(self) -> bool:
        """Check if the network circuit breaker is open."""
        if self._error_handler:
            return self._error_handler.is_circuit_open(ErrorCategory.NETWORK)
        return False

    def is_election_circuit_open(self) -> bool:
        """Check if the election circuit breaker is open."""
        if self._error_handler:
            return self._error_handler.is_circuit_open(ErrorCategory.ELECTION)
        return False

    def record_network_success(self) -> None:
        """Record a successful network operation (helps circuit recover)."""
        if self._error_handler:
            self._error_handler.record_success(ErrorCategory.NETWORK)

    def _setup_task_runner_integration(self) -> None:
        """Integrate TaskRunner with SWIM components."""
        # Hierarchical detector manages its own tasks via asyncio
        pass

    def _setup_health_monitor(self) -> None:
        """Set up event loop health monitor with LHM integration."""
        self._health_monitor.set_callbacks(
            on_lag_detected=self._on_event_loop_lag,
            on_critical_lag=self._on_event_loop_critical,
            on_recovered=self._on_event_loop_recovered,
            task_runner=self._task_runner,
        )

    async def _on_event_loop_lag(self, lag_ratio: float) -> None:
        """Called when event loop lag is detected."""
        # Proactively increment LHM before failures occur
        await self.increase_failure_detector("event_loop_lag")

    async def _on_event_loop_critical(self, lag_ratio: float) -> None:
        """Called when event loop is critically overloaded."""
        # More aggressive LHM increment: +2 total for critical (vs +1 for lag)
        # This helps the node back off faster when severely overloaded
        await self.increase_failure_detector("event_loop_critical")
        await self.increase_failure_detector("event_loop_critical")

        # Log TaskOverloadError for monitoring
        await self.handle_error(
            TaskOverloadError(
                task_count=len(self._task_runner.tasks),
                max_tasks=100,  # Nominal limit
            )
        )

    async def _on_event_loop_recovered(self) -> None:
        """Called when event loop recovers from degraded state."""
        await self.decrease_failure_detector("event_loop_recovered")

    async def start_health_monitor(self) -> None:
        """Start the event loop health monitor."""
        self._setup_health_monitor()
        self._setup_graceful_degradation()
        await self._health_monitor.start()

    async def stop_health_monitor(self) -> None:
        """Stop the event loop health monitor."""
        await self._health_monitor.stop()

    def get_health_stats(self) -> dict:
        """Get event loop health statistics."""
        return self._health_monitor.get_stats()

    def is_event_loop_degraded(self) -> bool:
        """Check if event loop is in degraded state."""
        return self._health_monitor.is_degraded

    def _setup_graceful_degradation(self) -> None:
        """Set up graceful degradation with health callbacks."""
        self._degradation.set_health_callbacks(
            get_lhm=lambda: self._local_health.score,
            get_event_loop_lag=lambda: self._health_monitor.average_lag_ratio,
            on_level_change=self._on_degradation_level_change,
        )

    def _on_degradation_level_change(
        self,
        old_level: DegradationLevel,
        new_level: DegradationLevel,
    ) -> None:
        """Handle degradation level changes."""
        direction = "increased" if new_level.value > old_level.value else "decreased"
        policy = self._degradation.get_current_policy()

        # Log TaskOverloadError for severe/critical degradation
        if (
            new_level.value >= DegradationLevel.CRITICAL.value
            and new_level.value > old_level.value
        ):
            self._task_runner.run(
                self.handle_error,
                TaskOverloadError(
                    task_count=len(self._task_runner.tasks),
                    max_tasks=100,
                ),
            )

        # Log the change
        if hasattr(self, "_udp_logger"):
            try:
                from hyperscale.logging.hyperscale_logging_models import (
                    ServerInfo as ServerInfoLog,
                )

                self._udp_logger.log(
                    ServerInfoLog(
                        message=f"Degradation {direction}: {old_level.name} -> {new_level.name} ({policy.description})",
                        node_host=self._host,
                        node_port=self._port,
                        node_id=self._node_id.numeric_id
                        if hasattr(self, "_node_id")
                        else 0,
                    )
                )
            except Exception as e:
                # Don't let logging failure prevent degradation handling
                # But still track the unexpected error
                self._task_runner.run(
                    self.handle_error,
                    UnexpectedError(e, "degradation_logging"),
                )

        # Check if we need to step down from leadership
        if policy.should_step_down and self._leader_election.state.is_leader():
            # Log NotEligibleError - we're being forced to step down
            self._task_runner.run(
                self.handle_error,
                NotEligibleError(
                    reason="Stepping down due to degradation policy",
                    lhm_score=self._local_health.score,
                    max_lhm=self._leader_election.eligibility.max_leader_lhm,
                ),
            )
            self._task_runner.run(self._leader_election._step_down)

    def get_degradation_stats(self) -> dict:
        """Get graceful degradation statistics."""
        return self._degradation.get_stats()

    async def update_degradation(self) -> DegradationLevel:
        """Update and get current degradation level."""
        return await self._degradation.update()

    async def should_skip_probe(self) -> bool:
        """Check if probe should be skipped due to degradation."""
        await self._degradation.update()
        return self._degradation.should_skip_probe()

    async def should_skip_gossip(self) -> bool:
        """Check if gossip should be skipped due to degradation."""
        await self._degradation.update()
        return self._degradation.should_skip_gossip()

    def get_degraded_timeout_multiplier(self) -> float:
        """Get timeout multiplier based on degradation level."""
        return self._degradation.get_timeout_multiplier()

    # === Serf-Style Heartbeat Embedding ===
    # State embedding is handled via composition (StateEmbedder protocol).
    # Node types (Worker, Manager, Gate) inject their own embedder implementation.

    # Piggyback separators - all use consistent #|x pattern
    # This avoids conflicts since we search for the full 3-byte marker
    _STATE_SEPARATOR = b"#|s"  # State piggyback: #|sbase64...
    _MEMBERSHIP_SEPARATOR = b"#|m"  # Membership piggyback: #|mtype:inc:host:port...
    _HEALTH_SEPARATOR = b"#|h"  # Health piggyback: #|hentry1;entry2...

    def set_state_embedder(self, embedder: StateEmbedder) -> None:
        """
        Set the state embedder for this server.

        This allows node types to inject their own state embedding logic
        after construction (e.g., when the node has access to its own state).

        Args:
            embedder: The StateEmbedder implementation to use.
        """
        self._state_embedder = embedder

    def _get_embedded_state(self) -> bytes | None:
        """
        Get state to embed in SWIM probe responses.

        Delegates to the injected StateEmbedder to get serialized
        heartbeat data for Serf-style passive state discovery.

        Returns:
            Serialized state bytes, or None if no state to embed.
        """
        return self._state_embedder.get_state()

    def _process_embedded_state(
        self,
        state_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Process embedded state received from another node.

        Delegates to the injected StateEmbedder to handle heartbeat data
        from incoming SWIM messages.

        Args:
            state_data: Serialized state bytes from the remote node.
            source_addr: The (host, port) of the node that sent the state.
        """
        self._state_embedder.process_state(state_data, source_addr)

    async def _build_xprobe_response(
        self,
        source_addr: tuple[str, int] | bytes,
        probe_data: bytes,
    ) -> bytes | None:
        """
        Build a response to a cross-cluster health probe (xprobe).

        This is a hook for subclasses (e.g., ManagerServer) to provide
        aggregate datacenter health information to gates.

        By default, returns None (not a manager, can't respond).

        Args:
            source_addr: The source address of the probe (gate)
            probe_data: The probe message data

        Returns:
            Serialized CrossClusterAck bytes, or None if can't respond.
        """
        # Base implementation: not a manager, don't respond
        return None

    async def _handle_xack_response(
        self,
        source_addr: tuple[str, int] | bytes,
        ack_data: bytes,
    ) -> None:
        """
        Handle a cross-cluster health acknowledgment (xack).

        This is a hook for subclasses (e.g., GateServer) to process
        health data from datacenter leaders.

        By default, does nothing (not a gate, don't care about xack).

        Args:
            source_addr: The source address of the ack (DC leader)
            ack_data: The ack message data
        """
        # Base implementation: not a gate, ignore
        pass

    def _build_ack_with_state(self) -> bytes:
        """
        Build an ack response with embedded state (using self address).

        Format: ack>host:port#|sbase64_state (if state available)
                ack>host:port (if no state)

        Returns:
            Ack message bytes with optional embedded state.
        """
        return self._build_ack_with_state_for_addr(self._udp_addr_slug)

    def _build_ack_with_state_for_addr(self, addr_slug: bytes) -> bytes:
        """
        Build an ack response with embedded state for a specific address.

        Format: ack>host:port#|sbase64_state#|mtype:inc:host:port#|hentry1;entry2

        All piggyback uses consistent #|x pattern:
        1. Serf-style embedded state (heartbeat) after #|s
        2. Membership gossip piggyback after #|m
        3. Health gossip piggyback after #|h

        Args:
            addr_slug: The address slug to include in the ack (e.g., b'127.0.0.1:9000')

        Returns:
            Ack message bytes with embedded state and gossip piggyback.
        """
        base_ack = b"ack>" + addr_slug

        # Add Serf-style embedded state (heartbeat)
        state = self._get_embedded_state()
        if state is not None:
            encoded_state = b64encode(state)
            ack_with_state = base_ack + self._STATE_SEPARATOR + encoded_state
            # Check if state fits
            if len(ack_with_state) <= MAX_UDP_PAYLOAD:
                base_ack = ack_with_state

        # Add gossip piggyback (membership + health) - Phase 6.1 compliant
        return self._add_piggyback_safe(base_ack)

    def _extract_embedded_state(
        self,
        message: bytes,
        source_addr: tuple[str, int],
    ) -> bytes:
        """
        Extract and process embedded state from an incoming message.

        Separates the message content from any embedded state, processes
        the state if present, and returns the clean message.

        Wire format: msg_type>host:port#|sbase64_state#|mtype:inc:host:port#|hentry1;entry2#|v{json}

        All piggyback uses consistent #|x pattern - parsing is unambiguous:
        1. Strip Vivaldi coordinates (#|v...) - AD-35 Task 12.2.3, added last, strip first
        2. Strip health gossip (#|h...) - added second to last, strip second
        3. Strip membership piggyback (#|m...) - added third to last, strip third
        4. Extract state (#|s...) - part of base message

        Args:
            message: Raw message that may contain embedded state and piggyback.
            source_addr: The (host, port) of the sender.

        Returns:
            The message with embedded state and piggyback removed.
        """
        # Track boundaries to avoid repeated slicing until the end
        # msg_end marks where the core message ends (before any piggyback)
        msg_end = len(message)
        vivaldi_piggyback: bytes | None = None
        health_piggyback: bytes | None = None
        membership_piggyback: bytes | None = None

        # Step 1: Find Vivaldi coordinate piggyback (#|v...) - AD-35 Task 12.2.3
        # Vivaldi is always appended last, so strip first
        vivaldi_idx = message.find(b"#|v")
        if vivaldi_idx > 0:
            vivaldi_piggyback = message[vivaldi_idx + 3:]  # Skip '#|v' separator
            msg_end = vivaldi_idx

        # Step 2: Find health gossip piggyback (#|h...)
        # Health is added second to last, strip second
        health_idx = message.find(self._HEALTH_SEPARATOR, 0, msg_end)
        if health_idx > 0:
            health_piggyback = message[health_idx:]
            msg_end = health_idx

        # Step 3: Find membership piggyback (#|m...) in the remaining portion
        membership_idx = message.find(self._MEMBERSHIP_SEPARATOR, 0, msg_end)
        if membership_idx > 0:
            membership_piggyback = message[membership_idx:msg_end]
            msg_end = membership_idx

        # Step 4: Find message structure in core message only
        # Format: msg_type>host:port#|sbase64_state
        addr_sep_idx = message.find(b">", 0, msg_end)
        if addr_sep_idx < 0:
            # No address separator - process piggyback and return
            if vivaldi_piggyback:
                self._process_vivaldi_piggyback(vivaldi_piggyback, source_addr)
            if health_piggyback:
                self._health_gossip_buffer.decode_and_process_piggyback(
                    health_piggyback
                )
            if membership_piggyback:
                self._task_runner.run(self.process_piggyback_data, membership_piggyback)
            return message[:msg_end] if msg_end < len(message) else message

        # Find state separator after '>' but before piggyback
        state_sep_idx = message.find(self._STATE_SEPARATOR, addr_sep_idx, msg_end)

        # Process piggyback data (can happen in parallel with state processing)
        if vivaldi_piggyback:
            self._process_vivaldi_piggyback(vivaldi_piggyback, source_addr)
        if health_piggyback:
            self._health_gossip_buffer.decode_and_process_piggyback(health_piggyback)
        if membership_piggyback:
            self._task_runner.run(self.process_piggyback_data, membership_piggyback)

        # No state separator - return clean message
        if state_sep_idx < 0:
            return message[:msg_end] if msg_end < len(message) else message

        # Extract and decode state
        # Slice once: encoded_state is between state_sep and msg_end
        # Skip 3 bytes for '#|s' separator
        encoded_state = message[state_sep_idx + 3 : msg_end]

        try:
            state_data = b64decode(encoded_state)
            self._process_embedded_state(state_data, source_addr)
        except Exception:
            # Invalid base64 or processing error - ignore silently
            pass

        # Return message up to state separator (excludes state and all piggyback)
        return message[:state_sep_idx]

    def _process_vivaldi_piggyback(
        self,
        vivaldi_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Process Vivaldi coordinate piggyback from peer (AD-35 Task 12.2.4).

        Extracts peer's Vivaldi coordinate, calculates RTT if this is an ACK
        response to our probe, and updates the CoordinateTracker.

        Args:
            vivaldi_data: JSON-encoded coordinate dictionary
            source_addr: Sender's address tuple
        """
        try:
            import json
            from hyperscale.distributed_rewrite.models.coordinates import NetworkCoordinate

            coord_dict = json.loads(vivaldi_data)
            peer_coord = NetworkCoordinate.from_dict(coord_dict)

            # Check if this is a response to our probe (we have start time)
            probe_start = self._pending_probe_start.get(source_addr)
            if probe_start is not None:
                # Calculate RTT in milliseconds
                rtt_seconds = time.monotonic() - probe_start
                rtt_ms = rtt_seconds * 1000.0

                # Update coordinate tracker with RTT measurement (AD-35 Task 12.2.6)
                peer_id = f"{source_addr[0]}:{source_addr[1]}"
                self._coordinate_tracker.update_peer_coordinate(
                    peer_id=peer_id,
                    peer_coordinate=peer_coord,
                    rtt_ms=rtt_ms,
                )
            else:
                # No RTT measurement available - just store coordinate
                peer_id = f"{source_addr[0]}:{source_addr[1]}"
                # Store coordinate without updating (no RTT measurement)
                self._coordinate_tracker._peers[peer_id] = peer_coord
                self._coordinate_tracker._peer_last_seen[peer_id] = time.monotonic()

        except Exception:
            # Invalid JSON or coordinate data - ignore silently
            # Don't let coordinate processing errors break message handling
            pass

    # === Message Size Helpers ===

    def _add_piggyback_safe(self, base_message: bytes) -> bytes:
        """
        Add piggybacked gossip updates to a message, respecting MTU limits.

        This adds membership gossip, health gossip (Phase 6.1), and Vivaldi
        coordinates (AD-35 Task 12.2.5) to outgoing messages for O(log n)
        dissemination of both membership, health state, and network coordinates.

        Args:
            base_message: The core message to send.

        Returns:
            Message with piggybacked updates that fits within UDP MTU.
        """
        if len(base_message) >= MAX_UDP_PAYLOAD:
            # Base message already at limit, can't add piggyback
            return base_message

        # Add membership gossip (format: #|mtype:incarnation:host:port...)
        membership_piggyback = self._gossip_buffer.encode_piggyback_with_base(
            base_message
        )
        message_with_membership = base_message + membership_piggyback

        # Calculate remaining space for health gossip
        remaining = MAX_UDP_PAYLOAD - len(message_with_membership)
        if remaining < 50:
            # Not enough room for health piggyback
            return message_with_membership

        # Update local health state in the buffer before encoding
        health_piggyback = self._state_embedder.get_health_piggyback()
        if health_piggyback:
            self._health_gossip_buffer.update_local_health(health_piggyback)

        # Add health gossip (format: #|hentry1;entry2;...)
        health_gossip = self._health_gossip_buffer.encode_piggyback(
            max_count=5,
            max_size=remaining,
        )

        message_with_health = message_with_membership + health_gossip

        # AD-35 Task 12.2.5: Add Vivaldi coordinates (format: #|v{json})
        # Only add if there's room - coordinates are ~80-150 bytes
        remaining_after_health = MAX_UDP_PAYLOAD - len(message_with_health)
        if remaining_after_health >= 150:
            import json
            coord = self._coordinate_tracker.get_coordinate()
            coord_dict = coord.to_dict()
            coord_json = json.dumps(coord_dict, separators=(',', ':')).encode()
            vivaldi_piggyback = b"#|v" + coord_json

            if len(message_with_health) + len(vivaldi_piggyback) <= MAX_UDP_PAYLOAD:
                return message_with_health + vivaldi_piggyback

        return message_with_health

    def _check_message_size(self, message: bytes) -> bool:
        """
        Check if a message is safe to send via UDP.

        Returns:
            True if message is within safe limits, False otherwise.
        """
        return len(message) <= MAX_UDP_PAYLOAD

    async def start_cleanup(self) -> None:
        """Start the periodic cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.ensure_future(self._run_cleanup_loop())

    async def stop_cleanup(self) -> None:
        """Stop the periodic cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    async def _run_cleanup_loop(self) -> None:
        """Run periodic cleanup of all SWIM state."""
        while self._running:
            try:
                await asyncio.sleep(self._cleanup_interval)
                await self._run_cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "cleanup_loop")

    async def _run_cleanup(self) -> None:
        """Run one cleanup cycle for all SWIM components using ErrorContext."""
        stats = {}

        # Cleanup incarnation tracker (dead node GC)
        async with ErrorContext(self._error_handler, "incarnation_cleanup"):
            stats["incarnation"] = await self._incarnation_tracker.cleanup()

        # Cleanup hierarchical detector (reconciliation)
        async with ErrorContext(self._error_handler, "suspicion_cleanup"):
            stats["suspicion"] = self._hierarchical_detector.get_stats()

        # Cleanup indirect probe manager
        async with ErrorContext(self._error_handler, "indirect_probe_cleanup"):
            stats["indirect_probe"] = self._indirect_probe_manager.cleanup()

        # Cleanup gossip buffer
        async with ErrorContext(self._error_handler, "gossip_cleanup"):
            stats["gossip"] = self._gossip_buffer.cleanup()

        # Cleanup old messages from dedup cache
        async with ErrorContext(self._error_handler, "dedup_cleanup"):
            self._seen_messages.cleanup_older_than(self._dedup_window * 2)

        # Cleanup old rate limit entries
        async with ErrorContext(self._error_handler, "rate_limit_cleanup"):
            self._rate_limits.cleanup_older_than(60.0)  # 1 minute

        # AD-29: Check for stale unconfirmed peers and log warnings
        async with ErrorContext(self._error_handler, "stale_unconfirmed_cleanup"):
            await self._check_stale_unconfirmed_peers()

        # AD-35 Task 12.5.6: Run RoleAwareConfirmationManager cleanup
        async with ErrorContext(self._error_handler, "confirmation_manager_cleanup"):
            confirmation_results = (
                await self._confirmation_manager.check_and_cleanup_unconfirmed_peers()
            )
            stats["confirmation_manager"] = {
                "total": len(confirmation_results),
                "confirmed": sum(1 for r in confirmation_results if r.confirmed),
                "removed": sum(1 for r in confirmation_results if r.removed),
            }

        # Check for counter overflow and reset if needed
        # (Python handles big ints, but we reset periodically for monitoring clarity)
        self._check_and_reset_stats()

    def get_cleanup_stats(self) -> dict:
        """Get cleanup statistics from all components."""
        return {
            "incarnation": self._incarnation_tracker.get_stats(),
            "suspicion": self._hierarchical_detector.get_stats_sync(),
            "indirect_probe": self._indirect_probe_manager.get_stats(),
            "gossip": self._gossip_buffer.get_stats(),
        }

    def _check_and_reset_stats(self) -> None:
        """
        Check for counter overflow and reset stats if they're too large.

        While Python handles arbitrary precision integers, we reset
        periodically to keep monitoring data meaningful and prevent
        very large numbers that might cause issues in serialization
        or logging.
        """
        MAX_COUNTER = 10_000_000_000  # 10 billion - reset threshold

        # Reset dedup stats if too large
        if (
            self._dedup_stats["duplicates"] > MAX_COUNTER
            or self._dedup_stats["unique"] > MAX_COUNTER
        ):
            self._dedup_stats = {"duplicates": 0, "unique": 0}

        # Reset rate limit stats if too large
        if (
            self._rate_limit_stats["accepted"] > MAX_COUNTER
            or self._rate_limit_stats["rejected"] > MAX_COUNTER
        ):
            self._rate_limit_stats = {"accepted": 0, "rejected": 0}

    async def _check_stale_unconfirmed_peers(self) -> None:
        """
        Check for unconfirmed peers that have exceeded the stale threshold (AD-29).

        Unconfirmed peers are peers we've been told about but haven't successfully
        communicated with via SWIM. If they remain unconfirmed for too long, this
        may indicate network issues or misconfiguration.

        Logs a warning for each stale peer to aid debugging cluster formation issues.
        """
        # Threshold: peers unconfirmed for more than 60 seconds are considered stale
        STALE_UNCONFIRMED_THRESHOLD = 60.0

        stale_count = 0
        now = time.monotonic()

        for peer, added_at in list(self._unconfirmed_peer_added_at.items()):
            age = now - added_at
            if age > STALE_UNCONFIRMED_THRESHOLD:
                stale_count += 1
                await self._udp_logger.log(
                    ServerWarning(
                        message=f"Unconfirmed peer {peer[0]}:{peer[1]} stale for {age:.1f}s (AD-29)",
                        node_host=self._host,
                        node_port=self._tcp_port,
                        node_id=self._node_id.short if hasattr(self, '_node_id') else "unknown",
                    )
                )

        # Update metrics for stale unconfirmed peers
        if stale_count > 0:
            self._metrics.record_counter("stale_unconfirmed_peers", stale_count)

    def _setup_leader_election(self) -> None:
        """Initialize leader election callbacks after server is started."""
        self._leader_election.set_callbacks(
            broadcast_message=self._broadcast_leadership_message,
            get_member_count=self._get_member_count,
            get_lhm_score=lambda: self._local_health.score,
            self_addr=self._get_self_udp_addr(),
            on_error=self._handle_election_error,
            should_refuse_leadership=lambda: self._degradation.should_refuse_leadership(),
            task_runner=self._task_runner,
            on_election_started=self._on_election_started,
            on_heartbeat_sent=self._on_heartbeat_sent,
        )

        # Set up leadership event callbacks
        self._leader_election.state.set_callbacks(
            on_become_leader=self._on_become_leader,
            on_lose_leadership=self._on_lose_leadership,
            on_leader_change=self._on_leader_change,
        )

    async def _handle_election_error(self, error) -> None:
        """Handle election errors through the error handler."""
        await self.handle_error(error)

    def _broadcast_leadership_message(self, message: bytes) -> None:
        """
        Broadcast a leadership message to all known nodes.

        Leadership messages are critical - schedule them via task runner
        with error tracking.
        """
        nodes: Nodes = self._context.read("nodes")
        self_addr = self._get_self_udp_addr()
        base_timeout = self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        # Snapshot nodes to avoid dict mutation during iteration
        for node in list(nodes.keys()):
            if node != self_addr:
                # Use task runner but schedule error-aware send
                self._task_runner.run(
                    self._send_leadership_message,
                    node,
                    message,
                    timeout,
                )

    async def _send_leadership_message(
        self,
        node: tuple[str, int],
        message: bytes,
        timeout: float,
    ) -> bool:
        """
        Send a leadership message with retry.

        Leadership messages are critical for cluster coordination,
        so we use retry_with_backoff with ELECTION_RETRY_POLICY.
        """
        result = await retry_with_result(
            lambda: self._send_once(node, message, timeout),
            policy=ELECTION_RETRY_POLICY,
            on_retry=self._on_leadership_retry,
        )

        if result.success:
            self.record_network_success()
            return True
        else:
            if result.last_error:
                await self.handle_error(
                    NetworkError(
                        f"Leadership message to {node[0]}:{node[1]} failed after retries: {result.last_error}",
                        severity=ErrorSeverity.DEGRADED,
                        target=node,
                        attempts=result.attempts,
                    )
                )
            return False

    async def _on_leadership_retry(
        self,
        attempt: int,
        error: Exception,
        delay: float,
    ) -> None:
        """Callback for leadership retry attempts."""
        await self.increase_failure_detector("leadership_retry")

    def _on_election_started(self) -> None:
        """Called when this node starts an election."""
        self._metrics.increment("elections_started")
        self._audit_log.record(
            AuditEventType.ELECTION_STARTED,
            node=self._get_self_udp_addr(),
            term=self._leader_election.state.current_term,
        )

    def _on_heartbeat_sent(self) -> None:
        """Called when this node sends a heartbeat as leader."""
        self._metrics.increment("heartbeats_sent")

    def _on_become_leader(self) -> None:
        """Called when this node becomes the leader."""
        self._metrics.increment("elections_won")
        self._metrics.increment("leadership_changes")
        self_addr = self._get_self_udp_addr()
        self._audit_log.record(
            AuditEventType.ELECTION_WON,
            node=self_addr,
            term=self._leader_election.state.current_term,
        )
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"[{self._udp_addr_slug.decode()}] Became LEADER (term {self._leader_election.state.current_term})",
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            ),
        )

        # Invoke registered callbacks (composition pattern)
        for callback in self._on_become_leader_callbacks:
            try:
                callback()
            except Exception as e:
                # Log but don't let one callback failure break others
                self._task_runner.run(
                    self.handle_exception, e, "on_become_leader_callback"
                )

    def _on_lose_leadership(self) -> None:
        """Called when this node loses leadership."""
        self._metrics.increment("elections_lost")
        self._metrics.increment("leadership_changes")
        self_addr = self._get_self_udp_addr()
        self._audit_log.record(
            AuditEventType.ELECTION_LOST,
            node=self_addr,
            term=self._leader_election.state.current_term,
        )
        self._task_runner.run(
            self._udp_logger.log,
            ServerInfo(
                message=f"[{self._node_id.short}] Lost leadership",
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            ),
        )

        # Invoke registered callbacks (composition pattern)
        for callback in self._on_lose_leadership_callbacks:
            try:
                callback()
            except Exception as e:
                self._task_runner.run(
                    self.handle_exception, e, "on_lose_leadership_callback"
                )

    def _on_leader_change(self, new_leader: tuple[str, int] | None) -> None:
        """Called when the known leader changes."""
        self._audit_log.record(
            AuditEventType.LEADER_CHANGED,
            node=new_leader,
            term=self._leader_election.state.current_term,
        )
        if new_leader:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"[{self._node_id.short}] New leader: {new_leader[0]}:{new_leader[1]}",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                ),
            )
        else:
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"[{self._node_id.short}] No leader currently",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                ),
            )

        # Invoke registered callbacks (composition pattern)
        for callback in self._on_leader_change_callbacks:
            try:
                callback(new_leader)
            except Exception as e:
                self._task_runner.run(
                    self.handle_exception, e, "on_leader_change_callback"
                )

    def _get_member_count(self) -> int:
        """Get the current number of known members."""
        nodes = self._context.read("nodes")
        return len(nodes) if nodes else 1

    def _on_suspicion_expired(self, node: tuple[str, int], incarnation: int) -> None:
        """Callback when a suspicion expires - mark node as DEAD."""
        # DEBUG: Track when nodes are marked DEAD

        self._metrics.increment("suspicions_expired")
        self._audit_log.record(
            AuditEventType.NODE_CONFIRMED_DEAD,
            node=node,
            incarnation=incarnation,
        )
        self._incarnation_tracker.update_node(
            node,
            b"DEAD",
            incarnation,
            time.monotonic(),
        )
        # Queue the death notification for gossip
        self.queue_gossip_update("dead", node, incarnation)
        nodes: Nodes = self._context.read("nodes")
        if node in nodes:
            self._safe_queue_put_sync(
                nodes[node], (int(time.monotonic()), b"DEAD"), node
            )

        # Update probe scheduler to stop probing this dead node
        self.update_probe_scheduler_membership()

        # Invoke registered callbacks (composition pattern)
        for callback in self._on_node_dead_callbacks:
            try:
                callback(node)
            except Exception as e:
                self._task_runner.run(self.handle_exception, e, "on_node_dead_callback")

    def _safe_queue_put_sync(
        self,
        queue: asyncio.Queue,
        item: tuple,
        node: tuple[str, int],
    ) -> bool:
        """
        Synchronous version of _safe_queue_put for use in sync callbacks.

        If queue is full, schedules error logging as a task and drops the update.
        """
        try:
            queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            # Schedule error logging via task runner since we can't await in sync context
            self._task_runner.run(
                self.handle_error,
                QueueFullError(
                    f"Node queue full for {node[0]}:{node[1]}, dropping update",
                    node=node,
                    queue_size=queue.qsize(),
                ),
            )
            return False

    async def _safe_queue_put(
        self,
        queue: asyncio.Queue,
        item: tuple,
        node: tuple[str, int],
    ) -> bool:
        """
        Safely put an item into a node's queue with overflow handling.

        If queue is full, logs QueueFullError and drops the update.
        This prevents blocking on slow consumers.

        Returns True if successful, False if queue was full.
        """
        try:
            queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            await self.handle_error(
                QueueFullError(
                    f"Node queue full for {node[0]}:{node[1]}, dropping update",
                    node=node,
                    queue_size=queue.qsize(),
                )
            )
            return False

    def queue_gossip_update(
        self,
        update_type: UpdateType,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Queue a membership update for piggybacking on future messages."""
        self._metrics.increment("gossip_updates_sent")

        # Track specific propagation metrics
        if update_type == "join":
            self._metrics.increment("joins_propagated")
        elif update_type == "leave":
            self._metrics.increment("leaves_propagated")

        n_members = self._get_member_count()
        # AD-35 Task 12.4.3: Include role in gossip updates
        role = self._peer_roles.get(node, None) if hasattr(self, "_peer_roles") else None
        # If this is our own node, use our role
        if node == self._get_self_udp_addr():
            role = self._node_role
        self._gossip_buffer.add_update(update_type, node, incarnation, n_members, role)

    def get_piggyback_data(self, max_updates: int = 5) -> bytes:
        """Get piggybacked membership updates to append to a message."""
        return self._gossip_buffer.encode_piggyback(max_updates)

    async def process_piggyback_data(self, data: bytes) -> None:
        """Process piggybacked membership updates received in a message."""
        updates = GossipBuffer.decode_piggyback(data)
        self._metrics.increment("gossip_updates_received", len(updates))
        for update in updates:
            # AD-35 Task 12.4.3: Extract and store peer role from gossip
            if update.role and hasattr(self, "_peer_roles"):
                from hyperscale.distributed_rewrite.models.distributed import NodeRole

                try:
                    self._peer_roles[update.node] = NodeRole(update.role.lower())
                except ValueError:
                    # Invalid role, ignore
                    pass

            status_map = {
                "alive": b"OK",
                "join": b"OK",
                "suspect": b"SUSPECT",
                "dead": b"DEAD",
                "leave": b"DEAD",
            }
            status = status_map.get(update.update_type, b"OK")

            if self.is_message_fresh(update.node, update.incarnation, status):
                # Check previous state BEFORE updating (for callback invocation)
                previous_state = self._incarnation_tracker.get_node_state(update.node)
                was_dead = previous_state and previous_state.status == b"DEAD"

                updated = self.update_node_state(
                    update.node,
                    status,
                    update.incarnation,
                    update.timestamp,
                )

                if update.update_type == "suspect":
                    self_addr = self._get_self_udp_addr()
                    if update.node != self_addr:
                        await self.start_suspicion(
                            update.node,
                            update.incarnation,
                            self_addr,
                        )
                elif update.update_type == "alive":
                    await self.refute_suspicion(update.node, update.incarnation)

                # Gossip-informed dead callback: if gossip tells us a node is dead
                # and we didn't already know, invoke the callbacks so application
                # layer can respond (e.g., update _active_gate_peers, trigger job
                # leadership election). This is symmetric with recovery detection
                # that's already in update_node_state for DEAD->OK transitions.
                if updated and update.update_type in ("dead", "leave") and not was_dead:
                    self._metrics.increment("gossip_informed_deaths")
                    self._audit_log.record(
                        AuditEventType.NODE_CONFIRMED_DEAD,
                        node=update.node,
                        incarnation=update.incarnation,
                        source="gossip",
                    )

                    # Update probe scheduler to stop probing this dead node
                    self._probe_scheduler.remove_member(update.node)

                    # Invoke registered callbacks (same pattern as _on_suspicion_expired)
                    for callback in self._on_node_dead_callbacks:
                        try:
                            callback(update.node)
                        except Exception as callback_error:
                            self._task_runner.run(
                                self.handle_exception,
                                callback_error,
                                "on_node_dead_callback (gossip)",
                            )

                self.queue_gossip_update(
                    update.update_type,
                    update.node,
                    update.incarnation,
                )

    def get_other_nodes(self, node: tuple[str, int]):
        target_host, target_port = node
        nodes: Nodes = self._context.read("nodes")
        # Use list() to snapshot keys before iteration to prevent
        # "dictionary changed size during iteration" errors
        return [
            (host, port)
            for host, port in list(nodes.keys())
            if not (host == target_host and port == target_port)
        ]

    async def _gather_with_errors(
        self,
        coros: list,
        operation: str,
        timeout: float | None = None,
    ) -> tuple[list, list[Exception]]:
        """
        Run coroutines concurrently with proper error handling.

        Unlike asyncio.gather, this:
        - Returns (results, errors) tuple instead of raising
        - Applies optional timeout to prevent hanging
        - Logs failures via error handler

        Args:
            coros: List of coroutines to run
            operation: Name for error context
            timeout: Optional timeout for the entire gather

        Returns:
            (successful_results, exceptions)
        """
        if not coros:
            return [], []

        try:
            if timeout:
                results = await asyncio.wait_for(
                    asyncio.gather(*coros, return_exceptions=True),
                    timeout=timeout,
                )
            else:
                results = await asyncio.gather(*coros, return_exceptions=True)
        except asyncio.TimeoutError:
            await self.handle_error(
                NetworkError(
                    f"Gather timeout in {operation}",
                    severity=ErrorSeverity.DEGRADED,
                    operation=operation,
                )
            )
            return [], [asyncio.TimeoutError(f"Gather timeout in {operation}")]

        successes = []
        errors = []

        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                successes.append(result)

        # Log aggregate errors if any
        if errors:
            await self.handle_error(
                NetworkError(
                    f"{operation}: {len(errors)}/{len(results)} operations failed",
                    severity=ErrorSeverity.TRANSIENT,
                    operation=operation,
                    error_count=len(errors),
                    success_count=len(successes),
                )
            )

        return successes, errors

    async def send_if_ok(
        self,
        node: tuple[str, int],
        message: bytes,
        include_piggyback: bool = True,
    ) -> bool:
        """
        Send a message to a node if its status is OK.

        Returns True if send was queued, False if skipped (node not OK).
        Failures are logged via error handler.
        """
        base_timeout = self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        # Check node status
        nodes: Nodes = self._context.read("nodes")
        node_entry = nodes.get(node)
        if not node_entry:
            return False

        try:
            _, status = node_entry.get_nowait()
            if status != b"OK":
                return False
        except asyncio.QueueEmpty:
            return False

        # Note: Piggyback is added centrally in send() hook via _add_piggyback_safe()
        # The include_piggyback parameter is kept for backwards compatibility but ignored

        # Track the send and log failures
        try:
            await self._send_with_retry(node, message, timeout)
            return True
        except Exception as e:
            # Log the failure but don't re-raise
            await self.handle_error(
                NetworkError(
                    f"send_if_ok to {node[0]}:{node[1]} failed: {e}",
                    target=node,
                    severity=ErrorSeverity.TRANSIENT,
                )
            )
            return False

    # poll_node method removed - was deprecated, use start_probe_cycle instead

    async def join_cluster(
        self,
        seed_node: tuple[str, int],
        timeout: float = 5.0,
    ) -> bool:
        """
        Join a cluster via a seed node with retry support.

        Uses retry_with_backoff to handle transient failures when
        the seed node might not be ready yet.

        Args:
            seed_node: (host, port) of a node already in the cluster
            timeout: Timeout per attempt

        Returns:
            True if join succeeded, False if all retries exhausted
        """
        self_addr = self._get_self_udp_addr()
        # Format: join>v{major}.{minor}|{host}:{port}
        # Version prefix enables detecting incompatible nodes during join (AD-25)
        join_msg = (
            b"join>"
            + SWIM_VERSION_PREFIX
            + b"|"
            + f"{self_addr[0]}:{self_addr[1]}".encode()
        )

        async def attempt_join() -> bool:
            await self.send(seed_node, join_msg, timeout=timeout)
            # Add seed to our known nodes dict (defaultdict auto-creates Queue)
            nodes: Nodes = self._context.read("nodes")
            _ = nodes[seed_node]  # Access to create entry via defaultdict
            self._probe_scheduler.add_member(seed_node)
            return True

        result = await retry_with_result(
            attempt_join,
            policy=ELECTION_RETRY_POLICY,  # Use election policy for joining
            on_retry=lambda a, e, d: self.increase_failure_detector("join_retry"),
        )

        if result.success:
            self.record_network_success()
            return True
        else:
            if result.last_error:
                await self.handle_error(
                    NetworkError(
                        f"Failed to join cluster via {seed_node[0]}:{seed_node[1]} after {result.attempts} attempts",
                        severity=ErrorSeverity.DEGRADED,
                        target=seed_node,
                        attempts=result.attempts,
                    )
                )
            return False

    async def start_probe_cycle(self) -> None:
        """Start the SWIM randomized round-robin probe cycle."""
        # Ensure error handler is set up first
        if self._error_handler is None:
            self._setup_error_handler()

        # Integrate task runner with SWIM components
        self._setup_task_runner_integration()

        # Start hierarchical failure detector (AD-30)
        await self._hierarchical_detector.start()

        # Start health monitor for proactive CPU detection
        await self.start_health_monitor()

        # Start cleanup task
        await self.start_cleanup()

        self._probe_scheduler._running = True
        nodes: Nodes = self._context.read("nodes")
        self_addr = self._get_self_udp_addr()
        members = [node for node in list(nodes.keys()) if node != self_addr]
        self._probe_scheduler.update_members(members)

        protocol_period = self._context.read("udp_poll_interval", 1.0)
        self._probe_scheduler.protocol_period = protocol_period

        while self._running and self._probe_scheduler._running:
            try:
                await self._run_probe_round()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_exception(e, "probe_cycle")
            await asyncio.sleep(protocol_period)

    async def _run_probe_round(self) -> None:
        """Execute a single probe round in the SWIM protocol."""
        # Exit early if we're shutting down - don't attempt probes during shutdown
        if not self._running or not self._probe_scheduler._running:
            return

        # Check circuit breaker - if too many network errors, back off
        if self._error_handler and self._error_handler.is_circuit_open(
            ErrorCategory.NETWORK
        ):
            # Network circuit is open - skip this round to let things recover
            await asyncio.sleep(1.0)  # Brief pause before next attempt
            return

        target = self._probe_scheduler.get_next_target()
        if target is None:
            return

        if self.udp_target_is_self(target):
            return

        # Use ErrorContext for consistent error handling throughout the probe
        async with ErrorContext(
            self._error_handler, f"probe_round_{target[0]}_{target[1]}"
        ) as ctx:
            node_state = self._incarnation_tracker.get_node_state(target)
            incarnation = node_state.incarnation if node_state else 0

            base_timeout = self._context.read("current_timeout")
            timeout = self.get_lhm_adjusted_timeout(base_timeout)

            target_addr = f"{target[0]}:{target[1]}".encode()
            # Note: Piggyback is added centrally in send() hook via _add_piggyback_safe()
            probe_msg = b"probe>" + target_addr

            response_received = await self._probe_with_timeout(
                target, probe_msg, timeout
            )

            # Exit early if shutting down
            if not self._running:
                return

            if response_received:
                await self.decrease_failure_detector("successful_probe")
                ctx.record_success(
                    ErrorCategory.NETWORK
                )  # Help circuit breaker recover
                return

            await self.increase_failure_detector("probe_timeout")
            indirect_sent = await self.initiate_indirect_probe(target, incarnation)

            # Exit early if shutting down
            if not self._running:
                return

            if indirect_sent:
                await asyncio.sleep(timeout)

                # Exit early if shutting down
                if not self._running:
                    return

                probe = self._indirect_probe_manager.get_pending_probe(target)
                if probe and probe.is_completed():
                    await self.decrease_failure_detector("successful_probe")
                    ctx.record_success(ErrorCategory.NETWORK)
                    return

            # Don't start suspicions during shutdown
            if not self._running:
                return

            self_addr = self._get_self_udp_addr()
            await self.start_suspicion(target, incarnation, self_addr)
            await self.broadcast_suspicion(target, incarnation)

    async def _probe_with_timeout(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float,
    ) -> bool:
        """
        Send a probe message with retries before falling back to indirect.

        Uses PROBE_RETRY_POLICY for retry logic with exponential backoff.
        Returns True if probe succeeded (ACK received), False if all retries exhausted.

        Uses Future-based ACK tracking: we wait for the actual ACK message to arrive,
        not just checking cached node state which could be stale.
        """
        self._metrics.increment("probes_sent")
        attempt = 0
        max_attempts = PROBE_RETRY_POLICY.max_attempts + 1

        while attempt < max_attempts:
            # Exit early if shutting down
            if not self._running:
                return False

            try:
                # Create a Future to wait for ACK from this specific probe
                # Cancel any existing pending probe to the same target (stale)
                existing_future = self._pending_probe_acks.pop(target, None)
                if existing_future and not existing_future.done():
                    existing_future.cancel()

                ack_future: asyncio.Future[bool] = (
                    asyncio.get_event_loop().create_future()
                )
                self._pending_probe_acks[target] = ack_future

                self._pending_probe_start[target] = time.monotonic()
                await self.send(target, message, timeout=timeout)

                # Wait for ACK with timeout (reduced time for retries)
                wait_time = (
                    timeout * 0.5 if attempt < max_attempts - 1 else timeout * 0.8
                )

                try:
                    await asyncio.wait_for(ack_future, timeout=wait_time)
                    # Future completed means ACK was received
                    self._metrics.increment("probes_received")
                    return True
                except asyncio.TimeoutError:
                    # No ACK received within timeout, try again
                    pass
                finally:
                    self._pending_probe_acks.pop(target, None)
                    self._pending_probe_start.pop(target, None)

                attempt += 1
                if attempt < max_attempts:
                    # Exponential backoff with jitter before retry
                    backoff = PROBE_RETRY_POLICY.base_delay * (
                        PROBE_RETRY_POLICY.exponential_base ** (attempt - 1)
                    )
                    jitter = random.uniform(0, PROBE_RETRY_POLICY.jitter * backoff)
                    await asyncio.sleep(backoff + jitter)

            except asyncio.CancelledError:
                # Clean up on cancellation
                self._pending_probe_acks.pop(target, None)
                self._pending_probe_start.pop(target, None)
                raise
            except OSError as e:
                # Network error - wrap with appropriate error type
                self._pending_probe_acks.pop(target, None)
                self._pending_probe_start.pop(target, None)
                self._metrics.increment("probes_failed")
                await self.handle_error(self._make_network_error(e, target, "Probe"))
                return False
            except Exception as e:
                self._pending_probe_acks.pop(target, None)
                self._pending_probe_start.pop(target, None)
                self._metrics.increment("probes_failed")
                await self.handle_exception(e, f"probe_{target[0]}_{target[1]}")
                return False

        self._metrics.increment("probes_timeout")
        await self.handle_error(ProbeTimeoutError(target, timeout))
        return False

    def stop_probe_cycle(self) -> None:
        """Stop the probe cycle."""
        self._probe_scheduler.stop()

    def update_probe_scheduler_membership(self) -> None:
        """Update the probe scheduler with current membership, excluding DEAD nodes."""
        nodes: Nodes = self._context.read("nodes")
        self_addr = self._get_self_udp_addr()
        members = []
        for node in list(nodes.keys()):
            if node == self_addr:
                continue
            # Check if node is DEAD via incarnation tracker
            node_state = self._incarnation_tracker.get_node_state(node)
            if node_state and node_state.status == b"DEAD":
                continue
            members.append(node)
        self._probe_scheduler.update_members(members)

    async def start_leader_election(self) -> None:
        """Start the leader election process."""
        # Ensure error handler is set up first
        if self._error_handler is None:
            self._setup_error_handler()
        self._setup_leader_election()
        await self._leader_election.start()

    async def stop_leader_election(self) -> None:
        """Stop the leader election process."""
        await self._leader_election.stop()

    async def _graceful_shutdown(
        self,
        drain_timeout: float = 5.0,
        broadcast_leave: bool = True,
    ) -> None:
        """
        Perform graceful shutdown of the SWIM protocol node.

        This method coordinates the shutdown of all components in the proper order:
        1. Step down from leadership (if leader)
        2. Broadcast leave message to cluster
        3. Wait for drain period (allow in-flight messages to complete)
        4. Stop all background tasks
        5. Clean up resources

        Args:
            drain_timeout: Seconds to wait for in-flight messages to complete.
            broadcast_leave: Whether to broadcast a leave message.
        """
        self._running = False
        self_addr = self._get_self_udp_addr()

        # Signal to error handler that we're shutting down - suppress non-fatal errors
        if self._error_handler:
            self._error_handler.start_shutdown()

        # 1. Step down from leadership if we're the leader
        if self._leader_election.state.is_leader():
            try:
                await self._leader_election._step_down()
            except Exception as e:
                if self._error_handler:
                    await self.handle_exception(e, "shutdown_step_down")

        # 2. Broadcast leave message to cluster
        if broadcast_leave:
            try:
                leave_msg = b"leave>" + f"{self_addr[0]}:{self_addr[1]}".encode()
                nodes: Nodes = self._context.read("nodes")
                timeout = self.get_lhm_adjusted_timeout(1.0)

                send_failures = 0
                for node in list(nodes.keys()):
                    if node != self_addr:
                        try:
                            await self.send(node, leave_msg, timeout=timeout)
                        except Exception as e:
                            # Best effort - log but don't fail shutdown for send errors
                            send_failures += 1
                            await self._udp_logger.log(
                                ServerDebug(
                                    message=f"Leave broadcast to {node[0]}:{node[1]} failed: {type(e).__name__}",
                                    node_host=self._host,
                                    node_port=self._port,
                                    node_id=self._node_id.numeric_id,
                                )
                            )

                if send_failures > 0:
                    await self._udp_logger.log(
                        ServerDebug(
                            message=f"Leave broadcast: {send_failures}/{len(nodes) - 1} sends failed",
                            node_host=self._host,
                            node_port=self._port,
                            node_id=self._node_id.numeric_id,
                        )
                    )
            except Exception as e:
                if self._error_handler:
                    await self.handle_exception(e, "shutdown_broadcast_leave")

        # 3. Wait for drain period
        if drain_timeout > 0:
            await asyncio.sleep(drain_timeout)

        # 4. Stop all background tasks in proper order
        # Stop probe cycle first (stops probing other nodes)
        try:
            self.stop_probe_cycle()
        except Exception as e:
            if self._error_handler:
                await self.handle_exception(e, "shutdown_stop_probe_cycle")

        # Cancel all pending probe ACK futures
        for future in self._pending_probe_acks.values():
            if not future.done():
                future.cancel()
        self._pending_probe_acks.clear()

        # Stop leader election (stops sending heartbeats)
        try:
            await self.stop_leader_election()
        except Exception as e:
            if self._error_handler:
                await self.handle_exception(e, "shutdown_stop_election")

        # Stop health monitor
        try:
            await self.stop_health_monitor()
        except Exception as e:
            if self._error_handler:
                await self.handle_exception(e, "shutdown_stop_health_monitor")

        # Stop cleanup task
        try:
            await self.stop_cleanup()
        except Exception as e:
            if self._error_handler:
                await self.handle_exception(e, "shutdown_stop_cleanup")

        # Stop hierarchical failure detector (AD-30)
        try:
            await self._hierarchical_detector.stop()
        except Exception as e:
            if self._error_handler:
                await self.handle_exception(e, "shutdown_stop_hierarchical_detector")

        # 5. Log final audit event
        self._audit_log.record(
            AuditEventType.NODE_LEFT,
            node=self_addr,
            reason="graceful_shutdown",
        )

    async def stop(
        self, drain_timeout: float = 5, broadcast_leave: bool = True
    ) -> None:
        """
        Stop the server. Alias for graceful_shutdown with minimal drain time.

        For tests or quick shutdown, use this. For production, prefer
        graceful_shutdown() with appropriate drain_timeout.
        """
        await self._graceful_shutdown(
            drain_timeout=drain_timeout, broadcast_leave=broadcast_leave
        )

        try:
            await super().shutdown()

        except Exception:
            import traceback

            print(traceback.format_exc())

    def get_current_leader(self) -> tuple[str, int] | None:
        """Get the current leader, if known."""
        return self._leader_election.get_current_leader()

    def is_leader(self) -> bool:
        """Check if this node is the current leader."""
        return self._leader_election.state.is_leader()

    def get_leadership_status(self) -> dict:
        """Get current leadership status for debugging."""
        return self._leader_election.get_status()

    async def increase_failure_detector(self, event_type: str = "probe_timeout"):
        """Increase local health score based on event type."""
        if event_type == "probe_timeout":
            self._local_health.on_probe_timeout()
        elif event_type == "refutation":
            self._local_health.on_refutation_needed()
        elif event_type == "missed_nack":
            self._local_health.on_missed_nack()
        elif event_type == "event_loop_lag":
            self._local_health.on_event_loop_lag()
        elif event_type == "event_loop_critical":
            self._local_health.on_event_loop_critical()
        else:
            self._local_health.increment()

    async def decrease_failure_detector(self, event_type: str = "successful_probe"):
        """Decrease local health score based on event type."""
        if event_type == "successful_probe":
            self._local_health.on_successful_probe()
        elif event_type == "successful_nack":
            self._local_health.on_successful_nack()
        elif event_type == "event_loop_recovered":
            self._local_health.on_event_loop_recovered()
        else:
            self._local_health.decrement()

    def get_lhm_adjusted_timeout(
        self, base_timeout: float, target_node_id: str | None = None
    ) -> float:
        """
        Get timeout adjusted by Local Health Multiplier, degradation level, peer health,
        and Vivaldi-based latency (AD-35 Task 12.6.3).

        Phase 6.2: When probing a peer that we know is overloaded (via health gossip),
        we extend the timeout to avoid false failure detection.

        AD-35: When Vivaldi coordinates are available, adjust timeout based on estimated RTT
        to account for geographic distance.

        Formula: timeout = base × lhm × degradation × latency_mult × confidence_adj
        - latency_mult = min(10.0, max(1.0, estimated_rtt / reference_rtt))
        - confidence_adj = 1.0 + (coordinate_error / 10.0)

        Args:
            base_timeout: Base probe timeout in seconds
            target_node_id: Optional node ID of the probe target for peer-aware adjustment

        Returns:
            Adjusted timeout in seconds
        """
        lhm_multiplier = self._local_health.get_multiplier()
        degradation_multiplier = self._degradation.get_timeout_multiplier()
        base_adjusted = base_timeout * lhm_multiplier * degradation_multiplier

        # AD-35 Task 12.6.3: Apply Vivaldi-based latency multiplier
        if target_node_id:
            peer_coord = self._coordinate_tracker.get_peer_coordinate(target_node_id)
            if peer_coord is not None:
                # Estimate RTT with upper confidence bound for conservative timeout
                estimated_rtt_ms = self._coordinate_tracker.estimate_rtt_ucb_ms(
                    peer_coordinate=peer_coord
                )
                reference_rtt_ms = 10.0  # Same-datacenter baseline (10ms)

                # Latency multiplier: 1.0x for same-DC, up to 10.0x for cross-continent
                latency_multiplier = min(
                    10.0,
                    max(1.0, estimated_rtt_ms / reference_rtt_ms)
                )

                # Confidence adjustment based on coordinate quality
                # Lower quality (higher error) → higher adjustment (more conservative)
                quality = self._coordinate_tracker.coordinate_quality(peer_coord)
                confidence_adjustment = 1.0 + (1.0 - quality) * 0.5

                base_adjusted *= latency_multiplier * confidence_adjustment

        # Apply peer health-aware timeout adjustment (Phase 6.2)
        if target_node_id:
            return self._peer_health_awareness.get_probe_timeout(
                target_node_id, base_adjusted
            )

        return base_adjusted

    def get_self_incarnation(self) -> int:
        """Get this node's current incarnation number."""
        return self._incarnation_tracker.get_self_incarnation()

    def increment_incarnation(self) -> int:
        """Increment and return this node's incarnation number (for refutation)."""
        return self._incarnation_tracker.increment_self_incarnation()

    def encode_message_with_incarnation(
        self,
        msg_type: bytes,
        target: tuple[str, int] | None = None,
        incarnation: int | None = None,
    ) -> bytes:
        """Encode a SWIM message with incarnation number."""
        inc = incarnation if incarnation is not None else self.get_self_incarnation()
        msg = msg_type + b":" + str(inc).encode()
        if target:
            msg += b">" + f"{target[0]}:{target[1]}".encode()
        return msg

    def decode_message_with_incarnation(
        self,
        data: bytes,
    ) -> tuple[bytes, int, tuple[str, int] | None]:
        """Decode a SWIM message with incarnation number."""
        parts = data.split(b">", maxsplit=1)
        msg_part = parts[0]

        target = None
        if len(parts) > 1:
            target_str = parts[1].decode()
            host, port = target_str.split(":", maxsplit=1)
            target = (host, int(port))

        msg_parts = msg_part.split(b":", maxsplit=1)
        msg_type = msg_parts[0]
        incarnation = int(msg_parts[1].decode()) if len(msg_parts) > 1 else 0

        return msg_type, incarnation, target

    async def _parse_incarnation_safe(
        self,
        message: bytes,
        source: tuple[str, int],
    ) -> int:
        """
        Parse incarnation number from message safely.

        Returns 0 on parse failure but logs the error for monitoring.
        """
        msg_parts = message.split(b":", maxsplit=1)
        if len(msg_parts) > 1:
            try:
                return int(msg_parts[1].decode())
            except ValueError as e:
                await self.handle_error(
                    MalformedMessageError(
                        message,
                        f"Invalid incarnation number: {e}",
                        source,
                    )
                )
        return 0

    async def _parse_term_safe(
        self,
        message: bytes,
        source: tuple[str, int],
    ) -> int:
        """
        Parse term number from message safely.

        Returns 0 on parse failure but logs the error for monitoring.
        """
        msg_parts = message.split(b":", maxsplit=1)
        if len(msg_parts) > 1:
            try:
                return int(msg_parts[1].decode())
            except ValueError as e:
                await self.handle_error(
                    MalformedMessageError(
                        message,
                        f"Invalid term number: {e}",
                        source,
                    )
                )
        return 0

    async def _parse_leadership_claim(
        self,
        message: bytes,
        source: tuple[str, int],
    ) -> tuple[int, int]:
        """
        Parse term and LHM from leader-claim or pre-vote-req message.

        Returns (term, lhm) tuple, with 0 for any failed parses.
        """
        msg_parts = message.split(b":", maxsplit=2)
        term = 0
        lhm = 0

        if len(msg_parts) >= 2:
            try:
                term = int(msg_parts[1].decode())
            except ValueError as e:
                await self.handle_error(
                    MalformedMessageError(message, f"Invalid term: {e}", source)
                )

        if len(msg_parts) >= 3:
            try:
                lhm = int(msg_parts[2].decode())
            except ValueError as e:
                await self.handle_error(
                    MalformedMessageError(message, f"Invalid LHM: {e}", source)
                )

        return term, lhm

    async def _parse_pre_vote_response(
        self,
        message: bytes,
        source: tuple[str, int],
    ) -> tuple[int, bool]:
        """
        Parse term and granted from pre-vote-resp message.

        Returns (term, granted) tuple.
        """
        msg_parts = message.split(b":", maxsplit=2)
        term = 0
        granted = False

        if len(msg_parts) >= 2:
            try:
                term = int(msg_parts[1].decode())
            except ValueError as e:
                await self.handle_error(
                    MalformedMessageError(message, f"Invalid term: {e}", source)
                )

        if len(msg_parts) >= 3:
            granted = msg_parts[2].decode() == "1"

        return term, granted

    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: Status,
    ) -> bool:
        """
        Check if a message about a node should be processed.

        Uses check_message_freshness to get detailed rejection reason,
        then handles each case appropriately:
        - FRESH: Process the message
        - DUPLICATE: Silent ignore (normal in gossip protocols)
        - STALE: Log as error (may indicate network issues)
        - INVALID: Log as error (bug or corruption)
        - SUSPICIOUS: Log as error (possible attack)
        """
        freshness = self._incarnation_tracker.check_message_freshness(
            node, incarnation, status
        )

        if freshness == MessageFreshness.FRESH:
            return True

        # Get current state for logging context
        current_incarnation = self._incarnation_tracker.get_node_incarnation(node)
        current_state = self._incarnation_tracker.get_node_state(node)
        current_status = current_state.status.decode() if current_state else "unknown"

        if freshness == MessageFreshness.DUPLICATE:
            # Duplicates are completely normal in gossip - debug log only, no error handler
            self._task_runner.run(
                self._udp_logger.log,
                ServerInfo(
                    message=f"[DUPLICATE] {node[0]}:{node[1]} incarnation={incarnation} status={status.decode()} "
                    f"(current: incarnation={current_incarnation} status={current_status})",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                ),
            )
        elif freshness == MessageFreshness.STALE:
            # Stale messages may indicate delayed network or state drift
            self._task_runner.run(
                self.handle_error,
                StaleMessageError(node, incarnation, current_incarnation),
            )
        elif freshness == MessageFreshness.INVALID:
            # Invalid incarnation - log as protocol error
            self._task_runner.run(
                self.handle_error,
                ProtocolError(
                    f"Invalid incarnation {incarnation} from {node[0]}:{node[1]}",
                    severity=ErrorSeverity.DEGRADED,
                    node=node,
                    incarnation=incarnation,
                ),
            )
        elif freshness == MessageFreshness.SUSPICIOUS:
            # Suspicious jump - possible attack or serious bug
            self._task_runner.run(
                self.handle_error,
                ProtocolError(
                    f"Suspicious incarnation jump to {incarnation} from {node[0]}:{node[1]} "
                    f"(current: {current_incarnation})",
                    severity=ErrorSeverity.DEGRADED,
                    node=node,
                    incarnation=incarnation,
                    current_incarnation=current_incarnation,
                ),
            )

        return False

    def _make_network_error(
        self,
        e: OSError,
        target: tuple[str, int],
        operation: str,
    ) -> NetworkError:
        """
        Create the appropriate NetworkError subclass based on OSError type.

        Returns ConnectionRefusedError for ECONNREFUSED, otherwise NetworkError.
        """
        import errno

        if e.errno == errno.ECONNREFUSED:
            return SwimConnectionRefusedError(target)
        return NetworkError(
            f"{operation} to {target[0]}:{target[1]} failed: {e}",
            target=target,
        )

    def _is_duplicate_message(
        self,
        addr: tuple[str, int],
        data: bytes,
    ) -> bool:
        """
        Check if a message is a duplicate using content hash.

        Messages are considered duplicates if:
        1. Same hash seen within dedup window
        2. Hash is in seen_messages dict

        Returns True if duplicate (should skip), False if new.
        """
        # Create hash from source + message content
        msg_hash = hash((addr, data))
        now = time.monotonic()

        if msg_hash in self._seen_messages:
            seen_time = self._seen_messages[msg_hash]
            if now - seen_time < self._dedup_window:
                self._dedup_stats["duplicates"] += 1
                self._metrics.increment("messages_deduplicated")
                return True
            # Seen but outside window - update timestamp
            self._seen_messages[msg_hash] = now
        else:
            # New message - track it
            self._seen_messages[msg_hash] = now

        self._dedup_stats["unique"] += 1
        return False

    def get_dedup_stats(self) -> dict:
        """Get message deduplication statistics."""
        return {
            "duplicates": self._dedup_stats["duplicates"],
            "unique": self._dedup_stats["unique"],
            "cache_size": len(self._seen_messages),
            "window_seconds": self._dedup_window,
        }

    async def _check_rate_limit(self, addr: tuple[str, int]) -> bool:
        """
        Check if a sender is within rate limits using token bucket.

        Each sender has a token bucket that refills over time.
        If bucket is empty, message is rejected.

        Returns True if allowed, False if rate limited.
        """
        now = time.monotonic()

        if addr not in self._rate_limits:
            # New sender - initialize bucket
            self._rate_limits[addr] = {
                "tokens": self._rate_limit_tokens,
                "last_refill": now,
            }

        bucket = self._rate_limits[addr]

        # Refill tokens based on elapsed time
        elapsed = now - bucket["last_refill"]
        refill = int(elapsed * self._rate_limit_refill)
        if refill > 0:
            bucket["tokens"] = min(
                bucket["tokens"] + refill,
                self._rate_limit_tokens,
            )
            bucket["last_refill"] = now

        # Check if we have tokens
        if bucket["tokens"] > 0:
            bucket["tokens"] -= 1
            self._rate_limit_stats["accepted"] += 1
            return True
        else:
            self._rate_limit_stats["rejected"] += 1
            self._metrics.increment("messages_rate_limited")
            # Log rate limit violation
            await self.handle_error(
                ResourceError(
                    f"Rate limit exceeded for {addr[0]}:{addr[1]}",
                    source=addr,
                    tokens=bucket["tokens"],
                )
            )
            return False

    def get_rate_limit_stats(self) -> dict:
        """Get rate limiting statistics."""
        return {
            "accepted": self._rate_limit_stats["accepted"],
            "rejected": self._rate_limit_stats["rejected"],
            "tracked_senders": len(self._rate_limits),
            "tokens_per_sender": self._rate_limit_tokens,
            "refill_rate": self._rate_limit_refill,
        }

    def get_metrics(self) -> dict:
        """Get all protocol metrics for monitoring."""
        return self._metrics.to_dict()

    def get_audit_log(self) -> list[dict]:
        """Get recent audit events for debugging and compliance."""
        return self._audit_log.export()

    def get_audit_stats(self) -> dict:
        """Get audit log statistics."""
        return self._audit_log.get_stats()

    async def _validate_target(
        self,
        target: tuple[str, int] | None,
        msg_type: bytes,
        addr: tuple[str, int],
    ) -> bool:
        """
        Validate that target is present when required.

        Logs MalformedMessageError if target is missing.
        Returns True if valid, False if invalid.
        """
        if target is None:
            await self.handle_error(
                MalformedMessageError(
                    msg_type,
                    "Missing target address in message",
                    addr,
                )
            )
            return False
        return True

    async def _clear_stale_state(self, node: tuple[str, int]) -> None:
        """
        Clear any stale state when a node rejoins.

        This prevents:
        - Acting on old suspicions after rejoin
        - Stale indirect probes interfering with new probes
        - Incarnation confusion from old state
        """
        # Clear any active suspicion via hierarchical detector
        await self._hierarchical_detector.refute_global(
            node,
            self._incarnation_tracker.get_node_incarnation(node) + 1,
        )

        # Clear any pending indirect probes
        if self._indirect_probe_manager.get_pending_probe(node):
            self._indirect_probe_manager.cancel_probe(node)

        # Remove from gossip buffer (old state)
        self._gossip_buffer.remove_node(node)

    def _on_gossip_overflow(self, evicted: int, capacity: int) -> None:
        """
        Called when gossip buffer overflows and updates are evicted.

        This indicates high churn or undersized buffer.
        """
        self._metrics.increment("gossip_buffer_overflows")
        self._task_runner.run(
            self.handle_error,
            ResourceError(
                f"Gossip buffer overflow: evicted {evicted} updates at capacity {capacity}",
                evicted=evicted,
                capacity=capacity,
            ),
        )

    def update_node_state(
        self,
        node: tuple[str, int],
        status: Status,
        incarnation: int,
        timestamp: float,
    ) -> bool:
        """
        Update the state of a node. Returns True if state changed.

        Also invokes _on_node_join_callbacks when a node transitions from
        DEAD to OK/ALIVE (recovery detection).
        """
        # Get previous state before updating
        previous_state = self._incarnation_tracker.get_node_state(node)
        was_dead = previous_state and previous_state.status == b"DEAD"
        prev_status = previous_state.status if previous_state else b"UNKNOWN"

        # Perform the actual update
        updated = self._incarnation_tracker.update_node(
            node, status, incarnation, timestamp
        )

        # If node was DEAD and is now being set to OK/ALIVE, invoke join callbacks
        # This handles recovery detection for nodes that come back after being marked dead
        if updated and was_dead and status in (b"OK", b"ALIVE"):
            self._metrics.increment("node_recoveries_detected")
            self._audit_log.record(
                AuditEventType.NODE_RECOVERED,
                node=node,
                incarnation=incarnation,
            )

            # Add back to probe scheduler
            self._probe_scheduler.add_member(node)

            # Invoke registered callbacks (composition pattern)
            for callback in self._on_node_join_callbacks:
                try:
                    callback(node)
                except Exception as e:
                    self._task_runner.run(
                        self.handle_exception, e, "on_node_join_callback (recovery)"
                    )

        return updated

    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> SuspicionState | None:
        """
        Start suspecting a node or add confirmation to existing suspicion.

        Per AD-29: Only confirmed peers can be suspected. If we've never
        successfully communicated with a peer, we can't meaningfully suspect
        them - they might just not be up yet during cluster formation.

        AD-29 Task 12.3.4: UNCONFIRMED → SUSPECT transitions are explicitly
        prevented by the formal state machine.
        """
        # AD-29: Guard against suspecting unconfirmed peers
        # Use formal state machine check which prevents UNCONFIRMED → SUSPECT
        if not self._incarnation_tracker.can_suspect_node(node):
            self._metrics.increment("suspicions_skipped_unconfirmed")
            return None

        self._metrics.increment("suspicions_started")
        self._audit_log.record(
            AuditEventType.NODE_SUSPECTED,
            node=node,
            from_node=from_node,
            incarnation=incarnation,
        )
        self._incarnation_tracker.update_node(
            node,
            b"SUSPECT",
            incarnation,
            time.monotonic(),
        )
        return await self._hierarchical_detector.suspect_global(
            node, incarnation, from_node
        )

    async def confirm_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """Add a confirmation to an existing suspicion."""
        result = await self._hierarchical_detector.confirm_global(
            node, incarnation, from_node
        )
        if result:
            self._metrics.increment("suspicions_confirmed")
        return result

    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """Refute a suspicion - the node proved it's alive."""
        if await self._hierarchical_detector.refute_global(node, incarnation):
            self._metrics.increment("suspicions_refuted")
            self._audit_log.record(
                AuditEventType.NODE_REFUTED,
                node=node,
                incarnation=incarnation,
            )
            self._incarnation_tracker.update_node(
                node,
                b"OK",
                incarnation,
                time.monotonic(),
            )
            return True
        return False

    def is_node_suspected(self, node: tuple[str, int]) -> bool:
        """Check if a node is currently under suspicion."""
        return self._hierarchical_detector.is_suspected_global(node)

    def get_suspicion_timeout(self, node: tuple[str, int]) -> float | None:
        """Get the remaining timeout for a suspicion, if any."""
        return self._hierarchical_detector.get_time_remaining_global(node)

    def get_random_proxy_nodes(
        self,
        target: tuple[str, int],
        k: int = 3,
    ) -> list[tuple[str, int]]:
        """
        Get k random nodes to use as proxies for indirect probing.

        Phase 6.2: Prefers healthy nodes over stressed/overloaded ones.
        We avoid using stressed peers as proxies because:
        1. They may be slow to respond, causing indirect probe timeouts
        2. We want to reduce load on already-stressed nodes
        """
        nodes: Nodes = self._context.read("nodes")
        self_addr = self._get_self_udp_addr()

        # Snapshot nodes.items() to avoid dict mutation during iteration
        all_candidates = [
            node
            for node, queue in list(nodes.items())
            if node != target and node != self_addr
        ]

        if not all_candidates:
            return []

        # Phase 6.2: Filter to prefer healthy proxies
        # We need node_id (string) but have (host, port) tuples
        # For filtering, use addr-based lookup since health gossip uses node_id
        healthy_candidates: list[tuple[str, int]] = []
        stressed_candidates: list[tuple[str, int]] = []

        for node in all_candidates:
            # Convert to node_id format for health lookup
            node_id = f"{node[0]}:{node[1]}"
            if self._peer_health_awareness.should_use_as_proxy(node_id):
                healthy_candidates.append(node)
            else:
                stressed_candidates.append(node)

        # Prefer healthy nodes, but fall back to stressed if necessary
        k = min(k, len(all_candidates))
        if k <= 0:
            return []

        if len(healthy_candidates) >= k:
            return random.sample(healthy_candidates, k)
        elif healthy_candidates:
            # Use all healthy + some stressed to fill
            result = healthy_candidates.copy()
            remaining = k - len(result)
            if remaining > 0 and stressed_candidates:
                additional = random.sample(
                    stressed_candidates, min(remaining, len(stressed_candidates))
                )
                result.extend(additional)
            return result
        else:
            # No healthy candidates, use stressed
            return random.sample(stressed_candidates, min(k, len(stressed_candidates)))

    def _get_self_udp_addr(self) -> tuple[str, int]:
        """Get this server's UDP address as a tuple."""
        host, port = self._udp_addr_slug.decode().split(":")
        return (host, int(port))

    async def initiate_indirect_probe(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """
        Initiate indirect probing for a target node with retry support.

        If a proxy send fails, we try another proxy. Tracks which proxies
        were successfully contacted.
        """
        k = self._indirect_probe_manager.k_proxies
        proxies = self.get_random_proxy_nodes(target, k)

        if not proxies:
            return False

        base_timeout = self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        probe = self._indirect_probe_manager.start_indirect_probe(
            target=target,
            requester=self._get_self_udp_addr(),
            timeout=timeout,
        )
        self._metrics.increment("indirect_probes_sent")

        target_addr = f"{target[0]}:{target[1]}".encode()
        msg = b"ping-req:" + str(incarnation).encode() + b">" + target_addr

        successful_sends = 0
        failed_proxies: list[tuple[str, int]] = []

        for proxy in proxies:
            probe.add_proxy(proxy)
            success = await self._send_indirect_probe_to_proxy(proxy, msg, timeout)
            if success:
                successful_sends += 1
            else:
                failed_proxies.append(proxy)

        # If some proxies failed, try to get replacement proxies
        if failed_proxies and successful_sends < k:
            # Get additional proxies excluding those we already tried
            all_tried = set(proxies)
            additional = self.get_random_proxy_nodes(target, k - successful_sends)

            for proxy in additional:
                if proxy not in all_tried:
                    success = await self._send_indirect_probe_to_proxy(
                        proxy, msg, timeout
                    )
                    if success:
                        probe.add_proxy(proxy)
                        successful_sends += 1

        if successful_sends == 0:
            await self.handle_error(IndirectProbeTimeoutError(target, proxies, timeout))
            return False

        return True

    async def _send_indirect_probe_to_proxy(
        self,
        proxy: tuple[str, int],
        msg: bytes,
        timeout: float,
    ) -> bool:
        """
        Send an indirect probe request to a single proxy.

        Returns True if send succeeded, False otherwise.
        """
        try:
            await self.send(proxy, msg, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
        except OSError as e:
            await self.handle_error(
                self._make_network_error(e, proxy, "Indirect probe")
            )
            return False
        except Exception as e:
            await self.handle_exception(
                e, f"indirect_probe_proxy_{proxy[0]}_{proxy[1]}"
            )
            return False

    async def handle_indirect_probe_response(
        self,
        target: tuple[str, int],
        is_alive: bool,
    ) -> None:
        """Handle response from an indirect probe."""
        if is_alive:
            if self._indirect_probe_manager.record_ack(target):
                await self.decrease_failure_detector("successful_probe")

    async def broadcast_refutation(self) -> int:
        """
        Broadcast an alive message to refute any suspicions about this node.

        Uses retry_with_backoff for each send since refutation is critical.
        Tracks send failures and logs them but doesn't fail the overall operation.

        Rate limited to prevent incarnation exhaustion attacks - if an attacker
        sends many probes/suspects about us, we don't want to burn through
        all possible incarnation numbers.
        """
        # Rate limiting check
        now = time.monotonic()
        window_elapsed = now - self._last_refutation_time

        if window_elapsed >= self._refutation_rate_limit_window:
            # Reset window
            self._last_refutation_time = now
            self._refutation_count_in_window = 1
        else:
            self._refutation_count_in_window += 1
            if self._refutation_count_in_window > self._refutation_rate_limit_tokens:
                # Rate limited - return current incarnation without incrementing
                return self._incarnation_tracker.get_self_incarnation()

        new_incarnation = self.increment_incarnation()

        nodes: Nodes = self._context.read("nodes")
        self_addr = self._get_self_udp_addr()

        self_addr_bytes = f"{self_addr[0]}:{self_addr[1]}".encode()
        msg = b"alive:" + str(new_incarnation).encode() + b">" + self_addr_bytes

        base_timeout = self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        successful = 0
        failed = 0

        # Snapshot nodes to avoid dict mutation during iteration
        for node in list(nodes.keys()):
            if node != self_addr:
                success = await self._send_with_retry(node, msg, timeout)
                if success:
                    successful += 1
                else:
                    failed += 1

        # Log if we had failures but don't fail the operation
        if failed > 0 and self._error_handler:
            await self.handle_error(
                NetworkError(
                    f"Refutation broadcast: {failed}/{successful + failed} sends failed",
                    severity=ErrorSeverity.TRANSIENT
                    if successful > 0
                    else ErrorSeverity.DEGRADED,
                    successful=successful,
                    failed=failed,
                )
            )

        return new_incarnation

    async def _send_with_retry(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float,
    ) -> bool:
        """
        Send a message with retry using retry_with_backoff.

        Returns True on success, False if all retries exhausted.
        """
        result = await retry_with_result(
            lambda: self._send_once(target, message, timeout),
            policy=PROBE_RETRY_POLICY,
            on_retry=self._on_send_retry,
        )

        if result.success:
            self.record_network_success()
            return True
        else:
            if result.last_error:
                await self.handle_exception(
                    result.last_error, f"send_retry_{target[0]}_{target[1]}"
                )
            return False

    async def _send_once(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float,
    ) -> bool:
        """Single send attempt (for use with retry_with_backoff)."""
        await self.send(target, message, timeout=timeout)
        return True

    async def _on_send_retry(
        self,
        attempt: int,
        error: Exception,
        delay: float,
    ) -> None:
        """Callback for retry attempts - update LHM."""
        await self.increase_failure_detector("send_retry")

    async def broadcast_suspicion(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> None:
        """
        Broadcast a suspicion about a node to all other members.

        Tracks send failures for monitoring but continues to all nodes.
        """
        nodes: Nodes = self._context.read("nodes")
        self_addr = self._get_self_udp_addr()

        target_addr_bytes = f"{target[0]}:{target[1]}".encode()
        msg = b"suspect:" + str(incarnation).encode() + b">" + target_addr_bytes

        base_timeout = self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        successful = 0
        failed = 0

        # Snapshot nodes to avoid dict mutation during iteration
        for node in list(nodes.keys()):
            if node != self_addr and node != target:
                success = await self._send_broadcast_message(node, msg, timeout)
                if success:
                    successful += 1
                else:
                    failed += 1

        if failed > 0 and self._error_handler:
            await self.handle_error(
                NetworkError(
                    f"Suspicion broadcast for {target}: {failed}/{successful + failed} sends failed",
                    severity=ErrorSeverity.TRANSIENT,
                    successful=successful,
                    failed=failed,
                    suspected_node=target,
                )
            )

    async def _send_broadcast_message(
        self,
        node: tuple[str, int],
        msg: bytes,
        timeout: float,
    ) -> bool:
        """
        Send a single broadcast message with error handling.

        Returns True on success, False on failure.
        Logs individual failures but doesn't raise exceptions.
        """
        try:
            await self.send(node, msg, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            # Timeouts are expected for unreachable nodes
            return False
        except OSError as e:
            # Network errors - log but don't fail broadcast
            if self._error_handler:
                await self.handle_error(self._make_network_error(e, node, "Broadcast"))
            return False
        except Exception as e:
            await self.handle_exception(e, f"broadcast_to_{node[0]}_{node[1]}")
            return False

    async def _send_to_addr(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float | None = None,
    ) -> bool:
        """
        Send a message to a specific address with error handling.

        Returns True on success, False on failure.
        """
        if timeout is None:
            base_timeout = self._context.read("current_timeout")
            timeout = self.get_lhm_adjusted_timeout(base_timeout)

        try:
            await self.send(target, message, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            await self.handle_error(ProbeTimeoutError(target, timeout))
            return False
        except OSError as e:
            await self.handle_error(self._make_network_error(e, target, "Send"))
            return False
        except Exception as e:
            await self.handle_exception(e, f"send_to_{target[0]}_{target[1]}")
            return False

    async def _send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        """
        Send a probe to target and wait for response indication.

        Since UDP is connectionless, we can't directly receive a response.
        Instead, we send the probe and wait a short time for the node's
        state to update (indicating an ack was processed).

        Returns True if target appears alive, False otherwise.
        """
        base_timeout = self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        target_addr = f"{target[0]}:{target[1]}".encode()
        msg = b"probe>" + target_addr

        # Get current node state before probe
        state_before = self._incarnation_tracker.get_node_state(target)
        last_seen_before = state_before.last_update_time if state_before else 0

        try:
            # Send probe with error handling
            await self.send(target, msg, timeout=timeout)

            # Wait for potential response to arrive
            await asyncio.sleep(min(timeout * 0.7, 0.5))

            # Check if node state was updated (indicates response received)
            state_after = self._incarnation_tracker.get_node_state(target)
            if state_after:
                # Node was updated more recently than before our probe
                if state_after.last_update_time > last_seen_before:
                    return state_after.status == b"OK"
                # Node status is OK
                if state_after.status == b"OK":
                    return True

            return False

        except asyncio.TimeoutError:
            await self.handle_error(ProbeTimeoutError(target, timeout))
            return False
        except OSError as e:
            await self.handle_error(self._make_network_error(e, target, "Probe"))
            return False
        except Exception as e:
            await self.handle_exception(e, f"probe_and_wait_{target[0]}_{target[1]}")
            return False

    @udp.send("receive")
    async def send(
        self,
        addr: tuple[str, int],
        message: bytes,
        timeout: int | None = None,
    ) -> bytes:
        """
        Prepare outgoing UDP message before sending.

        This hook adds piggybacked gossip data (membership + health) to
        outgoing messages for O(log n) dissemination.
        """
        # Add piggyback data (membership + health gossip) to outgoing messages
        message_with_piggyback = self._add_piggyback_safe(message)

        return (
            addr,
            message_with_piggyback,
            timeout,
        )

    @udp.handle("receive")
    async def process(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> Message:
        """
        Process UDP response data before it's returned to the caller.

        This hook intercepts responses from UDP sends (e.g., probe responses).
        We extract any embedded state for Serf-style passive discovery.
        """
        if not data:
            return data

        # Check if this is an ACK response - need to complete pending probe future
        msg_type = data.split(b">", maxsplit=1)[0].split(b":", maxsplit=1)[0]

        # Convert addr to tuple format for lookup - addr comes as bytes 'host:port'
        # but _pending_probe_acks uses tuple (host, port) keys
        addr_tuple: tuple[str, int] | None = None
        if isinstance(addr, bytes):
            try:
                host, port_str = addr.decode().split(":", 1)
                addr_tuple = (host, int(port_str))
            except (ValueError, UnicodeDecodeError):
                pass
        elif isinstance(addr, tuple):
            addr_tuple = addr

        if msg_type == b"ack" and addr_tuple:
            # Complete pending probe future for this address
            pending_future = self._pending_probe_acks.get(addr_tuple)
            if pending_future:
                if not pending_future.done():
                    pending_future.set_result(True)

        # Extract embedded state from response (Serf-style)
        # Response format: msg_type>host:port#|sbase64_state
        clean_data = self._extract_embedded_state(data, addr)
        return clean_data

    @udp.receive()
    async def receive(
        self,
        addr: tuple[str, int],
        data: Message,
        clock_time: int,
    ) -> Message:
        try:
            # Validate message size first - prevent memory issues from oversized messages
            if len(data) > MAX_UDP_PAYLOAD:
                await self.handle_error(
                    ProtocolError(
                        f"Message from {addr[0]}:{addr[1]} exceeds size limit "
                        f"({len(data)} > {MAX_UDP_PAYLOAD})",
                        size=len(data),
                        limit=MAX_UDP_PAYLOAD,
                        source=addr,
                    )
                )
                return b"nack>" + self._udp_addr_slug

            # Validate message has content
            if len(data) == 0:
                await self.handle_error(
                    ProtocolError(
                        f"Empty message from {addr[0]}:{addr[1]}",
                        source=addr,
                    )
                )
                return b"nack>" + self._udp_addr_slug

            # Check rate limit - drop if sender is flooding
            if not await self._check_rate_limit(addr):
                return b"nack>" + self._udp_addr_slug

            # Check for duplicate messages
            if self._is_duplicate_message(addr, data):
                # Duplicate - still send ack but don't process
                return b"ack>" + self._udp_addr_slug

            # Extract health gossip piggyback first (format: #|hentry1;entry2;...)
            health_piggyback_idx = data.find(self._HEALTH_SEPARATOR)
            if health_piggyback_idx > 0:
                health_piggyback_data = data[health_piggyback_idx:]
                data = data[:health_piggyback_idx]
                self._health_gossip_buffer.decode_and_process_piggyback(
                    health_piggyback_data
                )

            # Extract membership piggyback (format: #|mtype:incarnation:host:port...)
            piggyback_idx = data.find(self._MEMBERSHIP_SEPARATOR)
            if piggyback_idx > 0:
                main_data = data[:piggyback_idx]
                piggyback_data = data[piggyback_idx:]
                await self.process_piggyback_data(piggyback_data)
                data = main_data

            # Delegate to the message dispatcher for handler-based processing
            return await self._message_dispatcher.dispatch(addr, data, clock_time)

        except ValueError as error:
            # Message parsing error
            await self.handle_error(MalformedMessageError(data, str(error), addr))
            return b"nack"
        except Exception as error:
            await self.handle_exception(error, "receive")
            return b"nack"

    # ==========================================================================
    # Legacy receive() match statement - preserved for reference during testing
    # This entire block will be removed after confirming handlers work correctly
    # ==========================================================================
    async def _legacy_receive_removed(self) -> None:
        """Placeholder to mark where old receive() logic was removed."""
        # The old receive() method contained a ~600 line match statement.
        # It has been replaced by the message_handling module with separate
        # handler classes for each message type:
        #   - membership/: ack, nack, join, leave
        #   - probing/: probe, ping-req, ping-req-ack
        #   - suspicion/: alive, suspect
        #   - leadership/: leader-claim, leader-vote, leader-elected, etc.
        #   - cross_cluster/: xprobe, xack, xnack
        #
        # See hyperscale/distributed_rewrite/swim/message_handling/
        pass
