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
import collections
import math
import random
import time
from base64 import b64decode, b64encode
from typing import Callable, Literal

from hyperscale.distributed.env import Env
from hyperscale.distributed.server import udp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)
from hyperscale.distributed.server.protocol import MessagePriority
from hyperscale.distributed.taskex.run import Run
from hyperscale.distributed.swim.coordinates import CoordinateTracker
from hyperscale.distributed.models.coordinates import NetworkCoordinate, VivaldiConfig
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerDebug,
    ServerWarning,
    ServerError,
)

# Core types and utilities
from .core.types import Status, Ctx, UpdateType, Message
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
    StaleMessageError,
    ConnectionRefusedError as SwimConnectionRefusedError,
    ResourceError,
    TaskOverloadError,
    NotEligibleError,
)
from .admission import (
    SwimAdmissionClass,
    classify_swim_payload,
    has_auxiliary_piggyback,
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
from .detection.incarnation_store import IncarnationStore

# SuspicionManager replaced by HierarchicalFailureDetector (AD-30)
from .detection.indirect_probe_manager import IndirectProbeManager
from .detection.probe_scheduler import ProbeScheduler
from .detection.hierarchical_failure_detector import (
    HierarchicalFailureDetector,
    HierarchicalConfig,
    NodeStatus,
)
from .detection.peer_probe_reliability_tracker import (
    PeerProbeReliabilityTracker,
    PeerProbeReliabilityConfig,
)

# Gossip
from .gossip.gossip_buffer import GossipBuffer, MAX_UDP_PAYLOAD
from .gossip.piggyback_update import PiggybackUpdate
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
from hyperscale.distributed.protocol.version import CURRENT_PROTOCOL_VERSION

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
        # AD-35 Task 12.7: Vivaldi configuration
        vivaldi_config: "VivaldiConfig | None" = None,
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
        # Incarnation persistence settings
        incarnation_storage_dir: str
        | None = None,  # Directory for incarnation persistence
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Generate unique node identity
        self._node_id = NodeId.generate(datacenter=dc_id, priority=priority)

        # Store node role for role-aware failure detection (AD-35 Task 12.4.2)
        self._node_role: str = (
            node_role or "worker"
        )  # Default to worker if not specified

        # Store Vivaldi config for metrics and observability (AD-35 Task 12.7)
        self._vivaldi_config: VivaldiConfig = vivaldi_config or VivaldiConfig()

        # State embedder for Serf-style heartbeat embedding
        self._state_embedder: StateEmbedder = state_embedder or NullStateEmbedder()

        # Initialize SWIM components
        self._local_health = LocalHealthMultiplier()
        self._incarnation_tracker = IncarnationTracker()
        self._indirect_probe_manager = IndirectProbeManager()

        self._incarnation_storage_dir = incarnation_storage_dir
        self._incarnation_store: IncarnationStore | None = None

        # Direct probe ACK tracking - key is target addr, value is Future set when ACK received
        self._pending_probe_acks: dict[tuple[str, int], asyncio.Future[bool]] = {}
        self._pending_probe_start: dict[tuple[str, int], float] = {}

        # AD-35 Task 12.7: Initialize CoordinateTracker with config
        self._coordinate_tracker = CoordinateTracker(config=self._vivaldi_config)

        # Role-aware confirmation manager for unconfirmed peers (AD-35 Task 12.5.6)
        # Initialized after CoordinateTracker so it can use Vivaldi-based timeouts
        from hyperscale.distributed.swim.roles.confirmation_manager import (
            RoleAwareConfirmationManager,
        )
        from hyperscale.distributed.models.distributed import NodeRole

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

        # Per-peer probe-reliability tracker. Tracks each peer's
        # recent probe-success rate. Consumed by the direct-probe
        # retry-budget formula (``_compute_direct_probe_budget``) —
        # *not* by the suspicion bracket. AD-30 specifies the bracket
        # uses LHM/peer_load/vivaldi only; per-peer probe history is
        # an operational signal for the retry layer.
        self._peer_probe_reliability = PeerProbeReliabilityTracker(
            config=PeerProbeReliabilityConfig(),
        )
        self._global_suspicion_started_at: dict[tuple[str, int], float] = {}

        # AD-53 burst-failure cluster-degradation signal.
        # When K distinct direct+indirect probe failures occur within
        # W seconds, the prober temporarily widens confirmation work
        # for other silent peers. Each accelerated target still goes
        # through the normal direct probe -> indirect probe -> SUSPECT
        # pipeline; burst mode changes scheduling pressure, not SWIM's
        # membership state machine. See ``docs/architecture/AD_53.md``.
        _burst_default_env = Env()
        _burst_env = kwargs.get("env") or _burst_default_env
        self._burst_failure_threshold: int = (
            int(
                getattr(
                    _burst_env,
                    "BURST_FAILURE_THRESHOLD",
                    _burst_default_env.BURST_FAILURE_THRESHOLD,
                )
            )
        )
        self._burst_failure_window_seconds: float = (
            float(
                getattr(
                    _burst_env,
                    "BURST_FAILURE_WINDOW_SECONDS",
                    _burst_default_env.BURST_FAILURE_WINDOW_SECONDS,
                )
            )
        )
        self._burst_failure_observations: collections.deque[
            tuple[float, tuple[str, int]]
        ] = collections.deque()
        self._burst_failure_probe_concurrency: int = max(
            1,
            min(
                16,
                self._burst_failure_threshold
                * max(1, self._indirect_probe_manager.k_proxies)
                * 2,
            ),
        )
        self._burst_failure_active: bool = False
        self._burst_failure_run: Run | None = None

        # Hierarchical failure detector for multi-layer detection (AD-30)
        # - Global layer: Machine-level liveness (via timing wheel)
        # - Job layer: Per-job responsiveness (via adaptive polling)
        # Uses polling instead of cancel/reschedule to avoid timer starvation
        #
        # ``task_runner`` is threaded in so async callbacks (notably
        # ``_on_suspicion_expired``) are dispatched correctly per CLAUDE.md
        # — without it, async-def callbacks are silently dropped at GC
        # time and the SUSPECT->DEAD transition never executes.
        #
        # ``peer_health_awareness`` and the Vivaldi-quality callable feed
        # the AD-30 prob-OR composition of suspicion timers (LHM is
        # supplied via ``get_lhm_multiplier``; the bracket uses all
        # three of the AD-30 signals together via bounded
        # reliability composition).
        self._hierarchical_detector = HierarchicalFailureDetector(
            on_global_death=self._on_suspicion_expired,
            on_global_death_sync=self._record_global_death_sync,
            on_error=self._on_hierarchical_detector_error,
            get_n_members=self._get_member_count,
            get_global_indirect_witness_count=self._get_indirect_probe_witness_count,
            get_lhm_multiplier=self._get_lhm_multiplier,
            task_runner=self._task_runner,
            peer_health_awareness=self._peer_health_awareness,
            get_vivaldi_quality_multiplier=(
                self._compute_vivaldi_quality_multiplier_for_node
            ),
            on_expiration_diagnostic=self._log_suspicion_expiration_diagnostic,
        )

        # Initialize leader election with configurable parameters from Env
        from hyperscale.distributed.swim.leadership.leader_state import (
            LeaderState,
        )
        from hyperscale.distributed.swim.leadership.leader_eligibility import (
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
        self._rate_limits: BoundedDict[tuple[str, int, str], dict] = BoundedDict(
            max_size=rate_limit_cache_size * 8,
            eviction_policy="LRA",
        )
        self._rate_limit_tokens: int = rate_limit_tokens
        self._rate_limit_refill: float = rate_limit_refill
        self._swim_rate_limit_profiles: dict[str, tuple[int, float]] = {
            "probe_response": (
                max(rate_limit_tokens * 4, 256),
                max(rate_limit_refill * 10.0, 100.0),
            ),
            "probe_request": (
                max(rate_limit_tokens * 2, 128),
                max(rate_limit_refill * 5.0, 50.0),
            ),
            "lifecycle_direct": (
                max(rate_limit_tokens, 100),
                max(rate_limit_refill * 2.0, 20.0),
            ),
            "membership_gossip": (rate_limit_tokens, rate_limit_refill),
            "leadership": (
                max(rate_limit_tokens, 100),
                max(rate_limit_refill * 2.0, 20.0),
            ),
            "auxiliary": (
                max(rate_limit_tokens // 2, 50),
                max(rate_limit_refill, 10.0),
            ),
            "unknown": (
                max(rate_limit_tokens // 2, 50),
                max(rate_limit_refill, 10.0),
            ),
        }
        self._swim_rate_limit_stats: dict[str, dict[str, int]] = {
            admission_class: {"accepted": 0, "rejected": 0}
            for admission_class in self._swim_rate_limit_profiles
        }
        self._rate_limit_stats = {
            "accepted": 0,
            "rejected": 0,
        }

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
        self._leave_dissemination_queue: dict[
            tuple[str, int],
            tuple[int, bytes, bytes],
        ] = {}
        self._leave_dissemination_drain_scheduled: bool = False
        self._join_dissemination_queue: dict[
            tuple[str, int],
            tuple[int, bytes, bytes],
        ] = {}
        self._join_dissemination_drain_scheduled: bool = False

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

        # Peers that have completed an explicit registration handshake
        # with this node. This is a stricter gate than ``_confirmed_peers``:
        # passive observation (a probe-ack from a peer we've never heard
        # of) confirms a peer in ``_confirmed_peers`` for liveness
        # purposes, but does not register them. Only the explicit
        # registration handshakes do:
        #
        #   * TCP register endpoints — ``worker_register``,
        #     ``manager_peer_register``, ``gate_register`` (manager
        #     side) and ``register_node`` (worker side, when a manager
        #     registers down).
        #   * SWIM ``join`` handshake — ``join_handler``.
        #   * ``reset_peer_for_rejoin`` — TCP-register-driven rejoin
        #     of an address that was previously registered.
        #
        # ``start_suspicion`` checks membership in this set; a peer
        # that has not registered cannot be SUSPECTed. This prevents
        # boot-time false-positive suspicion on peers that probes
        # have transiently observed (e.g. a peer that probed *us*
        # before completing its own registration), and it cleanly
        # separates "we've seen this node alive" from "this node is
        # an authoritative cluster member whose liveness we are
        # responsible for". Removed on DEAD/LEAVE; re-added on
        # explicit re-registration.
        self._registered_peers: set[tuple[str, int]] = set()

        # Hierarchical detector callbacks already set in __init__
        # Debug: track port for logging
        self._hierarchical_detector._node_port = self._udp_port

        # Message dispatcher for handler-based message processing
        # ServerAdapter wraps this server to implement ServerInterface protocol
        self._server_adapter = ServerAdapter(self)
        self._message_dispatcher = MessageDispatcher(self._server_adapter)
        register_default_handlers(self._message_dispatcher, self._server_adapter)

    def _create_background_task(
        self,
        coro,
        name: str,
    ) -> asyncio.Task:
        """
        Create a background task with automatic error logging.

        This helper ensures that background tasks don't fail silently by
        attaching a done callback that logs any exceptions. Use this instead
        of bare asyncio.create_task() for all long-running background tasks.

        Args:
            coro: The coroutine to run as a background task.
            name: A descriptive name for the task (used in error messages).

        Returns:
            The created asyncio.Task with error callback attached.
        """
        task = asyncio.create_task(coro, name=name)
        task.add_done_callback(lambda t: self._handle_background_task_error(t, name))
        return task

    def _handle_background_task_error(self, task: asyncio.Task, name: str) -> None:
        """
        Handle errors from background tasks by logging them.

        This callback is attached to all background tasks created via
        _create_background_task(). It prevents silent failures by ensuring
        all task exceptions are logged.

        Args:
            task: The completed task.
            name: The descriptive name of the task.
        """
        if task.cancelled():
            return

        exception = task.exception()
        if exception is None:
            return

        node_id_value = getattr(self, "_node_id", None)
        node_id_short = node_id_value.short if node_id_value is not None else "unknown"

        host, port = self._get_self_udp_addr()

        if self._task_runner is not None and self._udp_logger is not None:
            # Pass the bound method + args separately so the TaskRunner is
            # the one to actually invoke and await the coroutine. The
            # earlier ``run(logger.log(msg))`` form built the coroutine
            # inline and handed it to ``run`` (which expects a callable),
            # leaking it as an unawaited coroutine.
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"Background task '{name}' failed ({type(exception).__name__}): {exception}",
                    node_id=node_id_short,
                    node_host=host,
                    node_port=port,
                ),
            )

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

    def get_vivaldi_metrics(self) -> dict[str, any]:
        """
        Get Vivaldi coordinate system metrics (AD-35 Task 12.8).

        Returns:
            Dictionary containing:
            - local_coordinate: Current coordinate dict
            - coordinate_error: Current error value
            - is_converged: Whether coordinate has converged
            - peer_count: Number of tracked peers
            - config: Active Vivaldi configuration
        """
        local_coord = self._coordinate_tracker.get_coordinate()
        return {
            "local_coordinate": local_coord.to_dict(),
            "coordinate_error": local_coord.error,
            "is_converged": self._coordinate_tracker.is_converged(),
            "peer_count": len(self._coordinate_tracker._peers),
            "sample_count": local_coord.sample_count,
            "config": {
                "dimensions": self._vivaldi_config.dimensions,
                "ce": self._vivaldi_config.ce,
                "error_decay": self._vivaldi_config.error_decay,
                "convergence_threshold": self._vivaldi_config.convergence_error_threshold,
            },
        }

    def get_confirmation_metrics(self) -> dict[str, any]:
        """
        Get role-aware confirmation metrics (AD-35 Task 12.9).

        Returns:
            Dictionary containing:
            - unconfirmed_count: Total unconfirmed peers
            - unconfirmed_by_role: Breakdown by role
            - manager_metrics: Detailed confirmation manager metrics
        """
        return {
            "unconfirmed_count": self._confirmation_manager.get_unconfirmed_peer_count(),
            "unconfirmed_by_role": self._confirmation_manager.get_unconfirmed_peers_by_role(),
            "manager_metrics": self._confirmation_manager.get_metrics(),
        }

    def validate_ad35_state(self) -> dict[str, bool | str]:
        """
        Validate AD-35 implementation state (AD-35 Task 12.10).

        Performs sanity checks on Vivaldi coordinates, role classification,
        and confirmation manager state.

        Returns:
            Dictionary with validation results:
            - coordinate_valid: Coordinate is within reasonable bounds
            - coordinate_converged: Coordinate has converged
            - role_set: Node role is configured
            - confirmation_manager_active: Confirmation manager is tracking peers
            - errors: List of any validation errors
        """
        errors: list[str] = []
        coord = self._coordinate_tracker.get_coordinate()

        # Validate coordinate bounds
        coord_valid = True
        if coord.error < 0 or coord.error > 10.0:
            coord_valid = False
            errors.append(f"Coordinate error out of bounds: {coord.error}")

        for dimension_value in coord.vec:
            if abs(dimension_value) > 10000:  # Sanity check: ~10s RTT max
                coord_valid = False
                errors.append(f"Coordinate dimension out of bounds: {dimension_value}")
                break

        # Validate convergence
        coord_converged = self._coordinate_tracker.is_converged()

        # Validate role
        role_set = self._node_role in ("gate", "manager", "worker")
        if not role_set:
            errors.append(f"Invalid node role: {self._node_role}")

        # Validate confirmation manager
        confirmation_active = (
            self._confirmation_manager.get_unconfirmed_peer_count() >= 0
        )

        return {
            "coordinate_valid": coord_valid,
            "coordinate_converged": coord_converged,
            "role_set": role_set,
            "confirmation_manager_active": confirmation_active,
            "errors": errors if errors else None,
            "overall_valid": coord_valid and role_set and confirmation_active,
        }

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

    def notify_node_dead(
        self,
        node: tuple[str, int],
        incarnation: int,
        source: str,
    ) -> None:
        """Invoke registered node-dead callbacks for an accepted DEAD transition."""
        self._incarnation_tracker.record_node_death(
            node,
            incarnation,
            time.monotonic(),
        )
        self._probe_scheduler.remove_member(node)
        self._peer_probe_reliability.remove_peer(node)
        self._registered_peers.discard(node)
        self._global_suspicion_started_at.pop(node, None)

        for callback in self._on_node_dead_callbacks:
            try:
                callback(node)
            except Exception as error:
                self._task_runner.run(
                    self.handle_exception,
                    error,
                    f"on_node_dead_callback ({source})",
                )

    def _get_registered_node_id_for_addr(self, addr: tuple[str, int]) -> str | None:
        """Return the registered node identity currently bound to ``addr``."""
        return None

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

    async def add_unconfirmed_peer(
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
            await self._incarnation_tracker.add_unconfirmed_node(peer)

            # AD-35 Task 12.5.6: Track with RoleAwareConfirmationManager
            from hyperscale.distributed.models.distributed import NodeRole

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

    def record_peer_role(self, peer: tuple[str, int], role: str) -> None:
        """Record a peer's self-declared role into ``_peer_roles``.

        Invoked from message handlers (join, gossip) when a peer tells
        us what role it is. The role drives:

        * leader-election cohort filtering (only same-tier peers count
          toward majority and receive leader-claim/heartbeat broadcasts);
        * role-aware probe scheduling and confirmation strategies;
        * any membership decision that should not blur tier boundaries.

        Unknown / unparseable role strings are dropped; the role map
        keeps the prior value rather than corrupting it.
        """
        from hyperscale.distributed.models.distributed import NodeRole

        if peer == self._get_self_udp_addr():
            return
        try:
            self._peer_roles[peer] = NodeRole(role.lower())
        except (ValueError, AttributeError):
            return

    async def confirm_peer(self, peer: tuple[str, int], incarnation: int = 0) -> bool:
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
        await self._incarnation_tracker.confirm_node(peer, incarnation)

        # Enrol the newly-confirmed peer in the probe scheduler. Without this
        # step the probe cycle (whose membership snapshot is taken once at
        # start-up and refreshed only on death/leave) will never probe peers
        # that joined after start-up — leaving SWIM's failure detector dark
        # for the rest of the run and forcing detection onto the coarser
        # deadline-enforcement fallback.
        self._probe_scheduler.add_member(peer)

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

    def register_peer(self, peer: tuple[str, int]) -> None:
        """Mark ``peer`` as having completed a registration handshake.

        Idempotent. Called from every explicit registration entry
        point so ``start_suspicion`` can authoritatively gate on
        "this peer is a cluster member I am responsible for". Passive
        observation paths (probe-ack, alive gossip, suspect gossip)
        must *not* call this — they update liveness without granting
        registration status.
        """
        if peer == self._get_self_udp_addr():
            return
        self._registered_peers.add(peer)

    def unregister_peer(self, peer: tuple[str, int]) -> None:
        """Drop ``peer`` from the registration set.

        Called when the peer is declared DEAD or has explicitly LEFT
        the cluster; the peer must complete a fresh registration
        handshake (TCP register or SWIM JOIN) before SUSPECT can fire
        on them again.
        """
        self._registered_peers.discard(peer)
        self._global_suspicion_started_at.pop(peer, None)

    def is_peer_registered(self, peer: tuple[str, int]) -> bool:
        """Whether ``peer`` has completed an explicit registration handshake."""
        return peer in self._registered_peers

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

    async def remove_peer_tracking(self, peer: tuple[str, int]) -> None:
        """
        Remove a peer from all confirmation tracking (AD-29 Task 12.3.6).

        Use when a peer is intentionally removed from the cluster.
        Also removes from incarnation tracker state machine.

        For the "address re-use by a new instance" case (e.g. a worker
        restarts and re-registers at the same UDP address) call
        :meth:`reset_peer_for_rejoin` instead — that variant promotes
        the tracker entry to OK at a *higher* incarnation than the
        dead predecessor, so any stale DEAD gossip still in flight is
        rejected by the freshness check rather than re-marking the
        new instance as dead.
        """
        self._confirmed_peers.discard(peer)
        self._unconfirmed_peers.discard(peer)
        self._unconfirmed_peer_added_at.pop(peer, None)
        # AD-29: Also remove from formal state machine
        await self._incarnation_tracker.remove_node(peer)

    async def reset_peer_for_rejoin(self, peer: tuple[str, int]) -> int:
        """Reset SWIM state for ``peer`` so a brand-new instance can join.

        Called when an out-of-band signal (e.g. ``worker_register``
        over TCP) tells us the process at ``peer`` is a new instance
        rather than the predecessor we had been tracking. The reset
        wipes every per-peer cache that would short-circuit fresh
        SWIM engagement (``_confirmed_peers`` membership, active
        suspicion bracket, ``globally_dead`` marker, adaptive
        extension tracker, probe-reliability history, death record)
        and seeds the incarnation tracker with a fresh ``OK`` entry
        at the predecessor's documented rejoin threshold
        (``death_incarnation + minimum_rejoin_incarnation_bump``).
        Returns the rejoin incarnation so the caller can gossip an
        ALIVE for it — without that gossip, peers that still hold the
        dead predecessor's entry will keep treating the address as
        dead.

        Picking the rejoin threshold (not just ``previous + 1``) is
        load-bearing for stale-gossip resistance: any in-flight DEAD
        update propagating at the zombie-bound incarnation would
        otherwise beat a smaller bump and re-mark the new instance
        as DEAD. Clearing the death record alongside is the
        symmetric step — it keeps the zombie check from later
        flagging the new instance's own gossip if its self-incarnation
        happens to fall below the threshold (e.g. fresh-state restart
        with no persisted incarnation).
        """
        rejoin_incarnation = max(
            1,
            self._incarnation_tracker.get_required_rejoin_incarnation(peer),
        )

        self._confirmed_peers.discard(peer)
        self._unconfirmed_peers.discard(peer)
        self._unconfirmed_peer_added_at.pop(peer, None)
        self._global_suspicion_started_at.pop(peer, None)
        await self._incarnation_tracker.remove_node(peer)
        self._incarnation_tracker.clear_death_record(peer)
        self._peer_probe_reliability.remove_peer(peer)
        if self._hierarchical_detector is not None:
            # Clear any active suspicion bracket and ``globally_dead``
            # marker; both would survive ``remove_node`` and short-
            # circuit the new instance's SWIM engagement.
            await self._hierarchical_detector.refute_global(
                peer, incarnation=rejoin_incarnation
            )
            await self._hierarchical_detector.clear_global_death(peer)
            self._hierarchical_detector.remove_extension_tracker(peer)

        # Re-seat at the bumped incarnation. ``confirm_node`` creates
        # a fresh ``OK`` entry because ``remove_node`` left the slot
        # empty — same code path as a brand-new peer's first
        # confirmation, but pinned to the rejoin incarnation.
        await self._incarnation_tracker.confirm_node(peer, rejoin_incarnation)
        self._confirmed_peers.add(peer)
        # The caller of ``reset_peer_for_rejoin`` is by contract a TCP
        # registration handler processing a fresh register for this
        # address (i.e. the new instance is going through the
        # registration handshake). Mark the peer as registered so
        # SUSPECT can fire once the cluster's gossip catches up; the
        # ``_on_suspicion_expired`` path will unregister on DEAD.
        self._registered_peers.add(peer)

        # Gossip an ALIVE at the rejoin incarnation so other peers
        # supersede their stale DEAD entries for this address.
        self.queue_gossip_update("alive", peer, rejoin_incarnation)

        return rejoin_incarnation

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
        # Calling ``init_hierarchical_detector`` replaces the HFD instance
        # that ``HealthAwareServer.__init__`` already constructed with the
        # canonical ``_on_suspicion_expired`` callback wired in. If the
        # caller does not supply ``on_global_death`` we must re-wire the
        # default here — otherwise the new HFD has ``None`` for
        # ``on_global_death`` and wheel expirations fire but no callback
        # runs: no DEAD log, no incarnation-tracker transition to DEAD,
        # no ``dead`` gossip, no ``_on_node_dead_callbacks`` fan-out, no
        # unregister. The bracket fires into the void.
        if on_global_death is None:
            on_global_death = self._on_suspicion_expired
        self._hierarchical_detector = HierarchicalFailureDetector(
            config=config,
            on_global_death=on_global_death,
            # Synchronous death record happens BEFORE the async DEAD
            # callback drains, so a concurrent ``reset_peer_for_rejoin``
            # can read the right rejoin threshold rather than defaulting
            # to ``1`` and getting overwritten by the queued async fire.
            on_global_death_sync=self._record_global_death_sync,
            on_job_death=on_job_death,
            on_error=self._on_hierarchical_detector_error,
            get_n_members=self._get_member_count,
            get_global_indirect_witness_count=self._get_indirect_probe_witness_count,
            get_job_n_members=get_job_n_members,
            get_lhm_multiplier=self._get_lhm_multiplier,
            task_runner=self._task_runner,
            peer_health_awareness=self._peer_health_awareness,
            get_vivaldi_quality_multiplier=(
                self._compute_vivaldi_quality_multiplier_for_node
            ),
        )
        return self._hierarchical_detector

    def _record_global_death_sync(
        self, node: tuple[str, int], incarnation: int
    ) -> None:
        """Synchronous death-event hook for HFD wheel expirations.

        Records the death in the incarnation tracker the moment the
        wheel fires, before the async ``_on_suspicion_expired``
        callback drains. The recording is what
        ``get_required_rejoin_incarnation`` reads, so any concurrent
        rejoin (e.g. TCP worker_register handler running while the
        async fire is queued) sees the correct ``death_incarnation +
        minimum_rejoin_incarnation_bump`` threshold rather than the
        default-zero fallback that lets a stale async fire overwrite
        the freshly-installed OK entry.
        """
        self._incarnation_tracker.record_node_death(
            node, incarnation, time.monotonic()
        )

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
        """Handle a SWIM protocol error.

        ``error`` is typed as ``SwimError`` but the receive path may
        deliver a raw ``Exception`` (e.g. a ``TypeError`` from
        upstream message parsing) before it has been wrapped into a
        ``SwimError``. Route raw exceptions through
        ``error_handler.handle_exception`` which wraps them — passing
        an unwrapped exception to ``error_handler.handle`` directly
        trips on ``error.cause`` access (the prior guard only covered
        ``error.category`` and let the ``.cause`` access through,
        replacing the original exception with an
        ``AttributeError`` that then masked the real bug).
        """
        if not isinstance(error, SwimError):
            if self._error_handler:
                await self._error_handler.handle_exception(
                    error, operation="handle_error"
                )
            return

        # Track error by category for SwimError instances.
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

    async def initialize_incarnation_store(self) -> int:
        """
        Initialize the incarnation store and return the starting incarnation.

        Must be called after the server has started and the UDP port is known.
        If incarnation_storage_dir was provided, this creates and initializes
        the IncarnationStore for persistent incarnation tracking.

        Returns:
            The initial incarnation number to use.
        """
        if self._incarnation_storage_dir is None:
            return 0

        from pathlib import Path

        node_address = f"{self._host}:{self._udp_port}"
        self._incarnation_store = IncarnationStore(
            storage_directory=Path(self._incarnation_storage_dir),
            node_address=node_address,
        )

        if self._udp_logger:
            self._incarnation_store.set_logger(
                self._udp_logger,
                self._host,
                self._udp_port,
            )

        initial_incarnation = await self._incarnation_store.initialize()
        self._incarnation_tracker.self_incarnation = initial_incarnation

        return initial_incarnation

    async def persist_incarnation(self, incarnation: int) -> bool:
        """
        Persist an incarnation number to disk.

        Called after incrementing incarnation (e.g., during refutation)
        to ensure the new value survives restarts.

        Returns:
            True if persisted successfully, False otherwise.
        """
        if self._incarnation_store is None:
            return False
        return await self._incarnation_store.update_incarnation(incarnation)

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

        # Log the change. ``_on_degradation_level_change`` runs from a
        # sync health-callback path, so ``await`` is unavailable — route
        # the coroutine through the TaskRunner with the callable + args
        # form so it is actually awaited (not constructed and dropped).
        if hasattr(self, "_udp_logger"):
            try:
                from hyperscale.logging.hyperscale_logging_models import (
                    ServerInfo as ServerInfoLog,
                )

                self._task_runner.run(
                    self._udp_logger.log,
                    ServerInfoLog(
                        message=f"Degradation {direction}: {old_level.name} -> {new_level.name} ({policy.description})",
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=self._node_id.short
                        if hasattr(self, "_node_id")
                        else 0,
                    ),
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

    _STATE_SEPARATOR = b"#|s"
    _MEMBERSHIP_SEPARATOR = b"#|m"
    _HEALTH_SEPARATOR = b"#|h"
    _WORKER_STATE_SEPARATOR = b"#|w"
    _VIVALDI_SEPARATOR = b"#|v"
    # AD-26 H7b: extension decision dissemination. Sits between
    # worker-state (#|w) and vivaldi (#|v) on the wire — added
    # second-to-last when encoding, stripped second when decoding.
    _EXTENSION_DECISION_SEPARATOR = b"#|x"
    # AD-26 H8b: extension outcome dissemination. Sits between
    # decision (#|x) and vivaldi (#|v); appended after #|x and
    # stripped right after #|v on the receive path.
    _EXTENSION_OUTCOME_SEPARATOR = b"#|o"

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

    async def _process_embedded_state(
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
        await self._state_embedder.process_state(state_data, source_addr)

    def _get_worker_state_piggyback(self, max_size: int) -> bytes:
        return b""

    async def _process_worker_state_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        pass

    def _get_extension_decision_piggyback(self, max_size: int) -> bytes:
        """AD-26 H7b hook: return piggyback bytes for the
        ``ExtensionDecisionGossipBuffer`` if the subclass provides
        one. Default implementation returns empty bytes — only
        ``ManagerServer`` produces extension events.
        """
        return b""

    async def _process_extension_decision_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """AD-26 H7b hook: ingest a ``#|x`` piggyback frame received
        from a peer. Default implementation is a no-op — only
        ``ManagerServer`` consumes extension events.
        """
        pass

    def _get_extension_outcome_piggyback(self, max_size: int) -> bytes:
        """AD-26 H8b hook: return piggyback bytes for the
        ``ExtensionOutcomeGossipBuffer`` if the subclass provides
        one. Default implementation returns empty bytes — only
        ``ManagerServer`` produces outcome events.
        """
        return b""

    async def _process_extension_outcome_piggyback(
        self,
        piggyback_data: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        """AD-26 H8b hook: ingest a ``#|o`` piggyback frame received
        from a peer. Default implementation is a no-op — only
        ``ManagerServer`` consumes outcome events.
        """
        pass

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

    async def _extract_embedded_state(
        self,
        message: bytes,
        source_addr: tuple[str, int],
        process_piggybacks: bool = True,
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
            process_piggybacks: Whether to process auxiliary piggyback data.

        Returns:
            The message with embedded state and piggyback removed.
        """
        msg_end = len(message)
        vivaldi_piggyback: bytes | None = None
        extension_outcome_piggyback: bytes | None = None
        extension_decision_piggyback: bytes | None = None
        worker_state_piggyback: bytes | None = None
        health_piggyback: bytes | None = None
        membership_piggyback: bytes | None = None

        vivaldi_idx = message.find(b"#|v")
        if vivaldi_idx > 0:
            vivaldi_piggyback = message[vivaldi_idx + 3 :]
            msg_end = vivaldi_idx

        # AD-26 H8b: outcome channel sits between extension-decision
        # and vivaldi on the wire — strip after vivaldi but before
        # the decision channel.
        extension_outcome_idx = message.find(
            self._EXTENSION_OUTCOME_SEPARATOR, 0, msg_end
        )
        if extension_outcome_idx > 0:
            extension_outcome_piggyback = message[extension_outcome_idx:msg_end]
            msg_end = extension_outcome_idx

        # AD-26 H7b: extension decision channel sits between
        # worker-state and outcome on the wire — strip it before
        # we move on to the older worker-state channel.
        extension_decision_idx = message.find(
            self._EXTENSION_DECISION_SEPARATOR, 0, msg_end
        )
        if extension_decision_idx > 0:
            extension_decision_piggyback = message[extension_decision_idx:msg_end]
            msg_end = extension_decision_idx

        worker_state_idx = message.find(self._WORKER_STATE_SEPARATOR, 0, msg_end)
        if worker_state_idx > 0:
            worker_state_piggyback = message[worker_state_idx:msg_end]
            msg_end = worker_state_idx

        health_idx = message.find(self._HEALTH_SEPARATOR, 0, msg_end)
        if health_idx > 0:
            health_piggyback = message[health_idx:msg_end]
            msg_end = health_idx

        membership_idx = message.find(self._MEMBERSHIP_SEPARATOR, 0, msg_end)
        if membership_idx > 0:
            membership_piggyback = message[membership_idx:msg_end]
            msg_end = membership_idx

        addr_sep_idx = message.find(b">", 0, msg_end)
        if addr_sep_idx < 0:
            if process_piggybacks:
                if vivaldi_piggyback:
                    self._process_vivaldi_piggyback(vivaldi_piggyback, source_addr)
                if extension_outcome_piggyback:
                    self._task_runner.run(
                        self._process_extension_outcome_piggyback,
                        extension_outcome_piggyback,
                        source_addr,
                    )
                if extension_decision_piggyback:
                    self._task_runner.run(
                        self._process_extension_decision_piggyback,
                        extension_decision_piggyback,
                        source_addr,
                    )
                if worker_state_piggyback:
                    self._task_runner.run(
                        self._process_worker_state_piggyback,
                        worker_state_piggyback,
                        source_addr,
                    )
                if health_piggyback:
                    self._health_gossip_buffer.decode_and_process_piggyback(
                        health_piggyback
                    )
                if membership_piggyback:
                    self._task_runner.run(
                        self.process_piggyback_data,
                        membership_piggyback,
                        source_addr,
                    )
            return message[:msg_end] if msg_end < len(message) else message

        state_sep_idx = message.find(self._STATE_SEPARATOR, addr_sep_idx, msg_end)

        if process_piggybacks:
            if vivaldi_piggyback:
                self._process_vivaldi_piggyback(vivaldi_piggyback, source_addr)
            if extension_outcome_piggyback:
                self._task_runner.run(
                    self._process_extension_outcome_piggyback,
                    extension_outcome_piggyback,
                    source_addr,
                )
            if extension_decision_piggyback:
                self._task_runner.run(
                    self._process_extension_decision_piggyback,
                    extension_decision_piggyback,
                    source_addr,
                )
            if worker_state_piggyback:
                self._task_runner.run(
                    self._process_worker_state_piggyback,
                    worker_state_piggyback,
                    source_addr,
                )
            if health_piggyback:
                self._health_gossip_buffer.decode_and_process_piggyback(health_piggyback)
            if membership_piggyback:
                self._task_runner.run(
                    self.process_piggyback_data,
                    membership_piggyback,
                    source_addr,
                )

        # No state separator - return clean message
        if state_sep_idx < 0:
            return message[:msg_end] if msg_end < len(message) else message

        if not process_piggybacks:
            return message[:state_sep_idx]

        # Extract and decode state
        # Slice once: encoded_state is between state_sep and msg_end
        # Skip 3 bytes for '#|s' separator
        encoded_state = message[state_sep_idx + 3 : msg_end]

        try:
            state_data = b64decode(encoded_state)
            await self._process_embedded_state(state_data, source_addr)
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
            from hyperscale.distributed.models.coordinates import NetworkCoordinate

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

        remaining_after_health = MAX_UDP_PAYLOAD - len(message_with_health)

        worker_state_piggyback = self._get_worker_state_piggyback(
            remaining_after_health
        )
        message_with_worker_state = message_with_health + worker_state_piggyback

        remaining_after_worker = MAX_UDP_PAYLOAD - len(message_with_worker_state)

        # AD-26 H7b: extension decision dissemination. Encoded after
        # worker-state so the parser strips it before falling back
        # to the worker-state channel (the parser walks right-to-
        # left through #|v -> #|o -> #|x -> #|w -> #|h -> #|m).
        extension_decision_piggyback = self._get_extension_decision_piggyback(
            remaining_after_worker
        )
        message_with_extension = (
            message_with_worker_state + extension_decision_piggyback
        )

        remaining_after_extension = MAX_UDP_PAYLOAD - len(message_with_extension)

        # AD-26 H8b: extension outcome dissemination. Encoded after
        # the decision channel and before vivaldi.
        extension_outcome_piggyback = self._get_extension_outcome_piggyback(
            remaining_after_extension
        )
        message_with_outcome = message_with_extension + extension_outcome_piggyback

        remaining_after_outcome = MAX_UDP_PAYLOAD - len(message_with_outcome)
        if remaining_after_outcome >= 150:
            import json

            coord = self._coordinate_tracker.get_coordinate()
            coord_dict = coord.to_dict()
            coord_json = json.dumps(coord_dict, separators=(",", ":")).encode()
            vivaldi_piggyback = b"#|v" + coord_json

            if (
                len(message_with_outcome) + len(vivaldi_piggyback)
                <= MAX_UDP_PAYLOAD
            ):
                return message_with_outcome + vivaldi_piggyback

        return message_with_outcome

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
            or any(
                class_stats["accepted"] > MAX_COUNTER
                or class_stats["rejected"] > MAX_COUNTER
                for class_stats in self._swim_rate_limit_stats.values()
            )
        ):
            self._rate_limit_stats = {
                "accepted": 0,
                "rejected": 0,
            }
            self._swim_rate_limit_stats = {
                admission_class: {"accepted": 0, "rejected": 0}
                for admission_class in self._swim_rate_limit_profiles
            }

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
                        node_id=self._node_id.short
                        if hasattr(self, "_node_id")
                        else "unknown",
                    )
                )

        # Update metrics for stale unconfirmed peers
        if stale_count > 0:
            self._metrics.record_counter("stale_unconfirmed_peers", stale_count)

    def _setup_leader_election(self) -> None:
        """Initialize leader election callbacks after server is started."""
        self._leader_election.set_callbacks(
            broadcast_message=self._broadcast_leadership_message,
            get_member_count=self._get_election_member_count,
            get_lhm_score=lambda: self._local_health.score,
            self_addr=self._get_self_udp_addr(),
            on_error=self._handle_election_error,
            should_refuse_leadership=lambda: self._degradation.should_refuse_leadership(),
            task_runner=self._task_runner,
            on_election_started=self._on_election_started,
            on_heartbeat_sent=self._on_heartbeat_sent,
        )

        # Wire the project Logger into the leader-election machinery so
        # _log_debug / _log_debug_sync calls land in the server log
        # stream alongside everything else. Without this, the election
        # subsystem is silent — any failure to converge (pre-vote not
        # reaching peers, broadcast targets empty, etc.) is invisible.
        self._leader_election.set_logger(
            logger=self._udp_logger,
            node_host=self._host,
            node_port=self._udp_port,
            node_id=self._node_id.short,
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

    async def _broadcast_leadership_message(self, message: bytes) -> None:
        """
        Broadcast a leadership message to same-tier peers.

        Leadership lives at the manager / gate tier; workers don't run
        leader election and don't grant pre-votes. Sending leader-claim
        to a worker wastes bandwidth and pollutes its log. Filter the
        SWIM tracker by ``_peer_roles`` so only same-tier peers receive
        the message. Falls back to the unfiltered tracker if peer roles
        haven't populated yet (early startup) — same fallback shape as
        ``_get_election_member_count``.

        Sends are scheduled via the task runner with error tracking;
        delivery failures bubble through ``_send_leadership_message``
        retries before reaching the LHM penalty path.
        """
        from hyperscale.distributed.models.distributed import NodeRole

        self_addr = self._get_self_udp_addr()
        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        all_nodes = list(self._incarnation_tracker.node_states.keys())
        try:
            self_role: NodeRole | None = NodeRole(self._node_role.lower())
        except (ValueError, AttributeError):
            self_role = None

        if self_role is not None and self._peer_roles:
            targets = [
                node
                for node in all_nodes
                if node != self_addr
                and self._peer_roles.get(node) == self_role
            ]
        else:
            targets = [node for node in all_nodes if node != self_addr]

        await self._udp_logger.log(
            ServerDebug(
                message=(
                    f"[Leadership] broadcast self={self_addr} "
                    f"role={self_role} "
                    f"msg_prefix={message[:32]!r} "
                    f"tracker_nodes={len(all_nodes)} "
                    f"same_tier_targets={targets} "
                    f"timeout={timeout:.3f}"
                ),
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            )
        )

        for node in targets:
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
        """Callback for leadership retry attempts.

        Per Lifeguard §4.3, LHM is incremented only on probe-timeout,
        refutation-needed, missed-nack, and (Hyperscale extension)
        event-loop-lag/critical events — *not* on protocol-level
        retries. An election retry could indicate peer slowness,
        network jitter, or any number of non-self-health causes;
        bumping LHM here would conflate operational retries with
        prober self-health and inflate every probe-timeout and
        suspicion bracket cluster-wide during normal election churn
        (especially during cluster spin-up). Election retry telemetry
        belongs in metrics, not LHM.
        """
        self._metrics.increment("leadership_retries")

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
        # Lifeguard's N is the cluster size including self; the tracker only
        # holds peers, so add 1. The floor of 2 prevents the n<=1 branch in
        # SuspicionState.calculate_timeout from returning max_timeout and
        # blowing past the detection budget.
        return max(2, len(self._incarnation_tracker.node_states) + 1)

    def _compute_vivaldi_quality_multiplier_for_node(
        self, node: tuple[str, int]
    ) -> float:
        """Vivaldi confidence-adjustment multiplier for ``node`` (AD-35:183).

        Per AD-35:183:
            ``confidence_adjustment = 1.0 + (vivaldi_error / 10.0)``

        Higher coordinate error = wider posterior uncertainty about
        the network distance to ``node`` = more conservative timeout.
        Returns 1.0 when:

        * the target's Vivaldi coordinate isn't tracked yet (cold
          start / first-ever contact), or
        * the local coordinate engine has not converged enough to
          produce meaningful error estimates.

        Both fall-throughs are intentionally neutral so that a node
        without Vivaldi data doesn't get a *shorter* suspicion timer
        than one with data — that would invert the safety property
        (less knowledge = more aggressive failure declaration).
        """
        host, port = node
        peer_id = f"{host}:{port}"
        try:
            peer_coord = self._coordinate_tracker.get_peer_coordinate(peer_id)
        except Exception:
            return 1.0
        if peer_coord is None:
            return 1.0
        error = float(getattr(peer_coord, "error", 0.0) or 0.0)
        # AD-35:183: 1 + error/10. Clamp to [1.0, 1.5] envelope per
        # AD-35:194-198 worked example (max error penalty seen there
        # is ~1.15× from confidence_adjustment alone).
        adjustment = 1.0 + error / 10.0
        return max(1.0, min(adjustment, 1.5))

    def _is_target_already_suspect_or_dead(
        self, target: tuple[str, int]
    ) -> bool:
        """Return True if probe failures to ``target`` should *not* bump LHM.

        Phase C signal-hygiene gate covering three peer-state cases
        where a missed ack is *not* evidence of prober self-slowness:

        * **SUSPECT** / **DEAD** — peer already concluded to be in
          trouble; further misses are peer-side deadness, not us.
          Without this gate a single dead peer pumps LHM to
          saturation while the suspicion timer scales accordingly,
          slowing detection under failure load (the canonical SWIM
          positive-feedback pathology).
        * **UNCONFIRMED** (AD-29) — peer recorded in our incarnation
          tracker but not yet verified. During cluster spin-up many
          peers are UNCONFIRMED while their startup completes; probes
          from us to them race their socket-bind / handler-registration.
          A timeout in that window is peer-not-ready-yet, not
          our slowness, and bumping LHM here would inflate every
          probe-timeout and suspicion-bracket cluster-wide for the
          duration of cluster formation.

        Reads from the incarnation-tracker's authoritative state per
        AD-46 ("All node state stored in IncarnationTracker.node_states").

        Returns False (i.e. *do* bump LHM) when the tracker has no
        entry for the target. That case is an actual ambiguous miss —
        we have no peer-state context, so the conservative Lifeguard
        default applies.
        """
        try:
            node_state = self._incarnation_tracker.get_node_state(target)
        except Exception:
            return False
        if node_state is None:
            return False
        return node_state.status in (b"SUSPECT", b"DEAD", b"UNCONFIRMED")

    def _should_increment_lhm_for_failed_confirmation(
        self,
        target: tuple[str, int],
    ) -> bool:
        """Return True when a failed probe should count against self-health.

        A confirmed miss to a registered peer is ambiguous in isolation
        but becomes target-failure evidence during a burst. Feeding
        every miss into local LHM during a burst makes peer death
        stretch probe and suspicion timers cluster-wide. Keep LHM for
        genuinely local signals (event-loop lag, missed nack,
        refutation pressure) and the first isolated confirmed miss
        before a burst is established.
        """
        if self._is_target_already_suspect_or_dead(target):
            return False
        if self._burst_failure_active or self._burst_failure_observations:
            return False
        return True

    def _has_indirect_probe_witnesses(self, target: tuple[str, int]) -> bool:
        """Return whether any registered healthy peer can verify ``target``."""
        return self._get_indirect_probe_witness_count(target) > 0

    def _get_indirect_probe_witness_count(self, target: tuple[str, int]) -> int:
        """Return the number of usable indirect-probe witnesses for ``target``."""
        k = self._indirect_probe_manager.k_proxies
        return len(self.get_random_proxy_nodes(target, k))

    def _requires_unwitnessed_dead_confirmation(
        self,
        target: tuple[str, int],
    ) -> bool:
        """Return whether DEAD requires final direct evidence for ``target``."""
        if not self.is_peer_registered(target):
            return False

        node_state = self._incarnation_tracker.get_node_state(target)
        if node_state is not None and node_state.status == b"DEAD":
            return False

        return not self._has_indirect_probe_witnesses(target)

    async def _clear_unwitnessed_suspicion_after_confirmation(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Clear a no-witness suspicion after direct liveness evidence."""
        if self._hierarchical_detector is not None:
            await self._hierarchical_detector.clear_global_death(node)

        cleared = await self._incarnation_tracker.clear_suspicion_after_confirmation(
            node,
            incarnation,
            time.monotonic(),
        )
        self._global_suspicion_started_at.pop(node, None)
        self._gossip_buffer.remove_node(node)
        self._probe_scheduler.add_member(node)

        if cleared:
            self._metrics.increment("suspicions_expired_refuted_direct")
            self._audit_log.record(
                AuditEventType.NODE_REFUTED,
                node=node,
                incarnation=incarnation,
                source="direct_confirmation",
            )

    async def _should_apply_unwitnessed_dead_transition(
        self,
        node: tuple[str, int],
        incarnation: int,
        suspicion_started_at: float,
    ) -> bool:
        """Return whether a no-witness SUSPECT expiry may become DEAD."""
        if not self._requires_unwitnessed_dead_confirmation(node):
            return True

        attempt_count = max(1, self._indirect_probe_manager.k_proxies)
        for attempt_number in range(attempt_count):
            if self._peer_probe_reliability.had_success_since(
                node,
                suspicion_started_at,
            ):
                await self._clear_unwitnessed_suspicion_after_confirmation(
                    node,
                    incarnation,
                )
                return False

            if await self._confirm_peer_reachable_by_swim(node, incarnation):
                await self._clear_unwitnessed_suspicion_after_confirmation(
                    node,
                    incarnation,
                )
                return False

            if attempt_number + 1 < attempt_count:
                await asyncio.sleep(0)

        return True

    def _get_election_member_count(self) -> int:
        """Members that participate in *this node's* leader election.

        Leader election majority must be computed against same-tier
        peers only — a manager's election cohort is the other managers,
        not the workers it knows about. Mixing tiers caused L2 elections
        to fail post-fault: when workers were added to the SWIM
        ``incarnation_tracker``, ``_get_member_count`` rose to 4 and the
        majority threshold (3) became unreachable from the manager-only
        candidate set.

        Counts ``self`` + every same-role peer in ``_peer_roles``.
        Falls back to the broader tracker count if roles haven't been
        populated yet (early in startup, before any role-bearing gossip
        has been processed) so we don't return 0.
        """
        from hyperscale.distributed.models.distributed import NodeRole

        try:
            self_role = NodeRole(self._node_role.lower())
        except (ValueError, AttributeError):
            return self._get_member_count()

        if not self._peer_roles:
            return self._get_member_count()

        same_tier_peers = sum(
            1 for role in self._peer_roles.values() if role == self_role
        )
        return same_tier_peers + 1  # plus self

    async def _on_suspicion_expired(
        self, node: tuple[str, int], incarnation: int
    ) -> None:
        """Callback when a suspicion expires - mark node as DEAD.

        Wheel expirations are dispatched via the TaskRunner and may
        execute with arbitrary lag after the wheel's ``call_later``
        fires (the TaskRunner queue can backlog under contention).
        During that window the tracker entry for ``node`` may have
        been replaced — most commonly by ``reset_peer_for_rejoin``
        when a new instance registers at the same address. In that
        case the suspicion expiry is for the *predecessor* and must
        not flow into the post-DEAD pipeline (gossip, probe-scheduler
        refresh, ``_on_node_dead_callbacks``). The freshness check on
        ``update_node`` rejects the DEAD write when the tracker
        carries a higher incarnation; we gate every downstream side
        effect on that result so the new instance is not
        re-unregistered by a stale fire.
        """
        now = time.monotonic()
        suspicion_started_at = self._global_suspicion_started_at.get(node, now)
        if not await self._should_apply_unwitnessed_dead_transition(
            node,
            incarnation,
            suspicion_started_at,
        ):
            return

        applied = await self._incarnation_tracker.update_node(
            node,
            b"DEAD",
            incarnation,
            now,
        )
        if not applied:
            # Stale wheel-expiration: the tracker has already moved
            # past this incarnation (e.g. via rejoin). Discard the
            # entire post-DEAD pipeline.
            self._metrics.increment("suspicions_expired_stale")
            self._global_suspicion_started_at.pop(node, None)
            return

        self._metrics.increment("suspicions_expired")
        self._global_suspicion_started_at.pop(node, None)
        self._audit_log.record(
            AuditEventType.NODE_CONFIRMED_DEAD,
            node=node,
            incarnation=incarnation,
        )
        # ``record_node_death`` already ran synchronously from HFD's
        # ``_handle_global_expiration`` via ``on_global_death_sync``;
        # no need to repeat it here.
        self.queue_gossip_update("dead", node, incarnation)

        self.update_probe_scheduler_membership()

        # Drop the dead peer's probe-reliability history. CLAUDE.md
        # requires explicit cleanup of long-running per-peer state to
        # prevent leaks across the kill/restart lifecycle. If the peer
        # rejoins it starts with a fresh, empty window (defaulting to
        # reliability=1.0 — the SWIM "assume healthy" baseline).
        self._peer_probe_reliability.remove_peer(node)
        # The peer is dead; their registration is no longer valid. The
        # rejoin path (TCP register or SWIM JOIN) will re-add them to
        # ``_registered_peers`` when the new instance arrives.
        self._registered_peers.discard(node)

        # Invoke registered callbacks (composition pattern)
        for callback in self._on_node_dead_callbacks:
            try:
                callback(node)
            except Exception as e:
                self._task_runner.run(self.handle_exception, e, "on_node_dead_callback")

    def _on_hierarchical_detector_error(
        self,
        error_message: str,
        error: Exception,
    ) -> None:
        if self._task_runner and self._udp_logger:
            self._task_runner.run(
                self._udp_logger.log,
                ServerWarning(
                    message=f"Hierarchical failure detector error: {error_message} - {error}",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short
                    if hasattr(self, "_node_id")
                    else 0,
                ),
            )

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
        role = (
            self._peer_roles.get(node, None) if hasattr(self, "_peer_roles") else None
        )
        # If this is our own node, use our role
        if node == self._get_self_udp_addr():
            role = self._node_role
        node_id = self._node_id.full if node == self._get_self_udp_addr() else (
            self._get_registered_node_id_for_addr(node)
        )
        self._gossip_buffer.add_update(
            update_type,
            node,
            incarnation,
            n_members,
            role,
            node_id,
        )

    def queue_leave_dissemination(
        self,
        target: tuple[str, int],
        incarnation: int,
        target_addr_bytes: bytes | None,
        message: bytes,
    ) -> None:
        """Queue explicit LEAVE dissemination with per-target coalescing."""
        if target_addr_bytes is None:
            return

        existing = self._leave_dissemination_queue.get(target)
        if existing is not None and existing[0] > incarnation:
            return

        self._leave_dissemination_queue[target] = (
            incarnation,
            target_addr_bytes,
            message,
        )

        if self._leave_dissemination_drain_scheduled:
            return

        self._leave_dissemination_drain_scheduled = True
        self._task_runner.run(
            self._drain_leave_dissemination_queue,
            alias="leave_dissemination_drain",
            keep=20,
            max_age="5m",
            keep_policy="COUNT_AND_AGE",
        )

    async def _drain_leave_dissemination_queue(self) -> None:
        """Drain coalesced LEAVE dissemination work through bounded sends."""
        try:
            while self._leave_dissemination_queue:
                pending_items = list(self._leave_dissemination_queue.items())
                self._leave_dissemination_queue.clear()
                await self._send_coalesced_leave_dissemination(pending_items)
        finally:
            self._leave_dissemination_drain_scheduled = False
            if self._leave_dissemination_queue:
                self._schedule_leave_dissemination_drain()

    def _schedule_leave_dissemination_drain(self) -> None:
        """Schedule a drain task for queued LEAVE dissemination."""
        if self._leave_dissemination_drain_scheduled:
            return

        self._leave_dissemination_drain_scheduled = True
        self._task_runner.run(
            self._drain_leave_dissemination_queue,
            alias="leave_dissemination_drain",
            keep=20,
            max_age="5m",
            keep_policy="COUNT_AND_AGE",
        )

    async def _send_coalesced_leave_dissemination(
        self,
        pending_items: list[tuple[tuple[str, int], tuple[int, bytes, bytes]]],
    ) -> None:
        """Send a bounded batch of explicit LEAVE propagation messages."""
        if not pending_items:
            return

        base_timeout = await self._context.read("current_timeout")
        gather_timeout = self.get_lhm_adjusted_timeout(base_timeout) * 2
        send_coros = []
        send_semaphore = asyncio.Semaphore(16)

        async def send_one(
            node: tuple[str, int],
            propagate_msg: bytes,
        ) -> None:
            async with send_semaphore:
                await self.send_if_ok(node, propagate_msg)

        for target, (_incarnation, target_addr_bytes, message) in pending_items:
            propagate_msg = message + b">" + target_addr_bytes
            send_coros.extend(
                send_one(node, propagate_msg)
                for node in self.get_other_nodes(target)
            )

        if send_coros:
            await self.gather_with_errors(
                send_coros,
                operation="leave_dissemination",
                timeout=gather_timeout,
            )

    def queue_join_dissemination(
        self,
        target: tuple[str, int],
        incarnation: int,
        target_addr_bytes: bytes | None,
        message: bytes,
    ) -> None:
        """Queue explicit JOIN dissemination with per-target coalescing.

        JOIN propagation used to ``await gather_with_errors`` inline in
        ``JoinHandler.handle``, holding the SWIM in-flight admission slot
        for ~2s LHM-adjusted-timeout per fan-out. With N=50 nodes and
        re-registration storms, that pegged the SWIM admission cap and
        load-shed unrelated SWIM traffic (LEAVE/probe/ack). Queuing here
        mirrors ``queue_leave_dissemination``: the handler ACKs the
        joiner after local state is durable, and propagation happens off
        the receive() task.
        """
        if target_addr_bytes is None:
            return

        existing = self._join_dissemination_queue.get(target)
        if existing is not None and existing[0] > incarnation:
            return

        self._join_dissemination_queue[target] = (
            incarnation,
            target_addr_bytes,
            message,
        )

        if self._join_dissemination_drain_scheduled:
            return

        self._join_dissemination_drain_scheduled = True
        self._task_runner.run(
            self._drain_join_dissemination_queue,
            alias="join_dissemination_drain",
            keep=20,
            max_age="5m",
            keep_policy="COUNT_AND_AGE",
        )

    async def _drain_join_dissemination_queue(self) -> None:
        """Drain coalesced JOIN dissemination work through bounded sends."""
        try:
            while self._join_dissemination_queue:
                pending_items = list(self._join_dissemination_queue.items())
                self._join_dissemination_queue.clear()
                await self._send_coalesced_join_dissemination(pending_items)
        finally:
            self._join_dissemination_drain_scheduled = False
            if self._join_dissemination_queue:
                self._schedule_join_dissemination_drain()

    def _schedule_join_dissemination_drain(self) -> None:
        """Schedule a drain task for queued JOIN dissemination."""
        if self._join_dissemination_drain_scheduled:
            return

        self._join_dissemination_drain_scheduled = True
        self._task_runner.run(
            self._drain_join_dissemination_queue,
            alias="join_dissemination_drain",
            keep=20,
            max_age="5m",
            keep_policy="COUNT_AND_AGE",
        )

    async def _send_coalesced_join_dissemination(
        self,
        pending_items: list[tuple[tuple[str, int], tuple[int, bytes, bytes]]],
    ) -> None:
        """Send a bounded batch of explicit JOIN propagation messages.

        ``message`` is the fully-formed propagate payload produced by
        ``JoinHandler._queue_join_propagation`` (``join>{ver}|{role}|...|
        i:{inc}``). The server only needs to fan it out to peers.
        """
        if not pending_items:
            return

        base_timeout = await self._context.read("current_timeout")
        gather_timeout = self.get_lhm_adjusted_timeout(base_timeout) * 2
        send_coros = []
        send_semaphore = asyncio.Semaphore(16)

        async def send_one(
            node: tuple[str, int],
            propagate_msg: bytes,
        ) -> None:
            async with send_semaphore:
                await self.send_if_ok(node, propagate_msg)

        for target, (_incarnation, _target_addr_bytes, message) in pending_items:
            send_coros.extend(
                send_one(node, message)
                for node in self.get_other_nodes(target)
            )

        if send_coros:
            await self.gather_with_errors(
                send_coros,
                operation="join_dissemination",
                timeout=gather_timeout,
            )

    def queue_suspicion_update(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Queue SUSPECT dissemination without blocking failure detection.

        Membership convergence belongs to the piggyback gossip queue.
        The only direct send we keep on the hot path's behalf is a
        managed best-effort notice to the suspected target so an alive
        peer can refute promptly.
        """
        self.queue_gossip_update("suspect", target, incarnation)
        if self._task_runner is None:
            return
        self._task_runner.run(
            self._send_direct_suspicion_notice,
            target,
            incarnation,
            alias=f"swim_suspect_notice_{target[0]}_{target[1]}_{incarnation}",
            keep=100,
            max_age="5m",
            keep_policy="COUNT_AND_AGE",
        )

    async def _send_direct_suspicion_notice(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Best-effort direct SUSPECT notification to the target."""
        if not self._running or target == self._get_self_udp_addr():
            return

        target_addr_bytes = f"{target[0]}:{target[1]}".encode()
        msg = b"suspect:" + str(incarnation).encode() + b">" + target_addr_bytes

        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        await self._send_broadcast_message(target, msg, timeout)

    def get_piggyback_data(self, max_updates: int = 5) -> bytes:
        """Get piggybacked membership updates to append to a message."""
        return self._gossip_buffer.encode_piggyback(max_updates)

    def _liveness_identity_matches(
        self,
        node: tuple[str, int],
        node_id: str | None,
    ) -> bool:
        """Return whether liveness evidence matches the current node binding."""
        registered_node_id = self._get_registered_node_id_for_addr(node)
        if registered_node_id is None:
            return True
        return node_id == registered_node_id

    def _is_locally_suspect_or_dead(self, node: tuple[str, int]) -> bool:
        """Return True when local state must not be cleared by third-party gossip."""
        node_state = self._incarnation_tracker.get_node_state(node)
        if node_state is not None and node_state.status in (b"SUSPECT", b"DEAD"):
            return True
        return self.is_node_suspected(node)

    def _should_apply_liveness_piggyback(
        self,
        update: PiggybackUpdate,
        source_addr: tuple[str, int] | None,
    ) -> bool:
        """Return whether piggybacked OK/JOIN evidence is authoritative enough.

        Third-party liveness gossip is convergence data only. Once this
        node has local SUSPECT/DEAD evidence for an address, clearing it
        requires first-party ALIVE from that address, or an explicit
        direct/rejoin path elsewhere. JOIN piggyback from another peer is
        never allowed to refute local suspicion; direct JOIN handling is
        the authoritative rejoin path.
        """
        if update.update_type not in ("alive", "join"):
            return True

        if not self._liveness_identity_matches(update.node, update.node_id):
            self._metrics.increment("gossip_liveness_identity_suppressed")
            return False

        if not self._is_locally_suspect_or_dead(update.node):
            return True

        if update.update_type != "alive":
            self._metrics.increment("gossip_liveness_refutations_suppressed")
            return False

        if source_addr != update.node:
            self._metrics.increment("gossip_liveness_refutations_suppressed")
            return False

        return True

    def _should_defer_unwitnessed_dead_piggyback(
        self,
        update: PiggybackUpdate,
    ) -> bool:
        """Return whether DEAD gossip should enter SUSPECT in no-witness mode."""
        if update.update_type != "dead":
            return False

        previous_state = self._incarnation_tracker.get_node_state(update.node)
        if previous_state is not None and previous_state.status == b"DEAD":
            return False

        return self._requires_unwitnessed_dead_confirmation(update.node)

    async def _defer_unwitnessed_dead_piggyback(
        self,
        update: PiggybackUpdate,
        source_addr: tuple[str, int] | None,
    ) -> None:
        """Treat uncorroborated DEAD gossip as suspicion in tiny clusters."""
        confirmer = source_addr or self._get_self_udp_addr()
        started = await self.start_suspicion(
            update.node,
            update.incarnation,
            confirmer,
        )
        if started:
            self.queue_suspicion_update(update.node, update.incarnation)
        self._metrics.increment("dead_gossip_deferred_unwitnessed")

    async def process_piggyback_data(
        self,
        data: bytes,
        source_addr: tuple[str, int] | None = None,
    ) -> None:
        """Process piggybacked membership updates received in a message."""
        updates = GossipBuffer.decode_piggyback(data)
        self._metrics.increment("gossip_updates_received", len(updates))
        for update in updates:
            # AD-35 Task 12.4.3: Extract and store peer role from gossip
            if update.role and hasattr(self, "_peer_roles"):
                from hyperscale.distributed.models.distributed import NodeRole

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
            status = status_map.get(update.update_type)
            if status is None:
                self._metrics.increment("gossip_unknown_updates_suppressed")
                continue

            if not self._should_apply_liveness_piggyback(update, source_addr):
                self._metrics.increment("gossip_alive_refutations_suppressed")
                continue

            if self.is_message_fresh(update.node, update.incarnation, status):
                self_addr = self._get_self_udp_addr()

                # Self-as-target gossip handling — Lifeguard §4.2 / §4.4.
                # A node receiving any negative-status gossip about
                # *itself* MUST refute, not apply. Processing
                # "I'm dead" / "I'm suspect" gossip as actual state
                # transitions is a fundamental SWIM-correctness
                # violation that produces split-brain: the alive
                # node enters its own dead-callback chain (worker
                # unregistration, leadership election rebalance,
                # …), while every probe still succeeds because the
                # node is still running. Refutation publishes a
                # higher-incarnation alive update; receiving peers
                # then clear the false suspicion via
                # ``refute_suspicion``.
                if update.node == self_addr and update.update_type in (
                    "suspect",
                    "dead",
                    "leave",
                ):
                    await self.increase_failure_detector("refutation")
                    await self.broadcast_refutation()
                    continue

                if self._should_defer_unwitnessed_dead_piggyback(update):
                    await self._defer_unwitnessed_dead_piggyback(
                        update,
                        source_addr,
                    )
                    continue

                # Check previous state BEFORE updating (for callback invocation)
                previous_state = self._incarnation_tracker.get_node_state(update.node)
                was_dead = previous_state and previous_state.status == b"DEAD"

                updated = await self.update_node_state(
                    update.node,
                    status,
                    update.incarnation,
                    update.timestamp,
                )

                if update.update_type == "suspect":
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

                    self.notify_node_dead(
                        update.node,
                        update.incarnation,
                        "gossip",
                    )

                self.queue_gossip_update(
                    update.update_type,
                    update.node,
                    update.incarnation,
                )

    def get_other_nodes(self, node: tuple[str, int]):
        target_host, target_port = node
        return [
            (host, port)
            for host, port in list(self._incarnation_tracker.node_states.keys())
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

        # ``asyncio.wait_for`` over ``asyncio.gather`` is broken: on
        # timeout, ``wait_for`` cancels the inner gather, but the
        # gather's resulting CancelledError is never retrieved (Python
        # logs ``_GatheringFuture exception was never retrieved`` from
        # the GC finaliser). ``asyncio.shield`` doesn't help — the
        # shield future itself becomes cancelled and that cancellation
        # is also unretrieved. The clean pattern is ``asyncio.wait``
        # with a timeout: it returns ``(done, pending)`` sets without
        # cancellation in the timeout path, then we explicitly cancel
        # and drain the pending set.
        if timeout:
            tasks = [asyncio.ensure_future(c) for c in coros]
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            for task in pending:
                task.cancel()
            if pending:
                # Drain CancelledError from the cancelled tasks so
                # their exceptions are retrieved.
                await asyncio.gather(*pending, return_exceptions=True)
            if pending:
                await self.handle_error(
                    NetworkError(
                        f"Gather timeout in {operation} "
                        f"({len(pending)}/{len(tasks)} tasks pending at deadline)",
                        severity=ErrorSeverity.DEGRADED,
                        operation=operation,
                    )
                )
            results = []
            timeout_err: list[Exception] = []
            for task in tasks:
                if task in done:
                    try:
                        results.append(task.result())
                    except BaseException as exc:
                        results.append(exc)
                else:
                    err = asyncio.TimeoutError(
                        f"Task in {operation} did not complete within {timeout}s"
                    )
                    results.append(err)
                    timeout_err.append(err)
            if timeout_err and not done:
                # Pure-timeout case (no task completed) — preserve the
                # legacy contract of returning an empty success list
                # plus a single sentinel TimeoutError.
                return [], [
                    asyncio.TimeoutError(f"Gather timeout in {operation}")
                ]
        else:
            results = await asyncio.gather(*coros, return_exceptions=True)

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
        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        node_state = self._incarnation_tracker.get_node_state(node)
        if node_state is None or node_state.status != b"OK":
            return False

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
        seed_role: str | None = None,
    ) -> bool:
        """
        Join a cluster via a seed node with retry support.

        Uses retry_with_backoff to handle transient failures when
        the seed node might not be ready yet.

        Args:
            seed_node: (host, port) of a node already in the cluster
            timeout: Timeout per attempt
            seed_role: Optional role of ``seed_node`` (e.g. "manager",
                "gate", "worker"). Pre-populates ``_peer_roles`` so
                downstream consumers (leader-election cohort,
                role-aware confirmation) can rely on the role being
                known *immediately* — without waiting for gossip to
                propagate it. Senders that know the seed's role from
                static configuration (manager peers, gate seeds) should
                pass it.

        Returns:
            True if join succeeded, False if all retries exhausted
        """
        from hyperscale.distributed.models.distributed import NodeRole

        self_addr = self._get_self_udp_addr()
        # Format: join>v{major}.{minor}|{role}|{host}:{port}|i:{incarnation}
        # The role field is mandatory in this protocol minor — receivers
        # parse it into _peer_roles so leader-election majority and
        # role-aware probe scheduling don't have to wait for gossip.
        # The trailing ``|i:{incarnation}`` field is the joining node's
        # current self_incarnation. Without it, a rejoining node whose
        # peers have already marked it DEAD looks like a zombie to the
        # receiver (which falls back to its stale tracker view of the
        # joiner's incarnation); the join is rejected and the recovery
        # path never fires. Including the live incarnation lets the
        # zombie check compare against the joiner's actual current
        # value. The ``i:`` prefix keeps the field self-describing
        # for receivers older than this minor that ignore unknown
        # trailing fields.
        # Version prefix lets old peers detect incompatible nodes (AD-25).
        self_role = (self._node_role or "worker").lower()
        # Bump the local self_incarnation past the receivers' rejoin
        # threshold (``death_incarnation + minimum_rejoin_incarnation_bump``).
        # A node whose peers have marked it DEAD must claim a strictly
        # higher incarnation to clear the zombie check; bumping here
        # makes ``join_cluster`` self-sufficient for rejoin without
        # forcing the caller to know whether peers consider the node
        # dead. ``bump_self_incarnation_by`` does the whole advance in
        # one lock acquisition.
        bump = self._incarnation_tracker.minimum_rejoin_incarnation_bump + 1
        await self._incarnation_tracker.bump_self_incarnation_by(bump)
        self_incarnation = self._incarnation_tracker.get_self_incarnation()
        join_msg = (
            b"join>"
            + SWIM_VERSION_PREFIX
            + b"|"
            + self_role.encode()
            + b"|"
            + f"{self_addr[0]}:{self_addr[1]}".encode()
            + b"|i:"
            + str(self_incarnation).encode()
        )

        # Pre-populate our local view of the seed's role so the very
        # first election cycle (which fires before any gossip from the
        # seed) sees a correct cohort. The seed will overwrite this if
        # its own gossip later disagrees, but for static-seed peers
        # (manager_udp_peers, gate_udp_addrs) this is authoritative.
        if seed_role:
            try:
                self._peer_roles[seed_node] = NodeRole(seed_role.lower())
            except ValueError:
                pass

        async def attempt_join() -> bool:
            await self.send(seed_node, join_msg, timeout=timeout)
            await self._incarnation_tracker.add_unconfirmed_node(seed_node)
            self._probe_scheduler.add_member(seed_node)
            return True

        result = await retry_with_result(
            attempt_join,
            policy=ELECTION_RETRY_POLICY,  # Use election policy for joining
            on_retry=lambda a, e, d: self._metrics.increment("join_retries"),
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

        # Start hierarchical failure detector (AD-30)
        await self._hierarchical_detector.start()

        # Start health monitor for proactive CPU detection
        await self.start_health_monitor()

        # Start cleanup task
        await self.start_cleanup()

        self._probe_scheduler._running = True
        self_addr = self._get_self_udp_addr()
        members = [
            node
            for node in list(self._incarnation_tracker.node_states.keys())
            if node != self_addr
        ]

        self._probe_scheduler.update_members(members)

        protocol_period = await self._context.read("udp_poll_interval", 1.0)
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

            base_timeout = await self._context.read("current_timeout")
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
                self._peer_probe_reliability.record_probe_outcome(
                    target, success=True
                )
                ctx.record_success(
                    ErrorCategory.NETWORK
                )  # Help circuit breaker recover
                self._reset_burst_failure_state()
                return

            # Per-peer probe-failure record. Unlike the LHM bump below
            # — which is gated to avoid feeding-back into our own
            # self-health signal — the per-peer tracker *must* record
            # every probe outcome to ``target``. Its purpose is exactly
            # to capture this peer's reliability over recent probes;
            # the architectural fix relies on this signal being
            # specific to ``target`` and isolated from cross-peer
            # contamination. The bracket bound ensures even an
            # all-failed window cannot push the suspicion timer past
            # ``2·base_max − base_min``.
            self._peer_probe_reliability.record_probe_outcome(
                target, success=False
            )

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
                    self._peer_probe_reliability.record_probe_outcome(
                        target, success=True
                    )
                    ctx.record_success(ErrorCategory.NETWORK)
                    self._reset_burst_failure_state()
                    return

            # Don't start suspicions during shutdown
            if not self._running:
                return

            if self._should_increment_lhm_for_failed_confirmation(target):
                await self.increase_failure_detector("probe_timeout")

            self_addr = self._get_self_udp_addr()
            await self.start_suspicion(target, incarnation, self_addr)
            self.queue_suspicion_update(target, incarnation)

            # AD-53 burst-failure detection (after start_suspicion for the
            # confirmed-dead target, so the failure window only counts
            # *actual* full direct+indirect failures, not partial ones).
            await self._record_probe_failure_and_check_burst(self_addr, target)

    def _reset_burst_failure_state(self) -> None:
        """Clear the burst-failure observation window on any probe success.

        An ordinary probe-cycle success ends the current consecutive
        full-failure run. Accelerated candidate successes do not call
        this for the whole batch; they refute only their own target.
        """
        if self._burst_failure_observations:
            self._burst_failure_observations.clear()
        if self._burst_failure_run is None or not self._burst_failure_run.task_running:
            self._burst_failure_active = False

    async def _record_probe_failure_and_check_burst(
        self,
        self_addr: tuple[str, int],
        failed_target: tuple[str, int],
    ) -> None:
        """Record a probe failure and, on threshold, accelerate confirmation.

        AD-53. Appends ``(now, failed_target)`` to the observation
        window, evicts entries older than
        ``BURST_FAILURE_WINDOW_SECONDS``, and if the distinct failed
        target count crosses ``BURST_FAILURE_THRESHOLD`` while no
        confirmation batch is already running, triggers bounded
        parallel confirmation for other registered members. A later
        ordinary probe success clears the observation window; candidate
        successes refute only their own candidate.
        """
        now = time.monotonic()
        observations = self._burst_failure_observations
        observations.append((now, failed_target))
        cutoff = now - self._burst_failure_window_seconds
        while observations and observations[0][0] < cutoff:
            observations.popleft()

        if self._burst_failure_active:
            return

        distinct_failed_targets = {target for _, target in observations}
        if len(distinct_failed_targets) < self._burst_failure_threshold:
            return

        self._burst_failure_active = True
        if self._task_runner is None:
            await self._run_burst_failure_confirmation(self_addr)
            return

        run = self._task_runner.run(
            self._run_burst_failure_confirmation,
            self_addr,
            alias="ad53_burst_failure_confirmation",
            keep=10,
            max_age="5m",
            keep_policy="COUNT_AND_AGE",
        )
        if run is None:
            self._burst_failure_active = False
            return
        self._burst_failure_run = run

    async def _run_burst_failure_confirmation(
        self,
        self_addr: tuple[str, int],
    ) -> None:
        """Run one managed AD-53 burst-confirmation batch."""
        try:
            await self._accelerate_burst_failure_confirmation(self_addr)
        finally:
            self._burst_failure_active = False
            self._burst_failure_run = None

    async def _cancel_burst_failure_run(self) -> None:
        """Cancel the in-flight managed burst-confirmation batch, if any."""
        run = self._burst_failure_run
        self._burst_failure_run = None
        self._burst_failure_active = False
        if run is not None and run.task_running:
            await run.cancel()

    async def _accelerate_burst_failure_confirmation(
        self,
        self_addr: tuple[str, int],
    ) -> None:
        """Probe silent members concurrently while preserving SWIM semantics.

        AD-53. The probe-walk is serial; at large ``N`` it cannot
        individually visit every dead peer within Phase-3 reap budgets.
        Burst mode temporarily widens confirmation work for registered
        members that are not already terminal. Each target still uses
        the normal direct probe -> indirect probe -> SUSPECT flow, so
        DEAD remains owned by the hierarchical suspicion timer and the
        canonical ``_on_suspicion_expired`` callback.

        Gates:

        * Skip targets already in a terminal state (SUSPECT/DEAD/UNCONFIRMED).
        * Skip targets that did not complete an explicit registration
          handshake; ``start_suspicion`` would reject them anyway.
        * Skip ``self_addr`` (cannot declare the prober dead).

        Prior successes do not skip a candidate. In rolling scale-down a
        worker can be reachable at the first burst failure and gone a few
        seconds later; only this candidate's fresh confirmation attempt can
        refute suspicion for this candidate.
        """
        candidates = self._get_burst_confirmation_candidates(self_addr)
        if not candidates:
            return

        semaphore = asyncio.Semaphore(
            min(self._burst_failure_probe_concurrency, len(candidates))
        )

        async def confirm_candidate(candidate: tuple[str, int]) -> bool:
            async with semaphore:
                try:
                    if not self._running:
                        return True
                    return await self._confirm_burst_failure_candidate(
                        candidate,
                        self_addr,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await self.handle_exception(
                        error,
                        f"ad53_burst_confirmation_{candidate[0]}_{candidate[1]}",
                    )
                    return False

        confirmation_results = await asyncio.gather(
            *(confirm_candidate(candidate) for candidate in candidates)
        )

        if all(confirmation_results):
            self._reset_burst_failure_state()

    def _get_burst_confirmation_candidates(
        self,
        self_addr: tuple[str, int],
    ) -> list[tuple[str, int]]:
        """Return members eligible for AD-53 accelerated confirmation."""
        candidates: list[tuple[str, int]] = []
        for member in list(self._probe_scheduler.members):
            if member == self_addr:
                continue
            if not self.is_peer_registered(member) or not self.is_peer_confirmed(member):
                continue
            if self._is_target_already_suspect_or_dead(member):
                continue
            candidates.append(member)
        return candidates

    async def _confirm_burst_failure_candidate(
        self,
        target: tuple[str, int],
        self_addr: tuple[str, int],
    ) -> bool:
        """Run one normal SWIM confirmation for an AD-53 burst candidate."""
        if not self._running or self._is_target_already_suspect_or_dead(target):
            return False

        node_state = self._incarnation_tracker.get_node_state(target)
        incarnation = node_state.incarnation if node_state else 0
        confirmation_started_at = time.monotonic()

        confirmed_alive = await self._confirm_peer_reachable_by_swim(
            target,
            incarnation,
        )
        if confirmed_alive:
            return True

        if self._peer_probe_reliability.had_success_since(
            target,
            confirmation_started_at,
        ):
            return True

        if not self._running or self._is_target_already_suspect_or_dead(target):
            return False

        await self.start_suspicion(target, incarnation, self_addr)
        self.queue_suspicion_update(target, incarnation)
        return False

    async def _confirm_peer_reachable_by_swim(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """Run direct and indirect SWIM confirmation for ``target``.

        This helper intentionally stops before suspicion. Callers that
        need a membership transition must decide how to interpret the
        failed confirmation in their own layer, while positive results
        consistently update probe reliability and LHM just like the
        ordinary probe loop.
        """
        if not self._running:
            return False

        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        target_addr = f"{target[0]}:{target[1]}".encode()

        response_received = await self._probe_with_timeout(
            target,
            b"probe>" + target_addr,
            timeout,
        )
        if response_received:
            await self.decrease_failure_detector("successful_probe")
            self._peer_probe_reliability.record_probe_outcome(
                target,
                success=True,
            )
            return True

        self._peer_probe_reliability.record_probe_outcome(target, success=False)

        indirect_sent = await self.initiate_indirect_probe(target, incarnation)
        if indirect_sent:
            await asyncio.sleep(timeout)
            if not self._running:
                return False

            probe = self._indirect_probe_manager.get_pending_probe(target)
            if probe and probe.is_completed():
                await self.decrease_failure_detector("successful_probe")
                self._peer_probe_reliability.record_probe_outcome(
                    target,
                    success=True,
                )
                return True

        return False

    def _compute_direct_probe_budget(
        self,
        target: tuple[str, int],
        base_timeout: float,
    ) -> float:
        """Continuous time budget for the direct-probe phase, derived from
        cluster health and per-peer behaviour.

        Lifeguard prescribes a *single* direct probe with an LHM-stretched
        timeout, then K indirect probes via random proxies, then suspicion.
        That maps cleanly to the "all signals healthy" case here:
        ``budget = base_timeout``. Under noise — peer overload, small
        cluster (less indirect coverage), reliable peer with a transient
        miss — we extend the budget by a bounded continuous factor so
        retries can fire within the same probe round. Under self-overload
        (high LHM) the budget shrinks again because each LHM-stretched
        attempt already buys patience and retries would double-pay.

        Composition follows the same prob-OR pattern used in the
        suspicion-bracket and probe-timeout layers:

        * ``peer_load_noise``  — reported peer load normalised against
          its configured saturation (PHA's ``timeout_multiplier_overloaded``).
        * ``cluster_pressure`` — ``1 / log2(n_members)``: small clusters
          have less indirect-probe redundancy and warrant more direct
          retries; large clusters have abundant proxies and don't.
        * ``peer_reliability`` — sliding-window probe-success rate to
          this peer. Multiplicative modulator: only retry peers with a
          good track record. A peer whose recent probes have all failed
          is dying, not noisy — extra retries waste detection time.
        * ``inhibition``       — ``1 / lhm_multiplier``. High self-LHM
          inhibits retries because each attempt is already stretched.

        Bounded by construction: ``warrant``, ``inhibition``, and
        ``peer_reliability`` are all in ``[0, 1]``; ``max_extra`` is
        ``lhm_max_multiplier − 1`` (default 2.0, derived from the same
        Lifeguard saturation cap that bounds the suspicion bracket and
        probe-timeout layers). Total budget is therefore strictly in
        ``[base_timeout, base_timeout × lhm_max_multiplier]`` regardless
        of input magnitudes — explosion is impossible at any cluster
        scale.

        Continuous in every input — small signal change → small budget
        change, never a step. Per-round attempt counts vary smoothly
        with conditions, eliminating the integer-boundary flap that
        afflicts a discrete attempts-count formulation.
        """
        target_node_id = f"{target[0]}:{target[1]}"

        peer_load_multiplier = max(
            1.0,
            self._peer_health_awareness.get_load_multiplier(target_node_id),
        )
        peer_reliability = self._peer_probe_reliability.get_reliability(target)
        n_members = max(2, self._get_member_count())
        lhm_multiplier = max(1.0, self._local_health.get_multiplier())

        peer_load_max = (
            self._peer_health_awareness.config.timeout_multiplier_overloaded
        )
        if peer_load_max > 1.0:
            peer_load_noise = (peer_load_multiplier - 1.0) / (
                peer_load_max - 1.0
            )
        else:
            peer_load_noise = 0.0
        peer_load_noise = min(1.0, max(0.0, peer_load_noise))

        cluster_pressure = 1.0 / math.log2(n_members)

        combined_noise = 1.0 - (1.0 - peer_load_noise) * (1.0 - cluster_pressure)
        warrant = combined_noise * peer_reliability

        inhibition = 1.0 / lhm_multiplier

        # ``max_extra`` is derived from LHM saturation, the same
        # ceiling used by the suspicion-bracket and probe-timeout
        # layers. Coherent across the architecture: any signal-driven
        # extension stays inside the Lifeguard saturation envelope.
        # With default config (``LHM.max_score=8``,
        # ``MULTIPLIER_WEIGHT=0.25``) this is ``2.0``, making the
        # direct-probe budget bounded in ``[base_timeout,
        # 3·base_timeout]`` regardless of input magnitudes — strictly
        # bounded; explosion is impossible at any cluster scale.
        max_extra = self._local_health.get_max_multiplier() - 1.0

        return base_timeout * (1.0 + max_extra * warrant * inhibition)

    async def _probe_with_timeout(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float,
    ) -> bool:
        """Direct-probe phase under a continuous adaptive deadline.

        The direct-probe budget is computed once at round start (see
        ``_compute_direct_probe_budget``) and consumed via successive
        send/wait_for cycles until either an ACK arrives or the
        deadline elapses. Each iteration sends one probe and waits up
        to ``timeout`` (the LHM-adjusted per-attempt timeout) for the
        ACK; the trailing iteration's wait is clamped to whatever budget
        remains. There are no magic retry counts, no per-attempt
        timeout fractions, no exponential-backoff sleeps between
        attempts — the four signals fed into the budget already encode
        every operator-meaningful pacing decision.

        Future-based ACK tracking (``_pending_probe_acks``) ensures we
        wait for the actual ACK message arrival, not stale cached node
        state.
        """
        self._metrics.increment("probes_sent")

        if not self._running:
            return False

        budget = self._compute_direct_probe_budget(target, timeout)
        deadline = time.monotonic() + budget

        while True:
            if not self._running:
                return False

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            try:
                # Cancel any stale pending probe to the same target, then
                # install a fresh future for this attempt.
                existing_future = self._pending_probe_acks.pop(target, None)
                if existing_future and not existing_future.done():
                    existing_future.cancel()

                ack_future: asyncio.Future[bool] = (
                    asyncio.get_event_loop().create_future()
                )
                self._pending_probe_acks[target] = ack_future
                self._pending_probe_start[target] = time.monotonic()

                await self.send(target, message, timeout=timeout)

                attempt_window = min(timeout, deadline - time.monotonic())
                if attempt_window <= 0:
                    break

                try:
                    await asyncio.wait_for(ack_future, timeout=attempt_window)
                    self._metrics.increment("probes_received")
                    return True
                except asyncio.TimeoutError:
                    pass
                finally:
                    self._pending_probe_acks.pop(target, None)
                    self._pending_probe_start.pop(target, None)

            except asyncio.CancelledError:
                self._pending_probe_acks.pop(target, None)
                self._pending_probe_start.pop(target, None)
                raise
            except OSError as e:
                self._pending_probe_acks.pop(target, None)
                self._pending_probe_start.pop(target, None)
                self._metrics.increment("probes_failed")
                await self.handle_error(
                    self._make_network_error(e, target, "Probe")
                )
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
        self_addr = self._get_self_udp_addr()
        members = []
        for node, node_state in self._incarnation_tracker.node_states.items():
            if node == self_addr:
                continue
            # Exclude DEAD nodes from probe scheduling
            if node_state.status == b"DEAD":
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
                await self._broadcast_leave()
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

        await self._cancel_burst_failure_run()

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

    def _get_additional_leave_targets(self) -> list[tuple[str, int]]:
        """Return role-specific UDP targets that must receive graceful leave."""
        return []

    def _get_leave_targets(self) -> list[tuple[str, int]]:
        """Return a deduplicated snapshot of UDP peers to notify on leave."""
        self_addr = self._get_self_udp_addr()
        targets: dict[tuple[str, int], None] = {}

        for node in self._incarnation_tracker.node_states.keys():
            if node != self_addr:
                targets[node] = None

        for node in self._get_additional_leave_targets():
            if node != self_addr and node[0] and node[1]:
                targets[node] = None

        return list(targets.keys())

    async def _broadcast_leave(self) -> None:
        """Best-effort broadcast of this node's SWIM leave message."""
        self_addr = self._get_self_udp_addr()
        incarnation = await self._prepare_leave_incarnation()
        leave_msg = (
            f"leave:{incarnation}:{self._node_id.full}>"
            f"{self_addr[0]}:{self_addr[1]}"
        ).encode()
        timeout = self.get_lhm_adjusted_timeout(1.0)

        node_addresses = self._get_leave_targets()
        await self._udp_logger.log(
            ServerError(
                message=(
                    f"[BCAST-ENTER] self={self_addr} "
                    f"targets={node_addresses} "
                    f"node_id={self._node_id.short}"
                ),
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            )
        )
        if not node_addresses:
            return

        concurrency = min(64, len(node_addresses))
        send_semaphore = asyncio.Semaphore(concurrency)
        max_attempts = 2

        async def send_leave(node: tuple[str, int]) -> bool:
            async with send_semaphore:
                last_failure: object = None
                for _attempt_number in range(1, max_attempts + 1):
                    try:
                        send_result = await self.send(
                            node,
                            leave_msg,
                            timeout=timeout,
                        )
                    except Exception as error:
                        last_failure = error
                        await self._udp_logger.log(
                            ServerError(
                                message=(
                                    f"[BCAST-SEND-EXC] self={self_addr} "
                                    f"-> node={node} attempt={_attempt_number} "
                                    f"err={type(error).__name__}:{error}"
                                ),
                                node_host=self._host,
                                node_port=self._udp_port,
                                node_id=self._node_id.short,
                            )
                        )
                        continue

                    response = (
                        send_result[0]
                        if isinstance(send_result, tuple)
                        else send_result
                    )
                    await self._udp_logger.log(
                        ServerError(
                            message=(
                                f"[BCAST-SEND-RSP] self={self_addr} "
                                f"-> node={node} attempt={_attempt_number} "
                                f"response={response!r:.80}"
                            ),
                            node_host=self._host,
                            node_port=self._udp_port,
                            node_id=self._node_id.short,
                        )
                    )
                    if isinstance(response, bytes) and response.startswith(
                        (b"ack", b"leave")
                    ):
                        return True
                    last_failure = response

                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            f"Leave broadcast to {node[0]}:{node[1]} failed "
                            f"after {max_attempts} attempts: "
                            f"{type(last_failure).__name__}"
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=self._node_id.short,
                    )
                )
                return False

        results = await asyncio.gather(
            *(send_leave(node) for node in node_addresses),
            return_exceptions=False,
        )
        send_failures = sum(1 for result in results if not result)

        if send_failures > 0:
            await self._udp_logger.log(
                ServerDebug(
                    message=(
                        f"Leave broadcast: {send_failures}/{len(node_addresses)} "
                        "sends failed"
                    ),
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                )
            )

    async def _prepare_leave_incarnation(self) -> int:
        """Advance self incarnation so direct leave wins receiver freshness checks."""
        return await self._incarnation_tracker.increment_self_incarnation()

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
        """Increase local health score based on event type.

        Per Lifeguard §4.3 LHM is bumped only on documented self-
        health events. The Hyperscale ``architecture.md`` extension
        adds ``event_loop_lag`` and ``event_loop_critical`` (proactive
        signals from the local event-loop monitor). All other inputs
        are *not* self-health events — protocol retries (election,
        join, send) reflect peer/network state, not prober slowness.
        Routing them through LHM would conflate operational retry
        traffic with self-health and inflate probe timeouts and
        suspicion brackets cluster-wide during normal startup churn.

        Unknown event types are surfaced as a warning rather than
        silently bumping LHM, so future callers cannot regress this
        invariant.
        """
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
            if self._task_runner and self._udp_logger:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=(
                            f"increase_failure_detector called with unrecognised "
                            f"event_type={event_type!r} — LHM not bumped. Per "
                            f"Lifeguard §4.3, LHM tracks self-health events only "
                            f"(probe_timeout/refutation/missed_nack/event_loop_lag/"
                            f"event_loop_critical). Operational retry telemetry "
                            f"belongs in metrics."
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=(
                            self._node_id.short
                            if hasattr(self, "_node_id")
                            else 0
                        ),
                    ),
                )

    async def decrease_failure_detector(self, event_type: str = "successful_probe"):
        """Decrease local health score based on event type.

        Symmetric to ``increase_failure_detector`` — only Lifeguard-
        documented self-health recovery events shrink LHM. Unknown
        event types surface a warning rather than silently
        decrementing.
        """
        if event_type == "successful_probe":
            self._local_health.on_successful_probe()
        elif event_type == "successful_nack":
            self._local_health.on_successful_nack()
        elif event_type == "event_loop_recovered":
            self._local_health.on_event_loop_recovered()
        else:
            if self._task_runner and self._udp_logger:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerWarning(
                        message=(
                            f"decrease_failure_detector called with unrecognised "
                            f"event_type={event_type!r} — LHM not decremented. "
                            f"See ``increase_failure_detector`` docstring."
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=(
                            self._node_id.short
                            if hasattr(self, "_node_id")
                            else 0
                        ),
                    ),
                )

    def get_lhm_adjusted_timeout(
        self, base_timeout: float, target_node_id: str | None = None
    ) -> float:
        """Adjust ``base_timeout`` via two-stage bounded composition.

        **Stage 1 — per-peer base RTT scaling (real network distance).**
        Vivaldi's ``latency_multiplier`` is the geometric estimate of
        round-trip time relative to a 10 ms same-DC reference. This is
        a *real* cost — a cross-continent peer's ack physically takes
        longer to arrive — so it scales the base timeout linearly,
        outside the uncertainty-padding stage.

        **Stage 2 — bounded uncertainty padding via prob-OR.** Four
        independent measurement-reliability signals collapse to a
        single bounded padding factor:

        * ``self_lhm``           — global self-health (LHM)
        * ``degradation``        — global graceful-degradation level
        * ``coord_quality``      — per-peer Vivaldi confidence
        * ``peer_load``          — per-peer reported load class

        Each multiplier ``m_i ≥ 1`` becomes a reliability
        ``r_i = 1 / m_i ∈ (0, 1]``. Independent reliabilities compose
        multiplicatively: ``R = ∏ r_i``. Unreliability is
        ``U = 1 − R ∈ [0, 1)``. Padding scales linearly within the
        cap derived from LHM saturation:

            timeout = peer_base × (1 + (lhm_max_multiplier − 1) × U)

        With default config (``LHM.max_score=8``,
        ``MULTIPLIER_WEIGHT=0.25``) the cap is ``3 × peer_base`` —
        matching the existing LHM-saturated bound — and remains there
        even if every signal is simultaneously at its individual
        worst-case. The previous code multiplied every signal
        ``base × lhm × degradation × latency × confidence × peer_load``
        which (e.g.) at all-saturated produced ``> 90×`` blow-ups.

        Why drop ``peer_health_awareness.get_probe_timeout`` here?
        Because that helper just multiplied ``peer_load`` on top of
        ``base_adjusted`` — which is exactly the multiplicative
        compounding this rewrite eliminates. ``peer_load_multiplier``
        is now folded into the prob-OR composition where it belongs,
        with the rest of the per-peer reliability inputs.

        Args:
            base_timeout: Base probe timeout in seconds.
            target_node_id: Optional probe target. When supplied,
                per-peer Vivaldi (RTT scaling, coord quality) and PHA
                (peer load) signals participate; otherwise only the
                two global signals (LHM, degradation) do.

        Returns:
            Adjusted timeout in seconds, strictly bounded by
            ``base_timeout × latency_multiplier × lhm_max_multiplier``.
        """
        latency_multiplier = 1.0
        coord_quality_multiplier = 1.0
        if target_node_id:
            peer_coord = self._coordinate_tracker.get_peer_coordinate(target_node_id)
            if peer_coord is not None:
                estimated_rtt_ms = self._coordinate_tracker.estimate_rtt_ucb_ms(
                    peer_coordinate=peer_coord
                )
                reference_rtt_ms = 10.0  # Same-datacenter baseline (10ms)
                latency_multiplier = min(
                    10.0, max(1.0, estimated_rtt_ms / reference_rtt_ms)
                )
                # Vivaldi coord quality ∈ [0, 1]; convert to a
                # multiplier ≥ 1 the same way the previous formula did
                # (``1 + (1 − quality) × 0.5``) so saturation gives a
                # 1.5× factor — preserves the existing per-peer
                # reliability semantic with the new composition.
                quality = self._coordinate_tracker.coordinate_quality(peer_coord)
                coord_quality_multiplier = 1.0 + (1.0 - quality) * 0.5

        peer_base = base_timeout * latency_multiplier

        peer_load_multiplier = 1.0
        if target_node_id:
            peer_load_multiplier = self._peer_health_awareness.get_load_multiplier(
                target_node_id
            )

        lhm_multiplier = max(1.0, self._local_health.get_multiplier())
        degradation_multiplier = max(
            1.0, self._degradation.get_timeout_multiplier()
        )
        coord_quality_multiplier = max(1.0, coord_quality_multiplier)
        peer_load_multiplier = max(1.0, peer_load_multiplier)

        combined_reliability = (
            (1.0 / lhm_multiplier)
            * (1.0 / degradation_multiplier)
            * (1.0 / coord_quality_multiplier)
            * (1.0 / peer_load_multiplier)
        )
        combined_unreliability = 1.0 - combined_reliability

        max_padding_factor = self._local_health.get_max_multiplier() - 1.0

        return peer_base * (1.0 + max_padding_factor * combined_unreliability)

    def get_self_incarnation(self) -> int:
        """Get this node's current incarnation number."""
        return self._incarnation_tracker.get_self_incarnation()

    async def increment_incarnation(self) -> int:
        """Increment and return this node's incarnation number (for refutation)."""
        new_incarnation = await self._incarnation_tracker.increment_self_incarnation()
        await self.persist_incarnation(new_incarnation)
        return new_incarnation

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

        msg_parts = msg_part.split(b":", maxsplit=2)
        msg_type = msg_parts[0]
        incarnation = int(msg_parts[1].decode()) if len(msg_parts) > 1 else 0

        return msg_type, incarnation, target

    def _parse_node_id_from_message(self, message: bytes) -> str | None:
        """Parse optional stable node identity from ``type:incarnation:node_id``."""
        msg_part = message.split(b">", maxsplit=1)[0]
        msg_parts = msg_part.split(b":", maxsplit=2)
        if len(msg_parts) < 3:
            return None
        try:
            node_id = msg_parts[2].decode()
        except UnicodeDecodeError:
            return None
        return node_id or None

    async def _parse_incarnation_safe(
        self,
        message: bytes,
        source: tuple[str, int],
    ) -> int:
        """
        Parse incarnation number from message safely.

        Returns 0 on parse failure but logs the error for monitoring.
        """
        msg_parts = message.split(b">", maxsplit=1)[0].split(b":", maxsplit=2)
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

    def is_authoritative_liveness_evidence(
        self,
        source_addr: tuple[str, int],
        target: tuple[str, int] | None,
        node_id: str | None,
    ) -> bool:
        """Return whether an ALIVE message can refute local suspicion.

        ALIVE is first-party evidence: the source address must be the
        subject address. When this receiver has a registered identity for
        that address, the message must carry the same identity so stale
        predecessor traffic cannot clear suspicion for a fresh process.
        """
        if target is None or source_addr != target:
            return False
        return self._liveness_identity_matches(target, node_id)

    async def _process_direct_alive_response(
        self,
        source_addr: tuple[str, int],
        data: bytes,
    ) -> None:
        """Apply a direct ``alive:`` probe response after identity fencing."""
        try:
            message_type, incarnation, target = self.decode_message_with_incarnation(
                data
            )
        except (ValueError, UnicodeDecodeError):
            return

        if message_type != b"alive":
            return

        node_id = self._parse_node_id_from_message(data)
        if not self.is_authoritative_liveness_evidence(
            source_addr,
            target,
            node_id,
        ):
            self._metrics.increment("non_authoritative_alive_suppressed")
            return

        pending_future = self._pending_probe_acks.get(source_addr)
        if pending_future and not pending_future.done():
            pending_future.set_result(True)

        if not target:
            return

        node_state = self._incarnation_tracker.get_node_state(target)
        if (
            node_state is not None
            and node_state.status == b"SUSPECT"
            and incarnation >= node_state.incarnation
        ):
            await self._clear_unwitnessed_suspicion_after_confirmation(
                target,
                incarnation,
            )
            return

        if self.is_message_fresh(target, incarnation, b"OK"):
            await self.refute_suspicion(target, incarnation)
            await self.update_node_state(
                target,
                b"OK",
                incarnation,
                time.monotonic(),
            )

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

    async def _check_rate_limit(
        self,
        addr: tuple[str, int],
        admission_class: SwimAdmissionClass | Literal["auxiliary"] = "unknown",
        *,
        log_rejection: bool = True,
    ) -> bool:
        """
        Check if a sender is within the class-aware SWIM token bucket.

        Each sender and SWIM admission class has a token bucket that refills over time.
        If bucket is empty, message is rejected.

        Returns True if allowed, False if rate limited.
        """
        now = time.monotonic()
        bucket_key = (addr[0], addr[1], admission_class)
        bucket_capacity, refill_rate = self._swim_rate_limit_profiles.get(
            admission_class,
            self._swim_rate_limit_profiles["unknown"],
        )
        class_stats = self._swim_rate_limit_stats.setdefault(
            admission_class,
            {"accepted": 0, "rejected": 0},
        )

        if bucket_key not in self._rate_limits:
            # New sender - initialize bucket
            self._rate_limits[bucket_key] = {
                "tokens": bucket_capacity,
                "last_refill": now,
            }

        bucket = self._rate_limits[bucket_key]

        # Refill tokens based on elapsed time
        elapsed = now - bucket["last_refill"]
        refill = int(elapsed * refill_rate)
        if refill > 0:
            bucket["tokens"] = min(
                bucket["tokens"] + refill,
                bucket_capacity,
            )
            bucket["last_refill"] = now

        # Check if we have tokens
        if bucket["tokens"] > 0:
            bucket["tokens"] -= 1
            self._rate_limit_stats["accepted"] += 1
            class_stats["accepted"] += 1
            return True
        else:
            self._rate_limit_stats["rejected"] += 1
            class_stats["rejected"] += 1
            self._metrics.increment("messages_rate_limited")
            # Log rate limit violation
            if log_rejection:
                await self.handle_error(
                    ResourceError(
                        f"SWIM {admission_class} rate limit exceeded for "
                        f"{addr[0]}:{addr[1]}",
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
            "tracked_buckets": len(self._rate_limits),
            "tokens_per_sender": self._rate_limit_tokens,
            "refill_rate": self._rate_limit_refill,
            "classes": {
                admission_class: dict(class_stats)
                for admission_class, class_stats in self._swim_rate_limit_stats.items()
            },
        }

    def get_metrics(self) -> dict:
        """Get all protocol metrics for monitoring."""
        return self._metrics.to_dict()

    def _classify_swim_admission(
        self,
        source_addr: tuple[str, int],
        data: bytes,
    ) -> SwimAdmissionClass:
        """Classify an inbound SWIM payload for class-aware rate limiting."""
        registered_node_id = self._get_registered_node_id_for_addr(source_addr)
        return classify_swim_payload(
            source_addr,
            data,
            registered_node_id=registered_node_id,
        )

    async def _should_process_auxiliary_piggyback(
        self,
        source_addr: tuple[str, int],
        data: bytes,
    ) -> bool:
        """Return whether auxiliary piggyback work is admitted for this packet."""
        if not has_auxiliary_piggyback(data):
            return True

        return await self._check_rate_limit(
            source_addr,
            "auxiliary",
            log_rejection=False,
        )

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

    async def update_node_state(
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

        # Perform the actual update
        updated = await self._incarnation_tracker.update_node(
            node, status, incarnation, timestamp
        )

        if updated and status == b"DEAD":
            import traceback as _tb
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"[NODE-DEAD] node={node} incarnation={incarnation} "
                        f"prev_status={previous_state.status if previous_state else None} "
                        f"stack={'/'.join(f.name for f in _tb.extract_stack()[-8:-1])}"
                    ),
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                )
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
    ) -> bool | None:
        """
        Start suspecting a node or add confirmation to existing suspicion.

        Per AD-29: Only confirmed peers can be suspected. If we've never
        successfully communicated with a peer, we can't meaningfully suspect
        them - they might just not be up yet during cluster formation.

        AD-29 Task 12.3.4: UNCONFIRMED → SUSPECT transitions are explicitly
        prevented by the formal state machine.
        """
        # Registration gate: a peer must have completed an explicit
        # registration handshake (TCP register endpoint or SWIM JOIN)
        # before this node may suspect them. Passive observation —
        # for instance an inbound probe from a peer we've never been
        # introduced to — does not promote them into a suspectable
        # state. This eliminates the "boot-time false positive": a
        # peer that's still completing its startup handshake cannot
        # be SUSPECTed by transient probe-timeouts.
        if not self.is_peer_registered(node):
            self._metrics.increment("suspicions_skipped_unregistered")
            return None

        # AD-29: Guard against suspecting unconfirmed peers
        # Use formal state machine check which prevents UNCONFIRMED → SUSPECT
        if not self._incarnation_tracker.can_suspect_node(node):
            self._metrics.increment("suspicions_skipped_unconfirmed")
            return None

        now = time.monotonic()
        self._metrics.increment("suspicions_started")
        self._audit_log.record(
            AuditEventType.NODE_SUSPECTED,
            node=node,
            from_node=from_node,
            incarnation=incarnation,
        )
        await self._incarnation_tracker.update_node(
            node,
            b"SUSPECT",
            incarnation,
            now,
        )
        result = await self._hierarchical_detector.suspect_global(
            node, incarnation, from_node
        )
        if result:
            self._global_suspicion_started_at.setdefault(node, now)
            witness_count, rejection_reasons = self._proxy_eligibility_breakdown(
                node
            )
            suspicion_state = (
                await self._hierarchical_detector._global_wheel.get_state(node)
                if self._hierarchical_detector is not None
                else None
            )
            min_timeout = (
                suspicion_state.min_timeout if suspicion_state is not None else None
            )
            max_timeout = (
                suspicion_state.max_timeout if suspicion_state is not None else None
            )
            mapped_worker_id = self._get_registered_node_id_for_addr(node)
            degradation_level = (
                self._degradation.current_level.name
                if self._degradation is not None
                else None
            )
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"[SUSPICION-START] target={node} "
                        f"worker_id={mapped_worker_id} incarnation={incarnation} "
                        f"witnesses={witness_count} "
                        f"rejected={rejection_reasons} "
                        f"lhm_score={self._local_health.score} "
                        f"lhm_multiplier={self._local_health.get_multiplier():.2f} "
                        f"degradation={degradation_level} "
                        f"min_timeout={min_timeout} max_timeout={max_timeout}"
                    ),
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                )
            )
        return result

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
            await self._incarnation_tracker.update_node(
                node,
                b"OK",
                incarnation,
                time.monotonic(),
            )
            self._global_suspicion_started_at.pop(node, None)
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
        self_addr = self._get_self_udp_addr()

        all_candidates = [
            node
            for node in self._incarnation_tracker.node_states.keys()
            if (
                node != target
                and node != self_addr
                and self._is_valid_indirect_probe_proxy(node)
            )
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

    def _is_valid_indirect_probe_proxy(self, node: tuple[str, int]) -> bool:
        """Return True when ``node`` can add useful indirect-probe evidence."""
        if not self.is_peer_registered(node) or not self.is_peer_confirmed(node):
            return False

        node_state = self._incarnation_tracker.get_node_state(node)
        if node_state is None:
            return False
        return node_state.status in (b"OK", b"JOIN")

    def _log_suspicion_expiration_diagnostic(
        self,
        node: tuple[str, int],
        actual_age_seconds: float,
        expected_timeout: float,
        incarnation: int,
        required_confirmations: int,
        min_timeout: float,
        max_timeout: float,
    ) -> None:
        """Sync callback from HFD wheel; route logging via TaskRunner."""
        started_at = self._global_suspicion_started_at.get(node)
        wall_age = (
            time.monotonic() - started_at if started_at is not None else None
        )
        target_state = self._incarnation_tracker.get_node_state(node)
        target_status = target_state.status if target_state is not None else None
        self._task_runner.run(
            self._udp_logger.log,
            ServerError(
                message=(
                    f"[SUSPICION-EXPIRY] target={node} incarnation={incarnation} "
                    f"actual_age_s={actual_age_seconds:.2f} "
                    f"expected_timeout_s={expected_timeout:.2f} "
                    f"min_timeout_s={min_timeout:.2f} max_timeout_s={max_timeout:.2f} "
                    f"required_confirmations={required_confirmations} "
                    f"wall_age_since_setdefault_s={wall_age} "
                    f"target_status={target_status} "
                    f"lhm_score={self._local_health.score} "
                    f"degradation={self._degradation.current_level.name}"
                ),
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            ),
            alias="suspicion_expiry_diag",
        )

    def _proxy_eligibility_breakdown(
        self,
        target: tuple[str, int],
    ) -> tuple[int, dict[str, int]]:
        """Return (witness_count, {rejection_reason: count}) for ``target``.

        Iterates every non-self, non-target node in the incarnation tracker
        and records exactly why each one is or isn't a valid indirect-probe
        proxy. Used by the suspicion-start diagnostic so we can see whether
        the cluster entered no-witness mode because witnesses *don't exist*
        or because they were transiently disqualified.
        """
        self_addr = self._get_self_udp_addr()
        eligible: list[tuple[str, int]] = []
        reasons: dict[str, int] = {}
        for node in self._incarnation_tracker.node_states.keys():
            if node == target or node == self_addr:
                continue
            if not self.is_peer_registered(node):
                reasons["unregistered"] = reasons.get("unregistered", 0) + 1
                continue
            if not self.is_peer_confirmed(node):
                reasons["unconfirmed"] = reasons.get("unconfirmed", 0) + 1
                continue
            node_state = self._incarnation_tracker.get_node_state(node)
            if node_state is None:
                reasons["no_state"] = reasons.get("no_state", 0) + 1
                continue
            status = node_state.status
            if status not in (b"OK", b"JOIN"):
                key = f"status={status.decode(errors='replace')}"
                reasons[key] = reasons.get(key, 0) + 1
                continue
            eligible.append(node)
        return len(eligible), reasons

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

        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        request_id = self._build_indirect_probe_request_id()

        probe = self._indirect_probe_manager.start_indirect_probe(
            target=target,
            requester=self._get_self_udp_addr(),
            request_id=request_id,
            timeout=timeout,
        )
        if probe is None:
            return False
        self._metrics.increment("indirect_probes_sent")

        target_addr = f"{target[0]}:{target[1]}".encode()
        msg = (
            b"ping-req:"
            + str(incarnation).encode()
            + b":"
            + request_id.encode()
            + b">"
            + target_addr
        )

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
        request_id: str | None = None,
    ) -> None:
        """Handle response from an indirect probe."""
        if is_alive:
            if self._indirect_probe_manager.record_ack(target, request_id):
                await self.decrease_failure_detector("successful_probe")
                self._peer_probe_reliability.record_probe_outcome(
                    target, success=True
                )

    def _build_indirect_probe_request_id(self) -> str:
        """Build a request token for fencing indirect-probe responses."""
        return (
            f"{self._node_id.short}-{time.monotonic_ns()}-"
            f"{random.getrandbits(32):08x}"
        )

    async def broadcast_refutation(self) -> int:
        """
        Broadcast an alive message to refute any suspicions about this node.

        Uses retry_with_backoff for each send since refutation is critical.
        Tracks send failures and logs them but doesn't fail the overall operation.

        Rate limited to prevent incarnation exhaustion attacks - if an attacker
        sends many probes/suspects about us, we don't want to burn through
        all possible incarnation numbers.

        Terminal-abort barrier: a stopping instance must never emit a
        refutation. The harness's hard kill closes the listener but
        previously-spawned SUSPECT handlers can reach this code path
        with a captured transport; without this check they refute their
        own death and the manager treats the killed node as still alive.
        """
        if not self._running:
            return self._incarnation_tracker.get_self_incarnation()

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

        new_incarnation = await self.increment_incarnation()

        # Post-await terminal barrier: ``abort()`` may have flipped
        # ``_running`` while we were inside ``increment_incarnation``.
        # Without this check, an in-flight self-suspicion handler
        # continues into the per-peer send loop after the instance is
        # supposed to be dark, re-emerging on the wire with a fresh
        # incarnation that the manager treats as authoritative liveness.
        if not self._running:
            return new_incarnation

        self_addr = self._get_self_udp_addr()

        self_addr_bytes = f"{self_addr[0]}:{self_addr[1]}".encode()
        msg = (
            b"alive:"
            + str(new_incarnation).encode()
            + b":"
            + self._node_id.full.encode()
            + b">"
            + self_addr_bytes
        )

        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        successful = 0
        failed = 0

        node_addresses = list(self._incarnation_tracker.node_states.keys())
        for node in node_addresses:
            if not self._running:
                break
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
        """Callback for UDP send retry attempts.

        UDP send retries indicate that ``socket.sendto`` raised an
        OSError (e.g. EAGAIN, ENOBUFS) and the retry policy retried.
        That can mean local socket pressure (our fault), kernel buffer
        saturation (could be ours, could be a peer flooding us), or
        ephemeral network issues — none of which are unambiguously
        "this prober is slow at processing messages," which is what
        LHM is supposed to measure (Lifeguard §4.3). Bumping LHM
        here would falsely conflate transient send-failure with
        prober self-health and inflate every probe-timeout and
        suspicion bracket cluster-wide. Send retry telemetry belongs
        in metrics, not LHM.
        """
        self._metrics.increment("send_retries")

    async def broadcast_suspicion(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> None:
        """
        Broadcast a suspicion about a node to all other members.

        Tracks send failures for monitoring but continues to all nodes.
        """
        self_addr = self._get_self_udp_addr()

        target_addr_bytes = f"{target[0]}:{target[1]}".encode()
        msg = b"suspect:" + str(incarnation).encode() + b">" + target_addr_bytes

        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        successful = 0
        failed = 0

        # Send to *all* peers, including the target. Lifeguard §4.2's
        # refutation flow requires the suspected node to receive the
        # SUSPECT — it's the only node that can refute by incrementing
        # its own incarnation and broadcasting ALIVE. Excluding the
        # target leaves refutation dependent on indirect gossip from
        # third parties, which in small clusters (or whenever the only
        # other peers are themselves suspect/dead) means the target
        # never learns it's being suspected and the bracket fires on
        # an alive peer. Direct delivery is also strictly cheaper than
        # waiting for the SUSPECT to propagate via random gossip.
        #
        # Sends are gathered concurrently so a wave of dead destinations
        # cannot serialize the probe loop behind ``N × per_send_timeout``
        # of pure timeout — at N≈50 with the default 1-3 s timeout that
        # serial form blocked the probe loop for ~100 s per round,
        # starving the burst-failure observation window of the failures
        # needed to trip its threshold.
        node_addresses = [
            node
            for node in list(self._incarnation_tracker.node_states.keys())
            if node != self_addr
        ]
        if node_addresses:
            results = await asyncio.gather(
                *(
                    self._send_broadcast_message(node, msg, timeout)
                    for node in node_addresses
                ),
                return_exceptions=False,
            )
            for success in results:
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
            base_timeout = await self._context.read("current_timeout")
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
        Send a probe to target and wait for a fresh response.

        Indirect-probe proxies must never answer from cached membership
        state. A proxy may report ``alive`` only when the target responds
        to this probe attempt and completes the per-target ACK future.

        Returns True if target appears alive, False otherwise.
        """
        base_timeout = await self._context.read("current_timeout")
        timeout = self.get_lhm_adjusted_timeout(base_timeout)

        target_addr = f"{target[0]}:{target[1]}".encode()
        msg = b"probe>" + target_addr

        try:
            existing_future = self._pending_probe_acks.pop(target, None)
            if existing_future and not existing_future.done():
                existing_future.cancel()

            ack_future: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
            self._pending_probe_acks[target] = ack_future
            self._pending_probe_start[target] = time.monotonic()

            await self.send(target, msg, timeout=timeout)
            await asyncio.wait_for(ack_future, timeout=timeout)
            return True

        except asyncio.TimeoutError:
            await self.handle_error(ProbeTimeoutError(target, timeout))
            return False
        except OSError as e:
            await self.handle_error(self._make_network_error(e, target, "Probe"))
            return False
        except Exception as e:
            await self.handle_exception(e, f"probe_and_wait_{target[0]}_{target[1]}")
            return False
        finally:
            self._pending_probe_acks.pop(target, None)
            self._pending_probe_start.pop(target, None)

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

        Terminal-abort barrier: after ``abort()`` flips ``_running``, no
        outbound SWIM message may leave this instance — including ALIVE
        refutations, probes, suspect broadcasts, and gossip piggyback.
        Without this guard, an in-flight handler that captured a
        reference to this server before abort can still reach the
        framework's wire-level ``sendto`` and look alive to peers.
        ``ConnectionResetError`` mimics the OS-level signal a real
        SIGKILL'd process would surface to outbound senders.
        """
        if not self._running:
            raise ConnectionResetError("instance stopped; outbound SWIM send blocked")

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

        # Extract embedded state from response (Serf-style). Pass the
        # *tuple* address so every downstream consumer — most importantly
        # ``confirm_peer`` → ``incarnation_tracker.confirm_node`` — keys
        # the peer the same way the rest of the SWIM layer does. Passing
        # the raw ``bytes`` ``host:port`` here produced phantom tracker
        # entries (one tuple-keyed and one bytes-keyed for every node),
        # inflating ``N`` and stretching every Lifeguard suspicion
        # bracket past the operator-budgeted detection envelope.
        if addr_tuple is None:
            # Source-address parse failed; we can't attribute embedded
            # state to a peer, so just strip piggyback and return the
            # clean message. Skipping is safe: piggyback is auxiliary
            # to the request/response.
            return data
        process_piggybacks = await self._should_process_auxiliary_piggyback(
            addr_tuple,
            data,
        )
        clean_data = await self._extract_embedded_state(
            data,
            addr_tuple,
            process_piggybacks=process_piggybacks,
        )
        clean_msg_type = clean_data.split(b">", maxsplit=1)[0].split(
            b":",
            maxsplit=1,
        )[0]
        if clean_msg_type == b"alive":
            await self._process_direct_alive_response(addr_tuple, clean_data)
        return clean_data

    @udp.receive(priority=MessagePriority.CRITICAL, admission_group="swim")
    async def receive(
        self,
        addr: tuple[str, int],
        data: Message,
        clock_time: int,
    ) -> Message:
        _t_entry = time.monotonic()
        _t_rl_done = _t_pb_done = _t_dedup_done = _t_extract_done = 0.0
        _msg_prefix = data[:8] if data else b""
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

            if data.startswith(b"leave"):
                await self._udp_logger.log(
                    ServerError(
                        message=(
                            f"[RECV-LEAVE] src={addr} self_udp={self._udp_port} "
                            f"data_prefix={data[:96]!r}"
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=self._node_id.short,
                    )
                )

            admission_class = self._classify_swim_admission(addr, data)

            # Check SWIM class-aware rate limit - drop if sender is flooding
            if not await self._check_rate_limit(addr, admission_class):
                if data.startswith(b"leave"):
                    await self._udp_logger.log(
                        ServerError(
                            message=(
                                f"[RATELIMIT-DROP] src={addr} "
                                f"class={admission_class}"
                            ),
                            node_host=self._host,
                            node_port=self._udp_port,
                            node_id=self._node_id.short,
                        )
                )
                return b"nack>" + self._udp_addr_slug
            _t_rl_done = time.monotonic()

            process_piggybacks = await self._should_process_auxiliary_piggyback(
                addr,
                data,
            )
            _t_pb_done = time.monotonic()

            # Check for duplicate messages
            if self._is_duplicate_message(addr, data):
                if data.startswith(b"leave"):
                    await self._udp_logger.log(
                        ServerError(
                            message=(
                                f"[DUPLICATE-LEAVE] src={addr} "
                                f"data_prefix={data[:96]!r}"
                            ),
                            node_host=self._host,
                            node_port=self._udp_port,
                            node_id=self._node_id.short,
                        )
                    )
                # Duplicate - still send ack but don't process
                return b"ack>" + self._udp_addr_slug
            _t_dedup_done = time.monotonic()

            # Strip ALL piggyback (vivaldi/worker_state/health/membership)
            # and any embedded #|s state, mirroring the layout produced by
            # _add_piggyback_safe on send. The previous code only stripped
            # #|h and #|m, leaving #|v (vivaldi) and #|w (worker state)
            # glued onto the target_addr portion of `probe>host:port`.
            # The parser then decoded `host:port#|v{...}` as the address,
            # int() failed on the port, target became None, and every
            # probe was rejected as `Missing target address`.
            data = await self._extract_embedded_state(
                data,
                addr,
                process_piggybacks=process_piggybacks,
            )
            _t_extract_done = time.monotonic()

            if data.startswith((b"pre-vote", b"leader-claim", b"leader-elected",
                                b"leader-heartbeat", b"leader-stepdown",
                                b"vote-grant", b"vote-deny")):
                await self._udp_logger.log(
                    ServerDebug(
                        message=(
                            f"[Leadership] inbound from={addr} "
                            f"data_prefix={data[:48]!r}"
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=self._node_id.short,
                    )
                )
            # Delegate to the message dispatcher for handler-based processing
            result = await self._message_dispatcher.dispatch(addr, data, clock_time)
            _t_dispatch_done = time.monotonic()
            _total_ms = (_t_dispatch_done - _t_entry) * 1000.0
            if _total_ms > 100.0:
                await self._udp_logger.log(
                    ServerError(
                        message=(
                            f"[RECV-SLOW] total_ms={_total_ms:.1f} "
                            f"prefix={_msg_prefix!r} "
                            f"rl_ms={(_t_rl_done - _t_entry) * 1000.0:.1f} "
                            f"pb_ms={(_t_pb_done - _t_rl_done) * 1000.0:.1f} "
                            f"dedup_ms={(_t_dedup_done - _t_pb_done) * 1000.0:.1f} "
                            f"extract_ms="
                            f"{(_t_extract_done - _t_dedup_done) * 1000.0:.1f} "
                            f"dispatch_ms="
                            f"{(_t_dispatch_done - _t_extract_done) * 1000.0:.1f}"
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=self._node_id.short,
                    )
                )
            return result

        except ValueError as error:
            # Message parsing error
            await self.handle_error(MalformedMessageError(data, str(error), addr))
            return b"nack"
        except Exception as error:
            await self.handle_exception(error, "receive")
            return b"nack"
