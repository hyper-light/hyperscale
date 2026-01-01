"""
SWIM + Lifeguard Test Server implementation.

This is the main server class that integrates all SWIM protocol
components with Lifeguard enhancements.
"""

import asyncio
import random
import time
from typing import Literal

from hyperscale.distributed_rewrite.server import tcp, udp, task
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer
from hyperscale.logging.hyperscale_logging_models import ServerInfo

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
    UnexpectedMessageError,
    UnexpectedError,
    QueueFullError,
    StaleMessageError,
    ConnectionRefusedError as SwimConnectionRefusedError,
    SplitBrainError,
    ResourceError,
    TaskOverloadError,
    NotEligibleError,
)
from .core.error_handler import ErrorHandler, ErrorContext
from .core.resource_limits import BoundedDict
from .core.metrics import Metrics
from .core.audit import AuditLog, AuditEventType
from .core.retry import (
    retry_with_backoff,
    retry_with_result,
    PROBE_RETRY_POLICY,
    ELECTION_RETRY_POLICY,
)
from .core.error_handler import ErrorContext

# Health monitoring
from .health.local_health_multiplier import LocalHealthMultiplier
from .health.health_monitor import EventLoopHealthMonitor
from .health.graceful_degradation import GracefulDegradation, DegradationLevel

# Failure detection
from .detection.incarnation_tracker import IncarnationTracker
from .detection.suspicion_state import SuspicionState
from .detection.suspicion_manager import SuspicionManager
from .detection.indirect_probe_manager import IndirectProbeManager
from .detection.probe_scheduler import ProbeScheduler

# Gossip
from .gossip.gossip_buffer import GossipBuffer, MAX_UDP_PAYLOAD

# Leadership
from .leadership.local_leader_election import LocalLeaderElection


class UDPServer(MercurySyncBaseServer[Ctx]):
    """
    SWIM + Lifeguard Protocol Server with Leadership Election.
    
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
        # Message deduplication settings
        dedup_cache_size: int = 2000,  # Default 2K messages (was 10K - excessive)
        dedup_window: float = 30.0,    # Seconds to consider duplicate
        # Rate limiting settings
        rate_limit_cache_size: int = 500,  # Track at most 500 senders
        rate_limit_tokens: int = 100,      # Max tokens per sender
        rate_limit_refill: float = 10.0,   # Tokens per second
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        
        # Generate unique node identity
        self._node_id = NodeId.generate(datacenter=dc_id, priority=priority)
        
        # Initialize SWIM components
        self._local_health = LocalHealthMultiplier()
        self._incarnation_tracker = IncarnationTracker()
        self._suspicion_manager = SuspicionManager()
        self._indirect_probe_manager = IndirectProbeManager()
        self._gossip_buffer = GossipBuffer()
        self._gossip_buffer.set_overflow_callback(self._on_gossip_overflow)
        self._probe_scheduler = ProbeScheduler()
        self._leader_election = LocalLeaderElection(dc_id=dc_id)
        
        # Message deduplication - track recently seen messages to prevent duplicates
        self._seen_messages: BoundedDict[int, float] = BoundedDict(
            max_size=dedup_cache_size,
            eviction_policy='LRA',  # Least Recently Added - old messages first
        )
        self._dedup_window: float = dedup_window
        self._dedup_stats = {'duplicates': 0, 'unique': 0}
        
        # Rate limiting - per-sender token bucket to prevent resource exhaustion
        self._rate_limits: BoundedDict[tuple[str, int], dict] = BoundedDict(
            max_size=rate_limit_cache_size,
            eviction_policy='LRA',
        )
        self._rate_limit_tokens: int = rate_limit_tokens
        self._rate_limit_refill: float = rate_limit_refill
        self._rate_limit_stats = {'accepted': 0, 'rejected': 0}
        
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
        
        # Set up suspicion manager callbacks
        self._suspicion_manager.set_callbacks(
            on_expired=self._on_suspicion_expired,
            get_n_members=self._get_member_count,
            get_lhm_multiplier=self._get_lhm_multiplier,
        )
    
    @property
    def node_id(self) -> NodeId:
        """Get this server's unique node identifier."""
        return self._node_id
    
    def get_node_address(self) -> NodeAddress:
        """Get the full node address (ID + network location)."""
        host, port = self._get_self_udp_addr()
        return NodeAddress(node_id=self._node_id, host=host, port=port)
    
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
            self._metrics.increment('network_errors')
        elif error.category == ErrorCategory.PROTOCOL:
            self._metrics.increment('protocol_errors')
        elif error.category == ErrorCategory.RESOURCE:
            self._metrics.increment('resource_errors')
        
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
        # Pass task runner to suspicion manager for timer management
        self._suspicion_manager.set_task_runner(self._task_runner)
    
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
        await self.increase_failure_detector('event_loop_lag')
    
    async def _on_event_loop_critical(self, lag_ratio: float) -> None:
        """Called when event loop is critically overloaded."""
        # More aggressive LHM increment
        await self.increase_failure_detector('event_loop_critical')
        await self.increase_failure_detector('event_loop_critical')
        
        # Log TaskOverloadError for monitoring
        await self.handle_error(
            TaskOverloadError(
                task_count=len(self._task_runner.tasks),
                max_tasks=100,  # Nominal limit
            )
        )
    
    async def _on_event_loop_recovered(self) -> None:
        """Called when event loop recovers from degraded state."""
        await self.decrease_failure_detector('event_loop_recovered')
    
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
        if new_level.value >= DegradationLevel.SEVERE.value and new_level.value > old_level.value:
            self._task_runner.run(
                self.handle_error,
                TaskOverloadError(
                    task_count=len(self._task_runner.tasks),
                    max_tasks=100,
                ),
            )
        
        # Log the change
        if hasattr(self, '_udp_logger'):
            try:
                from hyperscale.logging.hyperscale_logging_models import ServerInfo as ServerInfoLog
                self._udp_logger.log(
                    ServerInfoLog(
                        message=f"Degradation {direction}: {old_level.name} -> {new_level.name} ({policy.description})",
                        node_host=self._host,
                        node_port=self._port,
                        node_id=self._node_id.numeric_id if hasattr(self, '_node_id') else 0,
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
    
    # === Message Size Helpers ===
    
    def _add_piggyback_safe(self, base_message: bytes) -> bytes:
        """
        Add piggybacked gossip updates to a message, respecting MTU limits.
        
        Args:
            base_message: The core message to send.
        
        Returns:
            Message with piggybacked updates that fits within UDP MTU.
        """
        if len(base_message) >= MAX_UDP_PAYLOAD:
            # Base message already at limit, can't add piggyback
            return base_message
        
        piggyback = self._gossip_buffer.encode_piggyback_with_base(base_message)
        return base_message + piggyback
    
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
            self._cleanup_task = asyncio.create_task(self._run_cleanup_loop())
    
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
            stats['incarnation'] = await self._incarnation_tracker.cleanup()
        
        # Cleanup suspicion manager (orphaned suspicions)
        async with ErrorContext(self._error_handler, "suspicion_cleanup"):
            stats['suspicion'] = await self._suspicion_manager.cleanup()
        
        # Cleanup indirect probe manager
        async with ErrorContext(self._error_handler, "indirect_probe_cleanup"):
            stats['indirect_probe'] = self._indirect_probe_manager.cleanup()
        
        # Cleanup gossip buffer
        async with ErrorContext(self._error_handler, "gossip_cleanup"):
            stats['gossip'] = self._gossip_buffer.cleanup()
        
        # Cleanup old messages from dedup cache
        async with ErrorContext(self._error_handler, "dedup_cleanup"):
            self._seen_messages.cleanup_older_than(self._dedup_window * 2)
        
        # Cleanup old rate limit entries
        async with ErrorContext(self._error_handler, "rate_limit_cleanup"):
            self._rate_limits.cleanup_older_than(60.0)  # 1 minute
        
        # Check for counter overflow and reset if needed
        # (Python handles big ints, but we reset periodically for monitoring clarity)
        self._check_and_reset_stats()
    
    def get_cleanup_stats(self) -> dict:
        """Get cleanup statistics from all components."""
        return {
            'incarnation': self._incarnation_tracker.get_stats(),
            'suspicion': self._suspicion_manager.get_stats(),
            'indirect_probe': self._indirect_probe_manager.get_stats(),
            'gossip': self._gossip_buffer.get_stats(),
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
        if (self._dedup_stats['duplicates'] > MAX_COUNTER or 
            self._dedup_stats['unique'] > MAX_COUNTER):
            self._dedup_stats = {'duplicates': 0, 'unique': 0}
        
        # Reset rate limit stats if too large
        if (self._rate_limit_stats['accepted'] > MAX_COUNTER or
            self._rate_limit_stats['rejected'] > MAX_COUNTER):
            self._rate_limit_stats = {'accepted': 0, 'rejected': 0}
    
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
    
    async def _handle_election_error(self, error) -> None:
        """Handle election errors through the error handler."""
        await self.handle_error(error)
        
        # Set up leadership event callbacks
        self._leader_election.state.set_callbacks(
            on_become_leader=self._on_become_leader,
            on_lose_leadership=self._on_lose_leadership,
            on_leader_change=self._on_leader_change,
        )
    
    def _broadcast_leadership_message(self, message: bytes) -> None:
        """
        Broadcast a leadership message to all known nodes.
        
        Leadership messages are critical - schedule them via task runner
        with error tracking.
        """
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        for node in nodes:
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
        await self.increase_failure_detector('leadership_retry')
    
    def _on_election_started(self) -> None:
        """Called when this node starts an election."""
        self._metrics.increment('elections_started')
        self._audit_log.record(
            AuditEventType.ELECTION_STARTED,
            node=self._get_self_udp_addr(),
            term=self._leader_election.state.current_term,
        )
    
    def _on_heartbeat_sent(self) -> None:
        """Called when this node sends a heartbeat as leader."""
        self._metrics.increment('heartbeats_sent')
    
    def _on_become_leader(self) -> None:
        """Called when this node becomes the leader."""
        self._metrics.increment('elections_won')
        self._metrics.increment('leadership_changes')
        self_addr = self._get_self_udp_addr()
        self._audit_log.record(
            AuditEventType.ELECTION_WON,
            node=self_addr,
            term=self._leader_election.state.current_term,
        )
        self._udp_logger.log(
            ServerInfo(
                message=f"[{self._udp_addr_slug.decode()}] Became LEADER (term {self._leader_election.state.current_term})",
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            )
        )
    
    def _on_lose_leadership(self) -> None:
        """Called when this node loses leadership."""
        self._metrics.increment('elections_lost')
        self._metrics.increment('leadership_changes')
        self_addr = self._get_self_udp_addr()
        self._audit_log.record(
            AuditEventType.ELECTION_LOST,
            node=self_addr,
            term=self._leader_election.state.current_term,
        )
        self._udp_logger.log(
            ServerInfo(
                message=f"[{self._node_id.short}] Lost leadership",
                node_host=self._host,
                node_port=self._udp_port,
                node_id=self._node_id.short,
            )
        )
    
    def _on_leader_change(self, new_leader: tuple[str, int] | None) -> None:
        """Called when the known leader changes."""
        self._audit_log.record(
            AuditEventType.LEADER_CHANGED,
            node=new_leader,
            term=self._leader_election.state.current_term,
        )
        if new_leader:
            self._udp_logger.log(
                ServerInfo(
                    message=f"[{self._node_id.short}] New leader: {new_leader[0]}:{new_leader[1]}",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                )
            )

        else:
            self._udp_logger.log(
                ServerInfo(
                    message=f"[{self._node_id.short}] No leader currently",
                    node_host=self._host,
                    node_port=self._udp_port,
                    node_id=self._node_id.short,
                )
            )
    
    def _get_member_count(self) -> int:
        """Get the current number of known members."""
        nodes = self._context.read('nodes')
        return len(nodes) if nodes else 1
    
    def _on_suspicion_expired(self, node: tuple[str, int], incarnation: int) -> None:
        """Callback when a suspicion expires - mark node as DEAD."""
        self._metrics.increment('suspicions_expired')
        self._audit_log.record(
            AuditEventType.NODE_CONFIRMED_DEAD,
            node=node,
            incarnation=incarnation,
        )
        self._incarnation_tracker.update_node(
            node, 
            b'DEAD', 
            incarnation, 
            time.monotonic(),
        )
        # Queue the death notification for gossip
        self.queue_gossip_update('dead', node, incarnation)
        nodes: Nodes = self._context.read('nodes')
        if node in nodes:
            self._safe_queue_put_sync(nodes[node], (int(time.monotonic()), b'DEAD'), node)
    
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
        self._metrics.increment('gossip_updates_sent')
        n_members = self._get_member_count()
        self._gossip_buffer.add_update(update_type, node, incarnation, n_members)
    
    def get_piggyback_data(self, max_updates: int = 5) -> bytes:
        """Get piggybacked membership updates to append to a message."""
        return self._gossip_buffer.encode_piggyback(max_updates)
    
    async def process_piggyback_data(self, data: bytes) -> None:
        """Process piggybacked membership updates received in a message."""
        updates = GossipBuffer.decode_piggyback(data)
        self._metrics.increment('gossip_updates_received', len(updates))
        for update in updates:
            status_map = {
                'alive': b'OK',
                'join': b'OK', 
                'suspect': b'SUSPECT',
                'dead': b'DEAD',
                'leave': b'DEAD',
            }
            status = status_map.get(update.update_type, b'OK')
            
            if self.is_message_fresh(update.node, update.incarnation, status):
                self.update_node_state(
                    update.node,
                    status,
                    update.incarnation,
                    update.timestamp,
                )
                
                if update.update_type == 'suspect':
                    self_addr = self._get_self_udp_addr()
                    if update.node != self_addr:
                        await self.start_suspicion(
                            update.node,
                            update.incarnation,
                            self_addr,
                        )
                elif update.update_type == 'alive':
                    await self.refute_suspicion(update.node, update.incarnation)
                
                self.queue_gossip_update(
                    update.update_type,
                    update.node,
                    update.incarnation,
                )

    def get_other_nodes(self, node: tuple[str, int]):
        target_host, target_port = node
        nodes: Nodes = self._context.read('nodes')
        return [
            (host, port) for host, port in nodes 
            if target_host != host and target_port != port
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
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        # Check node status
        nodes: Nodes = self._context.read('nodes')
        node_entry = nodes.get(node)
        if not node_entry:
            return False
        
        try:
            _, status = node_entry.get_nowait()
            if status != b'OK':
                return False
        except asyncio.QueueEmpty:
            return False
        
        if include_piggyback:
            message = message + self.get_piggyback_data()
        
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
        join_msg = b'join>' + f'{self_addr[0]}:{self_addr[1]}'.encode()
        
        async def attempt_join() -> bool:
            await self.send(seed_node, join_msg, timeout=timeout)
            # Add seed to our known nodes
            self._context.write(seed_node, b'OK')
            self._probe_scheduler.add_member(seed_node)
            return True
        
        result = await retry_with_result(
            attempt_join,
            policy=ELECTION_RETRY_POLICY,  # Use election policy for joining
            on_retry=lambda a, e, d: self.increase_failure_detector('join_retry'),
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
        
        # Start health monitor for proactive CPU detection
        await self.start_health_monitor()
        
        # Start cleanup task
        await self.start_cleanup()
        
        self._probe_scheduler._running = True
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        members = [node for node in nodes.keys() if node != self_addr]
        self._probe_scheduler.update_members(members)
        
        protocol_period = self._context.read('udp_poll_interval', 1.0)
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
        # Check circuit breaker - if too many network errors, back off
        if self._error_handler and self._error_handler.is_circuit_open(ErrorCategory.NETWORK):
            # Network circuit is open - skip this round to let things recover
            await asyncio.sleep(1.0)  # Brief pause before next attempt
            return
        
        target = self._probe_scheduler.get_next_target()
        if target is None:
            return
        
        if self.udp_target_is_self(target):
            return
        
        # Use ErrorContext for consistent error handling throughout the probe
        async with ErrorContext(self._error_handler, f"probe_round_{target[0]}_{target[1]}") as ctx:
            node_state = self._incarnation_tracker.get_node_state(target)
            incarnation = node_state.incarnation if node_state else 0
            
            base_timeout = self._context.read('current_timeout')
            timeout = self.get_lhm_adjusted_timeout(base_timeout)
            
            target_addr = f'{target[0]}:{target[1]}'.encode()
            probe_msg = b'probe>' + target_addr + self.get_piggyback_data()
            
            response_received = await self._probe_with_timeout(target, probe_msg, timeout)
            
            if response_received:
                await self.decrease_failure_detector('successful_probe')
                ctx.record_success(ErrorCategory.NETWORK)  # Help circuit breaker recover
                return
            
            await self.increase_failure_detector('probe_timeout')
            indirect_sent = await self.initiate_indirect_probe(target, incarnation)
            
            if indirect_sent:
                await asyncio.sleep(timeout)
                probe = self._indirect_probe_manager.get_pending_probe(target)
                if probe and probe.is_completed():
                    await self.decrease_failure_detector('successful_probe')
                    ctx.record_success(ErrorCategory.NETWORK)
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
        Returns True if probe succeeded, False if all retries exhausted.
        """
        self._metrics.increment('probes_sent')
        attempt = 0
        max_attempts = PROBE_RETRY_POLICY.max_retries + 1
        
        while attempt < max_attempts:
            try:
                # Send probe
                await self.send(target, message, timeout=timeout)
                
                # Wait for potential response (reduced time for retries)
                wait_time = timeout * 0.5 if attempt < max_attempts - 1 else timeout * 0.8
                await asyncio.sleep(wait_time)
                
                # Check if we got an ack (tracked via incarnation/node state)
                node_state = self._incarnation_tracker.get_node_state(target)
                if node_state and node_state.status == b'OK':
                    self._metrics.increment('probes_received')  # Got response
                    return True
                
                attempt += 1
                if attempt < max_attempts:
                    # Exponential backoff with jitter before retry
                    backoff = PROBE_RETRY_POLICY.base_delay * (
                        PROBE_RETRY_POLICY.backoff_multiplier ** (attempt - 1)
                    )
                    jitter = random.uniform(0, PROBE_RETRY_POLICY.jitter_factor * backoff)
                    await asyncio.sleep(backoff + jitter)
                    
            except asyncio.TimeoutError:
                attempt += 1
                if attempt >= max_attempts:
                    self._metrics.increment('probes_timeout')
                    await self.handle_error(ProbeTimeoutError(target, timeout))
                    return False
            except OSError as e:
                # Network error - wrap with appropriate error type
                self._metrics.increment('probes_failed')
                await self.handle_error(self._make_network_error(e, target, "Probe"))
                return False
            except Exception as e:
                self._metrics.increment('probes_failed')
                await self.handle_exception(e, f"probe_{target[0]}_{target[1]}")
                return False
        
        self._metrics.increment('probes_failed')
        return False
    
    def stop_probe_cycle(self) -> None:
        """Stop the probe cycle."""
        self._probe_scheduler.stop()
    
    def update_probe_scheduler_membership(self) -> None:
        """Update the probe scheduler with current membership."""
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        members = [node for node in nodes.keys() if node != self_addr]
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
    
    async def graceful_shutdown(
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
                leave_msg = b'leave>' + f'{self_addr[0]}:{self_addr[1]}'.encode()
                nodes: Nodes = self._context.read('nodes')
                timeout = self.get_lhm_adjusted_timeout(1.0)
                
                send_failures = 0
                for node in nodes.keys():
                    if node != self_addr:
                        try:
                            await self.send(node, leave_msg, timeout=timeout)
                        except Exception as e:
                            # Best effort - log but don't fail shutdown for send errors
                            send_failures += 1
                            from hyperscale.logging.hyperscale_logging_models import ServerDebug
                            self._udp_logger.log(ServerDebug(
                                message=f"Leave broadcast to {node[0]}:{node[1]} failed: {type(e).__name__}",
                                node_host=self._host,
                                node_port=self._port,
                                node_id=self._node_id.numeric_id,
                            ))
                
                if send_failures > 0:
                    from hyperscale.logging.hyperscale_logging_models import ServerDebug
                    self._udp_logger.log(ServerDebug(
                        message=f"Leave broadcast: {send_failures}/{len(nodes)-1} sends failed",
                        node_host=self._host,
                        node_port=self._port,
                        node_id=self._node_id.numeric_id,
                    ))
            except Exception as e:
                if self._error_handler:
                    await self.handle_exception(e, "shutdown_broadcast_leave")
        
        # 3. Wait for drain period
        if drain_timeout > 0:
            await asyncio.sleep(drain_timeout)
        
        # 4. Stop all background tasks in proper order
        # Stop leader election first (stops sending heartbeats)
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
        
        # 5. Log final audit event
        self._audit_log.add_event(
            AuditEventType.NODE_LEFT,
            node=self_addr,
            details={'reason': 'graceful_shutdown'},
        )
    
    def get_current_leader(self) -> tuple[str, int] | None:
        """Get the current leader, if known."""
        return self._leader_election.get_current_leader()
    
    def is_leader(self) -> bool:
        """Check if this node is the current leader."""
        return self._leader_election.state.is_leader()
    
    def get_leadership_status(self) -> dict:
        """Get current leadership status for debugging."""
        return self._leader_election.get_status()

    async def increase_failure_detector(self, event_type: str = 'probe_timeout'):
        """Increase local health score based on event type."""
        if event_type == 'probe_timeout':
            self._local_health.on_probe_timeout()
        elif event_type == 'refutation':
            self._local_health.on_refutation_needed()
        elif event_type == 'missed_nack':
            self._local_health.on_missed_nack()
        elif event_type == 'event_loop_lag':
            self._local_health.on_event_loop_lag()
        elif event_type == 'event_loop_critical':
            self._local_health.on_event_loop_critical()
        else:
            self._local_health.increment()

    async def decrease_failure_detector(self, event_type: str = 'successful_probe'):
        """Decrease local health score based on event type."""
        if event_type == 'successful_probe':
            self._local_health.on_successful_probe()
        elif event_type == 'successful_nack':
            self._local_health.on_successful_nack()
        elif event_type == 'event_loop_recovered':
            self._local_health.on_event_loop_recovered()
        else:
            self._local_health.decrement()
    
    def get_lhm_adjusted_timeout(self, base_timeout: float) -> float:
        """Get timeout adjusted by Local Health Multiplier and degradation level."""
        lhm_multiplier = self._local_health.get_multiplier()
        degradation_multiplier = self._degradation.get_timeout_multiplier()
        return base_timeout * lhm_multiplier * degradation_multiplier
    
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
        msg = msg_type + b':' + str(inc).encode()
        if target:
            msg += b'>' + f'{target[0]}:{target[1]}'.encode()
        return msg
    
    def decode_message_with_incarnation(
        self, 
        data: bytes,
    ) -> tuple[bytes, int, tuple[str, int] | None]:
        """Decode a SWIM message with incarnation number."""
        parts = data.split(b'>', maxsplit=1)
        msg_part = parts[0]
        
        target = None
        if len(parts) > 1:
            target_str = parts[1].decode()
            host, port = target_str.split(':', maxsplit=1)
            target = (host, int(port))
        
        msg_parts = msg_part.split(b':', maxsplit=1)
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
        msg_parts = message.split(b':', maxsplit=1)
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
        msg_parts = message.split(b':', maxsplit=1)
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
        msg_parts = message.split(b':', maxsplit=2)
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
        msg_parts = message.split(b':', maxsplit=2)
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
            granted = msg_parts[2].decode() == '1'
        
        return term, granted
    
    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: Status,
    ) -> bool:
        """Check if a message about a node should be processed."""
        is_fresh = self._incarnation_tracker.is_message_fresh(node, incarnation, status)
        if not is_fresh:
            # Log stale message for monitoring (don't await since this is sync)
            self._task_runner.run(
                self.handle_error,
                StaleMessageError(
                    node,
                    incarnation,
                    self._incarnation_tracker.get_incarnation(node),
                ),
            )
        return is_fresh
    
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
                self._dedup_stats['duplicates'] += 1
                self._metrics.increment('messages_deduplicated')
                return True
            # Seen but outside window - update timestamp
            self._seen_messages[msg_hash] = now
        else:
            # New message - track it
            self._seen_messages[msg_hash] = now
        
        self._dedup_stats['unique'] += 1
        return False
    
    def get_dedup_stats(self) -> dict:
        """Get message deduplication statistics."""
        return {
            'duplicates': self._dedup_stats['duplicates'],
            'unique': self._dedup_stats['unique'],
            'cache_size': len(self._seen_messages),
            'window_seconds': self._dedup_window,
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
                'tokens': self._rate_limit_tokens,
                'last_refill': now,
            }
        
        bucket = self._rate_limits[addr]
        
        # Refill tokens based on elapsed time
        elapsed = now - bucket['last_refill']
        refill = int(elapsed * self._rate_limit_refill)
        if refill > 0:
            bucket['tokens'] = min(
                bucket['tokens'] + refill,
                self._rate_limit_tokens,
            )
            bucket['last_refill'] = now
        
        # Check if we have tokens
        if bucket['tokens'] > 0:
            bucket['tokens'] -= 1
            self._rate_limit_stats['accepted'] += 1
            return True
        else:
            self._rate_limit_stats['rejected'] += 1
            self._metrics.increment('messages_rate_limited')
            # Log rate limit violation
            await self.handle_error(
                ResourceError(
                    f"Rate limit exceeded for {addr[0]}:{addr[1]}",
                    source=addr,
                    tokens=bucket['tokens'],
                )
            )
            return False
    
    def get_rate_limit_stats(self) -> dict:
        """Get rate limiting statistics."""
        return {
            'accepted': self._rate_limit_stats['accepted'],
            'rejected': self._rate_limit_stats['rejected'],
            'tracked_senders': len(self._rate_limits),
            'tokens_per_sender': self._rate_limit_tokens,
            'refill_rate': self._rate_limit_refill,
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
        # Clear any active suspicion
        if node in self._suspicion_manager.suspicions:
            await self._suspicion_manager.refute_suspicion(
                node,
                self._incarnation_tracker.get_incarnation(node) + 1,
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
        self._metrics.increment('gossip_buffer_overflows')
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
        """Update the state of a node. Returns True if state changed."""
        return self._incarnation_tracker.update_node(node, status, incarnation, timestamp)
    
    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> SuspicionState | None:
        """Start suspecting a node or add confirmation to existing suspicion."""
        self._metrics.increment('suspicions_started')
        self._audit_log.record(
            AuditEventType.NODE_SUSPECTED,
            node=node,
            from_node=from_node,
            incarnation=incarnation,
        )
        self._incarnation_tracker.update_node(
            node,
            b'SUSPECT',
            incarnation,
            time.monotonic(),
        )
        return await self._suspicion_manager.start_suspicion(node, incarnation, from_node)
    
    async def confirm_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """Add a confirmation to an existing suspicion."""
        result = await self._suspicion_manager.confirm_suspicion(node, incarnation, from_node)
        if result:
            self._metrics.increment('suspicions_confirmed')
        return result
    
    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """Refute a suspicion - the node proved it's alive."""
        if await self._suspicion_manager.refute_suspicion(node, incarnation):
            self._metrics.increment('suspicions_refuted')
            self._audit_log.record(
                AuditEventType.NODE_REFUTED,
                node=node,
                incarnation=incarnation,
            )
            self._incarnation_tracker.update_node(
                node,
                b'OK',
                incarnation,
                time.monotonic(),
            )
            return True
        return False
    
    def is_node_suspected(self, node: tuple[str, int]) -> bool:
        """Check if a node is currently under suspicion."""
        return self._suspicion_manager.is_suspected(node)
    
    def get_suspicion_timeout(self, node: tuple[str, int]) -> float | None:
        """Get the remaining timeout for a suspicion, if any."""
        state = self._suspicion_manager.get_suspicion(node)
        return state.time_remaining() if state else None
    
    def get_random_proxy_nodes(
        self, 
        target: tuple[str, int], 
        k: int = 3,
    ) -> list[tuple[str, int]]:
        """Get k random nodes to use as proxies for indirect probing."""
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        candidates = [
            node for node, queue in nodes.items()
            if node != target and node != self_addr
        ]
        
        k = min(k, len(candidates))
        if k <= 0:
            return []
        return random.sample(candidates, k)
    
    def _get_self_udp_addr(self) -> tuple[str, int]:
        """Get this server's UDP address as a tuple."""
        host, port = self._udp_addr_slug.decode().split(':')
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
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        probe = self._indirect_probe_manager.start_indirect_probe(
            target=target,
            requester=self._get_self_udp_addr(),
            timeout=timeout,
        )
        self._metrics.increment('indirect_probes_sent')
        
        target_addr = f'{target[0]}:{target[1]}'.encode()
        msg = b'ping-req:' + str(incarnation).encode() + b'>' + target_addr
        
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
                    success = await self._send_indirect_probe_to_proxy(proxy, msg, timeout)
                    if success:
                        probe.add_proxy(proxy)
                        successful_sends += 1
        
        if successful_sends == 0:
            await self.handle_error(
                IndirectProbeTimeoutError(target, proxies, timeout)
            )
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
            await self.handle_error(self._make_network_error(e, proxy, "Indirect probe"))
            return False
        except Exception as e:
            await self.handle_exception(e, f"indirect_probe_proxy_{proxy[0]}_{proxy[1]}")
            return False
    
    async def handle_indirect_probe_response(
        self,
        target: tuple[str, int],
        is_alive: bool,
    ) -> None:
        """Handle response from an indirect probe."""
        if is_alive:
            if self._indirect_probe_manager.record_ack(target):
                await self.decrease_failure_detector('successful_probe')
    
    async def broadcast_refutation(self) -> int:
        """
        Broadcast an alive message to refute any suspicions about this node.
        
        Uses retry_with_backoff for each send since refutation is critical.
        Tracks send failures and logs them but doesn't fail the overall operation.
        """
        new_incarnation = self.increment_incarnation()
        
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        self_addr_bytes = f'{self_addr[0]}:{self_addr[1]}'.encode()
        msg = b'alive:' + str(new_incarnation).encode() + b'>' + self_addr_bytes
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        successful = 0
        failed = 0
        
        for node in nodes:
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
                    severity=ErrorSeverity.TRANSIENT if successful > 0 else ErrorSeverity.DEGRADED,
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
                await self.handle_exception(result.last_error, f"send_retry_{target[0]}_{target[1]}")
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
        await self.increase_failure_detector('send_retry')
    
    async def broadcast_suspicion(
        self, 
        target: tuple[str, int], 
        incarnation: int,
    ) -> None:
        """
        Broadcast a suspicion about a node to all other members.
        
        Tracks send failures for monitoring but continues to all nodes.
        """
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        target_addr_bytes = f'{target[0]}:{target[1]}'.encode()
        msg = b'suspect:' + str(incarnation).encode() + b'>' + target_addr_bytes
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        successful = 0
        failed = 0
        
        for node in nodes:
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
            base_timeout = self._context.read('current_timeout')
            timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        try:
            await self.send(target, message, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            await self.handle_error(
                ProbeTimeoutError(target, timeout)
            )
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
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        target_addr = f'{target[0]}:{target[1]}'.encode()
        msg = b'probe>' + target_addr
        
        # Get current node state before probe
        state_before = self._incarnation_tracker.get_node_state(target)
        last_seen_before = state_before.last_seen if state_before else 0
        
        try:
            # Send probe with error handling
            await self.send(target, msg, timeout=timeout)
            
            # Wait for potential response to arrive
            await asyncio.sleep(min(timeout * 0.7, 0.5))
            
            # Check if node state was updated (indicates response received)
            state_after = self._incarnation_tracker.get_node_state(target)
            if state_after:
                # Node was updated more recently than before our probe
                if state_after.last_seen > last_seen_before:
                    return state_after.status == b'OK'
                # Node status is OK
                if state_after.status == b'OK':
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

    @udp.send('receive')
    async def send(
        self,
        addr: tuple[str, int],
        message: bytes,
        timeout: int | None = None,
    ) -> bytes:
        return (
            addr,
            message,
            timeout,
        )
    
    @udp.handle('receive')
    async def process(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> Message:
        return data

    
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
                return b'nack>' + self._udp_addr_slug
            
            # Validate message has content
            if len(data) == 0:
                await self.handle_error(
                    ProtocolError(
                        f"Empty message from {addr[0]}:{addr[1]}",
                        source=addr,
                    )
                )
                return b'nack>' + self._udp_addr_slug
            
            # Check rate limit - drop if sender is flooding
            if not await self._check_rate_limit(addr):
                return b'nack>' + self._udp_addr_slug
            
            # Check for duplicate messages
            if self._is_duplicate_message(addr, data):
                # Duplicate - still send ack but don't process
                return b'ack>' + self._udp_addr_slug
            
            # Extract any piggybacked membership updates first
            piggyback_idx = data.find(b'|')
            if piggyback_idx > 0:
                main_data = data[:piggyback_idx]
                piggyback_data = data[piggyback_idx:]
                await self.process_piggyback_data(piggyback_data)
                data = main_data

            parsed = data.split(b'>', maxsplit=1)
            message = data

            target: tuple[str, int] | None = None
            target_addr: bytes | None = None
            source_addr = f'{addr[0]}:{addr[1]}'
            if len(parsed) > 1:
                message, target_addr = parsed
                host, port = target_addr.decode().split(':', maxsplit=1)
                target = (host, int(port))
            
            # Extract message type (before first colon)
            msg_type = message.split(b':', maxsplit=1)[0]

            match msg_type:
                case b'ack' | b'nack':
                    # ack/nack may or may not have target
                    if target:
                        nodes: Nodes = self._context.read('nodes')
                        if target not in nodes:
                            await self.increase_failure_detector('missed_nack')
                            return b'nack>' + self._udp_addr_slug
                        await self.decrease_failure_detector('successful_nack')
                    return b'ack>' + self._udp_addr_slug
                
                case b'join':
                    self._metrics.increment('joins_received')
                    if not await self._validate_target(target, b'join', addr):
                        return b'nack>' + self._udp_addr_slug
                    
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'ack' + b'>' + self._udp_addr_slug
                        
                        # Check if this is a rejoin
                        is_rejoin = target in nodes
                        
                        # Clear any stale state from previous membership
                        await self._clear_stale_state(target)
                        
                        # Record audit event
                        event_type = AuditEventType.NODE_REJOIN if is_rejoin else AuditEventType.NODE_JOINED
                        self._audit_log.record(
                            event_type,
                            node=target,
                            source=addr,
                        )
                        
                        self._context.write(target, b'OK')

                        others = self.get_other_nodes(target)
                        base_timeout = self._context.read('current_timeout')
                        gather_timeout = self.get_lhm_adjusted_timeout(base_timeout) * 2
                        await self._gather_with_errors(
                            [self.send_if_ok(node, b'join>' + target_addr) for node in others],
                            operation="join_propagation",
                            timeout=gather_timeout,
                        )

                        await self._safe_queue_put(nodes[target], (clock_time, b'OK'), target)
                        
                        self._probe_scheduler.add_member(target)
                        self._incarnation_tracker.update_node(target, b'OK', 0, time.monotonic())

                        return b'ack>' + self._udp_addr_slug

                case b'leave':
                    if not await self._validate_target(target, b'leave', addr):
                        return b'nack>' + self._udp_addr_slug
                    
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'leave>' + self._udp_addr_slug

                        if target not in nodes:
                            await self.increase_failure_detector('missed_nack')
                            return b'nack>' + self._udp_addr_slug
                        
                        # Record audit event
                        self._audit_log.record(
                            AuditEventType.NODE_LEFT,
                            node=target,
                            source=addr,
                        )
                        
                        others = self.get_other_nodes(target)
                        base_timeout = self._context.read('current_timeout')
                        gather_timeout = self.get_lhm_adjusted_timeout(base_timeout) * 2
                        await self._gather_with_errors(
                            [self.send_if_ok(node, message + b'>' + target_addr) for node in others],
                            operation="leave_propagation",
                            timeout=gather_timeout,
                        )

                        await self._safe_queue_put(nodes[target], (clock_time, b'DEAD'), target)
                        self._context.write('nodes', nodes)

                        return b'ack>' + self._udp_addr_slug
                
                case b'probe':
                    if not await self._validate_target(target, b'probe', addr):
                        return b'nack>' + self._udp_addr_slug
                    
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            await self.increase_failure_detector('refutation')
                            new_incarnation = await self.broadcast_refutation()
                            return b'alive:' + str(new_incarnation).encode() + b'>' + self._udp_addr_slug
                        
                        if target not in nodes:
                            return b'nack>' + self._udp_addr_slug
                        
                        base_timeout = self._context.read('current_timeout')
                        timeout = self.get_lhm_adjusted_timeout(base_timeout)

                        self._task_runner.run(
                            self.send,
                            target,
                            b'ack>' + source_addr.encode(),
                            timeout=timeout,
                        )
                        
                        others = self.get_other_nodes(target)
                        gather_timeout = timeout * 2
                        await self._gather_with_errors(
                            [self.send_if_ok(node, message + b'>' + target_addr) for node in others],
                            operation="probe_propagation",
                            timeout=gather_timeout,
                        )
                            
                        return b'ack'
                
                case b'ping-req':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')
                        
                        if target is None:
                            return b'nack>' + self._udp_addr_slug
                        
                        if self.udp_target_is_self(target):
                            return b'ping-req-ack:alive>' + self._udp_addr_slug
                        
                        if target not in nodes:
                            return b'ping-req-ack:unknown>' + self._udp_addr_slug
                        
                        base_timeout = self._context.read('current_timeout')
                        timeout = self.get_lhm_adjusted_timeout(base_timeout)
                        
                        try:
                            result = await asyncio.wait_for(
                                self._send_probe_and_wait(target),
                                timeout=timeout,
                            )
                            if result:
                                return b'ping-req-ack:alive>' + target_addr
                            else:
                                return b'ping-req-ack:dead>' + target_addr
                        except asyncio.TimeoutError:
                            return b'ping-req-ack:timeout>' + target_addr
                
                case b'ping-req-ack':
                    # Verify we have a pending indirect probe for this target
                    if target and not self._indirect_probe_manager.get_pending_probe(target):
                        await self.handle_error(
                            UnexpectedMessageError(
                                msg_type=b'ping-req-ack',
                                expected=None,  # Not expecting this at all
                                source=addr,
                            )
                        )
                        return b'ack>' + self._udp_addr_slug
                    
                    msg_parts = message.split(b':', maxsplit=1)
                    if len(msg_parts) > 1:
                        status_str = msg_parts[1]
                        if status_str == b'alive' and target:
                            await self.handle_indirect_probe_response(target, is_alive=True)
                            await self.decrease_failure_detector('successful_probe')
                            return b'ack>' + self._udp_addr_slug
                        elif status_str in (b'dead', b'timeout', b'unknown') and target:
                            await self.handle_indirect_probe_response(target, is_alive=False)
                    return b'ack>' + self._udp_addr_slug
                
                case b'alive':
                    msg_incarnation = await self._parse_incarnation_safe(message, addr)
                    
                    if target:
                        if self.is_message_fresh(target, msg_incarnation, b'OK'):
                            await self.refute_suspicion(target, msg_incarnation)
                            self.update_node_state(
                                target, 
                                b'OK', 
                                msg_incarnation, 
                                time.monotonic(),
                            )
                            await self.decrease_failure_detector('successful_probe')
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'suspect':
                    msg_incarnation = await self._parse_incarnation_safe(message, addr)
                    
                    if target:
                        if self.udp_target_is_self(target):
                            await self.increase_failure_detector('refutation')
                            new_incarnation = await self.broadcast_refutation()
                            return b'alive:' + str(new_incarnation).encode() + b'>' + self._udp_addr_slug
                        
                        if self.is_message_fresh(target, msg_incarnation, b'SUSPECT'):
                            await self.start_suspicion(target, msg_incarnation, addr)
                            
                            suspicion = self._suspicion_manager.get_suspicion(target)
                            if suspicion and suspicion.should_regossip():
                                suspicion.mark_regossiped()
                                await self.broadcast_suspicion(target, msg_incarnation)
                    
                    return b'ack>' + self._udp_addr_slug
                
                # Leadership messages
                case b'leader-claim':
                    term, candidate_lhm = await self._parse_leadership_claim(message, addr)
                    
                    if target:
                        vote_msg = self._leader_election.handle_claim(target, term, candidate_lhm)
                        if vote_msg:
                            self._task_runner.run(
                                self.send,
                                target,
                                vote_msg,
                                timeout=self.get_lhm_adjusted_timeout(
                                    self._context.read('current_timeout')
                                ),
                            )
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-vote':
                    # Verify we're actually expecting votes (are we a candidate?)
                    if not self._leader_election.state.is_candidate():
                        await self.handle_error(
                            UnexpectedMessageError(
                                msg_type=b'leader-vote',
                                expected=[b'probe', b'ack', b'leader-heartbeat'],
                                source=addr,
                            )
                        )
                        return b'ack>' + self._udp_addr_slug
                    
                    term = await self._parse_term_safe(message, addr)
                    
                    if self._leader_election.handle_vote(addr, term):
                        self._leader_election.state.become_leader(term)
                        self._leader_election.state.current_leader = self._get_self_udp_addr()
                        
                        self_addr = self._get_self_udp_addr()
                        elected_msg = (
                            b'leader-elected:' +
                            str(term).encode() + b'>' +
                            f'{self_addr[0]}:{self_addr[1]}'.encode()
                        )
                        self._broadcast_leadership_message(elected_msg)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-elected':
                    term = await self._parse_term_safe(message, addr)
                    
                    if target:
                        # Check if we received our own election announcement (shouldn't happen)
                        self_addr = self._get_self_udp_addr()
                        if target == self_addr:
                            await self.handle_error(
                                UnexpectedMessageError(
                                    msg_type=b'leader-elected',
                                    expected=None,
                                    source=addr,
                                )
                            )
                            return b'ack>' + self._udp_addr_slug
                        
                        await self._leader_election.handle_elected(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-heartbeat':
                    self._metrics.increment('heartbeats_received')
                    term = await self._parse_term_safe(message, addr)
                    
                    # Check if we received our own heartbeat (shouldn't happen)
                    if target:
                        self_addr = self._get_self_udp_addr()
                        if target == self_addr and addr != self_addr:
                            await self.handle_error(
                                UnexpectedMessageError(
                                    msg_type=b'leader-heartbeat',
                                    expected=None,
                                    source=addr,
                                )
                            )
                            return b'ack>' + self._udp_addr_slug
                    
                    if target:
                        self_addr = self._get_self_udp_addr()
                        if self._leader_election.state.is_leader() and target != self_addr:
                            should_yield = self._leader_election.handle_discovered_leader(target, term)

                            self._udp_logger.log(
                                ServerInfo(
                                    message=f"[{self._node_id.short}] Received heartbeat from leader {target} term={term}, yield={should_yield}",
                                    node_host=self._host,
                                    node_port=self._udp_port,
                                    node_id=self._node_id.short,
                                )
                            )

                            if should_yield:
                                self._udp_logger.log(
                                    ServerInfo(
                                        message=f"[SPLIT-BRAIN] Detected other leader {target} with term {term}, stepping down",
                                        node_host=self._host,
                                        node_port=self._udp_port,
                                        node_id=self._node_id.short,
                                    )
                                )
                                # Record split brain in audit log
                                self_addr = self._get_self_udp_addr()
                                self._audit_log.record(
                                    AuditEventType.SPLIT_BRAIN_DETECTED,
                                    node=self_addr,
                                    other_leader=target,
                                    self_term=self._leader_election.state.current_term,
                                    other_term=term,
                                )
                                self._metrics.increment('split_brain_events')
                                # Also log via error handler for monitoring
                                await self.handle_error(
                                    SplitBrainError(
                                        self_addr,
                                        target,
                                        self._leader_election.state.current_term,
                                        term,
                                    )
                                )
                                self._task_runner.run(self._leader_election._step_down)
                        
                        await self._leader_election.handle_heartbeat(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-stepdown':
                    term = await self._parse_term_safe(message, addr)
                    
                    if target:
                        await self._leader_election.handle_stepdown(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'pre-vote-req':
                    term, candidate_lhm = await self._parse_leadership_claim(message, addr)
                    
                    if target:
                        resp = self._leader_election.handle_pre_vote_request(
                            candidate=target,
                            term=term,
                            candidate_lhm=candidate_lhm,
                        )
                        if resp:
                            self._task_runner.run(
                                self._send_to_addr,
                                target,
                                resp,
                            )
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'pre-vote-resp':
                    # Verify we're actually in a pre-voting phase
                    if not self._leader_election.state.pre_voting_in_progress:
                        await self.handle_error(
                            UnexpectedMessageError(
                                msg_type=b'pre-vote-resp',
                                expected=None,  # Not expecting this
                                source=addr,
                            )
                        )
                        return b'ack>' + self._udp_addr_slug
                    
                    term, granted = await self._parse_pre_vote_response(message, addr)
                    
                    self._leader_election.handle_pre_vote_response(
                        voter=addr,
                        term=term,
                        granted=granted,
                    )
                    
                    return b'ack>' + self._udp_addr_slug
                    
                case _:
                    # Unknown message type - log for monitoring
                    await self.handle_error(
                        ProtocolError(
                            f"Unknown message type: {msg_type.decode(errors='replace')}",
                            source=addr,
                        )
                    )
                    return b'nack'
                
        except ValueError as e:
            # Message parsing error
            await self.handle_error(
                MalformedMessageError(data, str(e), addr)
            )
            return b'nack'
        except Exception as e:
            await self.handle_exception(e, "receive")
            return b'nack'

