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

from .types import Status, Nodes, Ctx, UpdateType, Message
from .node_id import NodeId, NodeAddress
from .local_health_multiplier import LocalHealthMultiplier
from .incarnation_tracker import IncarnationTracker
from .suspicion_state import SuspicionState
from .suspicion_manager import SuspicionManager
from .indirect_probe_manager import IndirectProbeManager
from .gossip_buffer import GossipBuffer
from .probe_scheduler import ProbeScheduler
from .local_leader_election import LocalLeaderElection
from .errors import (
    SwimError,
    ErrorCategory,
    NetworkError,
    ProbeTimeoutError,
    ProtocolError,
    MalformedMessageError,
    UnexpectedError,
)
from .error_handler import ErrorHandler, ErrorContext
from .retry import retry_with_backoff, PROBE_RETRY_POLICY
from .health_monitor import EventLoopHealthMonitor


class TestServer(MercurySyncBaseServer[Ctx]):
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
        self._probe_scheduler = ProbeScheduler()
        self._leader_election = LocalLeaderElection(dc_id=dc_id)
        
        # Initialize error handler (logger set up after server starts)
        self._error_handler: ErrorHandler | None = None
        
        # Event loop health monitor (proactive CPU saturation detection)
        self._health_monitor = EventLoopHealthMonitor()
        
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
        if self._error_handler:
            await self._error_handler.handle(error)
    
    async def handle_exception(self, exc: BaseException, operation: str) -> None:
        """Handle a raw exception, converting to SwimError."""
        if self._error_handler:
            await self._error_handler.handle_exception(exc, operation)
    
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
    
    async def _on_event_loop_recovered(self) -> None:
        """Called when event loop recovers from degraded state."""
        await self.decrease_failure_detector('event_loop_recovered')
    
    async def start_health_monitor(self) -> None:
        """Start the event loop health monitor."""
        self._setup_health_monitor()
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
        """Run one cleanup cycle for all SWIM components."""
        stats = {}
        
        # Cleanup incarnation tracker (dead node GC)
        try:
            stats['incarnation'] = self._incarnation_tracker.cleanup()
        except Exception as e:
            await self.handle_exception(e, "incarnation_cleanup")
        
        # Cleanup suspicion manager (orphaned suspicions)
        try:
            stats['suspicion'] = self._suspicion_manager.cleanup()
        except Exception as e:
            await self.handle_exception(e, "suspicion_cleanup")
        
        # Cleanup indirect probe manager
        try:
            stats['indirect_probe'] = self._indirect_probe_manager.cleanup()
        except Exception as e:
            await self.handle_exception(e, "indirect_probe_cleanup")
        
        # Cleanup gossip buffer
        try:
            stats['gossip'] = self._gossip_buffer.cleanup()
        except Exception as e:
            await self.handle_exception(e, "gossip_cleanup")
    
    def get_cleanup_stats(self) -> dict:
        """Get cleanup statistics from all components."""
        return {
            'incarnation': self._incarnation_tracker.get_stats(),
            'suspicion': self._suspicion_manager.get_stats(),
            'indirect_probe': self._indirect_probe_manager.get_stats(),
            'gossip': self._gossip_buffer.get_stats(),
        }
    
    def _setup_leader_election(self) -> None:
        """Initialize leader election callbacks after server is started."""
        self._leader_election.set_callbacks(
            broadcast_message=self._broadcast_leadership_message,
            get_member_count=self._get_member_count,
            get_lhm_score=lambda: self._local_health.score,
            self_addr=self._get_self_udp_addr(),
            on_error=self._handle_election_error,
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
        """Broadcast a leadership message to all known nodes."""
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        for node in nodes:
            if node != self_addr:
                self._task_runner.run(
                    self.send,
                    node,
                    message,
                    timeout=timeout,
                )
    
    def _on_become_leader(self) -> None:
        """Called when this node becomes the leader."""
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
            nodes[node].put_nowait((int(time.monotonic()), b'DEAD'))
    
    def queue_gossip_update(
        self,
        update_type: UpdateType,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        """Queue a membership update for piggybacking on future messages."""
        n_members = self._get_member_count()
        self._gossip_buffer.add_update(update_type, node, incarnation, n_members)
    
    def get_piggyback_data(self, max_updates: int = 5) -> bytes:
        """Get piggybacked membership updates to append to a message."""
        return self._gossip_buffer.encode_piggyback(max_updates)
    
    def process_piggyback_data(self, data: bytes) -> None:
        """Process piggybacked membership updates received in a message."""
        updates = GossipBuffer.decode_piggyback(data)
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
                        self.start_suspicion(
                            update.node,
                            update.incarnation,
                            self_addr,
                        )
                elif update.update_type == 'alive':
                    self.refute_suspicion(update.node, update.incarnation)
                
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
    
    async def send_if_ok(
        self,
        node: tuple[str, int],
        message: bytes,
        include_piggyback: bool = True,
    ):
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        _, status = node[-1]
        if status == b'OK':
            if include_piggyback:
                message = message + self.get_piggyback_data()
            self._task_runner.run(
                self.send,
                node,
                message,
                timeout=timeout,
            )

    async def poll_node(self, target: tuple[str, int]):
        """Legacy single-node polling (deprecated, use start_probe_cycle instead)."""
        status: Status = await self._context.read_with_lock(target)
        while self._running and status == b'OK':
            await self.send_if_ok(target, b'ack>' + target)
            await asyncio.sleep(self._context.read('udp_poll_interval', 1))
            status = await self._context.read_with_lock(target)
    
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
        target = self._probe_scheduler.get_next_target()
        if target is None:
            return
        
        if self.udp_target_is_self(target):
            return
        
        node_state = self._incarnation_tracker.get_node_state(target)
        incarnation = node_state.incarnation if node_state else 0
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        target_addr = f'{target[0]}:{target[1]}'.encode()
        probe_msg = b'probe>' + target_addr + self.get_piggyback_data()
        
        try:
            response_received = await self._probe_with_timeout(target, probe_msg, timeout)
            
            if response_received:
                await self.decrease_failure_detector('successful_probe')
                return
            
            await self.increase_failure_detector('probe_timeout')
            indirect_sent = await self.initiate_indirect_probe(target, incarnation)
            
            if indirect_sent:
                await asyncio.sleep(timeout)
                probe = self._indirect_probe_manager.get_pending_probe(target)
                if probe and probe.is_completed():
                    await self.decrease_failure_detector('successful_probe')
                    return
            
            self_addr = self._get_self_udp_addr()
            self.start_suspicion(target, incarnation, self_addr)
            await self.broadcast_suspicion(target, incarnation)
            
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            # Probe timeout - handle specifically
            await self.handle_error(
                ProbeTimeoutError(target, timeout)
            )
        except Exception as e:
            await self.handle_exception(e, "probe_round")
    
    async def _probe_with_timeout(
        self, 
        target: tuple[str, int], 
        message: bytes,
        timeout: float,
    ) -> bool:
        """Send a probe message and wait for response."""
        try:
            self._task_runner.run(
                self.send,
                target,
                message,
                timeout=timeout,
            )
            await asyncio.sleep(timeout * 0.8)
            return False  # Simplified: always try indirect
        except asyncio.TimeoutError:
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
        """Get timeout adjusted by Local Health Multiplier."""
        return base_timeout * self._local_health.get_multiplier()
    
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
    
    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: Status,
    ) -> bool:
        """Check if a message about a node should be processed."""
        return self._incarnation_tracker.is_message_fresh(node, incarnation, status)
    
    def update_node_state(
        self,
        node: tuple[str, int],
        status: Status,
        incarnation: int,
        timestamp: float,
    ) -> bool:
        """Update the state of a node. Returns True if state changed."""
        return self._incarnation_tracker.update_node(node, status, incarnation, timestamp)
    
    def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> SuspicionState:
        """Start suspecting a node or add confirmation to existing suspicion."""
        self._incarnation_tracker.update_node(
            node,
            b'SUSPECT',
            incarnation,
            time.monotonic(),
        )
        return self._suspicion_manager.start_suspicion(node, incarnation, from_node)
    
    def confirm_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """Add a confirmation to an existing suspicion."""
        return self._suspicion_manager.confirm_suspicion(node, incarnation, from_node)
    
    def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """Refute a suspicion - the node proved it's alive."""
        if self._suspicion_manager.refute_suspicion(node, incarnation):
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
        """Initiate indirect probing for a target node."""
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
        
        target_addr = f'{target[0]}:{target[1]}'.encode()
        msg = b'ping-req:' + str(incarnation).encode() + b'>' + target_addr
        
        for proxy in proxies:
            probe.add_proxy(proxy)
            self._task_runner.run(
                self.send,
                proxy,
                msg,
                timeout=timeout,
            )
        
        return True
    
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
        """Broadcast an alive message to refute any suspicions about this node."""
        new_incarnation = self.increment_incarnation()
        
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        self_addr_bytes = f'{self_addr[0]}:{self_addr[1]}'.encode()
        msg = b'alive:' + str(new_incarnation).encode() + b'>' + self_addr_bytes
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        for node in nodes:
            if node != self_addr:
                self._task_runner.run(
                    self.send,
                    node,
                    msg,
                    timeout=timeout,
                )
        
        return new_incarnation
    
    async def broadcast_suspicion(
        self, 
        target: tuple[str, int], 
        incarnation: int,
    ) -> None:
        """Broadcast a suspicion about a node to all other members."""
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        target_addr_bytes = f'{target[0]}:{target[1]}'.encode()
        msg = b'suspect:' + str(incarnation).encode() + b'>' + target_addr_bytes
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        for node in nodes:
            if node != self_addr and node != target:
                self._task_runner.run(
                    self.send,
                    node,
                    msg,
                    timeout=timeout,
                )
    
    async def _send_to_addr(
        self, 
        target: tuple[str, int], 
        message: bytes,
        timeout: float | None = None,
    ) -> None:
        """Send a message to a specific address."""
        if timeout is None:
            base_timeout = self._context.read('current_timeout')
            timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        await self.send(target, message, timeout=timeout)
    
    async def _send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        """Send a probe to target and wait for response."""
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        target_addr = f'{target[0]}:{target[1]}'.encode()
        msg = b'probe>' + target_addr
        
        try:
            response = await asyncio.wait_for(
                self._send_and_receive(target, msg),
                timeout=timeout,
            )
            return response and b'ack' in response
        except (asyncio.TimeoutError, Exception):
            return False
    
    async def _send_and_receive(
        self, 
        target: tuple[str, int], 
        message: bytes,
    ) -> bytes | None:
        """Send a message and wait for a response."""
        response_future: asyncio.Future[bytes] = asyncio.get_event_loop().create_future()
        
        self._task_runner.run(
            self.send,
            target,
            message,
            timeout=None,
        )
        
        try:
            await asyncio.sleep(0.1)
            return None
        except Exception:
            return None

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
            # Extract any piggybacked membership updates first
            piggyback_idx = data.find(b'|')
            if piggyback_idx > 0:
                main_data = data[:piggyback_idx]
                piggyback_data = data[piggyback_idx:]
                self.process_piggyback_data(piggyback_data)
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
                    nodes: Nodes = self._context.read('nodes')
                    if target not in nodes:
                        await self.increase_failure_detector('missed_nack')
                        return b'nack>' + self._udp_addr_slug
                    
                    await self.decrease_failure_detector('successful_nack')
                    return b'ack>' + self._udp_addr_slug
                
                case b'join':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'ack' + b'>' + self._udp_addr_slug
                        
                        self._context.write(target, b'OK')

                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                b'join>' + target_addr,
                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'OK'))
                        
                        self._probe_scheduler.add_member(target)
                        self._incarnation_tracker.update_node(target, b'OK', 0, time.monotonic())

                        return b'ack>' + self._udp_addr_slug

                case b'leave':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'leave>' + self._udp_addr_slug

                        if target not in nodes:
                            await self.increase_failure_detector('missed_nack')
                            return b'nack>' + self._udp_addr_slug
                        
                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,
                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'DEAD'))
                        self._context.write('nodes', nodes)

                        return b'ack>' + self._udp_addr_slug
                
                case b'probe':
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
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,
                            ) for node in others
                        ])
                            
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
                    msg_parts = message.split(b':', maxsplit=1)
                    msg_incarnation = 0
                    if len(msg_parts) > 1:
                        try:
                            msg_incarnation = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        if self.is_message_fresh(target, msg_incarnation, b'OK'):
                            self.refute_suspicion(target, msg_incarnation)
                            self.update_node_state(
                                target, 
                                b'OK', 
                                msg_incarnation, 
                                time.monotonic(),
                            )
                            await self.decrease_failure_detector('successful_probe')
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'suspect':
                    msg_parts = message.split(b':', maxsplit=1)
                    msg_incarnation = 0
                    if len(msg_parts) > 1:
                        try:
                            msg_incarnation = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        if self.udp_target_is_self(target):
                            await self.increase_failure_detector('refutation')
                            new_incarnation = await self.broadcast_refutation()
                            return b'alive:' + str(new_incarnation).encode() + b'>' + self._udp_addr_slug
                        
                        if self.is_message_fresh(target, msg_incarnation, b'SUSPECT'):
                            self.start_suspicion(target, msg_incarnation, addr)
                            
                            suspicion = self._suspicion_manager.get_suspicion(target)
                            if suspicion and suspicion.should_regossip():
                                suspicion.mark_regossiped()
                                await self.broadcast_suspicion(target, msg_incarnation)
                    
                    return b'ack>' + self._udp_addr_slug
                
                # Leadership messages
                case b'leader-claim':
                    msg_parts = message.split(b':', maxsplit=2)
                    term = 0
                    candidate_lhm = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    if len(msg_parts) >= 3:
                        try:
                            candidate_lhm = int(msg_parts[2].decode())
                        except ValueError:
                            pass
                    
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
                    msg_parts = message.split(b':', maxsplit=1)
                    term = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
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
                    msg_parts = message.split(b':', maxsplit=1)
                    term = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        self._leader_election.handle_elected(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-heartbeat':
                    msg_parts = message.split(b':', maxsplit=1)
                    term = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
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
                                asyncio.create_task(self._leader_election._step_down())
                        
                        self._leader_election.handle_heartbeat(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-stepdown':
                    msg_parts = message.split(b':', maxsplit=1)
                    term = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        self._leader_election.handle_stepdown(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'pre-vote-req':
                    msg_parts = message.split(b':', maxsplit=2)
                    term = 0
                    candidate_lhm = 0
                    if len(msg_parts) >= 3:
                        try:
                            term = int(msg_parts[1].decode())
                            candidate_lhm = int(msg_parts[2].decode())
                        except ValueError:
                            pass
                    
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
                    msg_parts = message.split(b':', maxsplit=2)
                    term = 0
                    granted = False
                    if len(msg_parts) >= 3:
                        try:
                            term = int(msg_parts[1].decode())
                            granted = msg_parts[2].decode() == '1'
                        except ValueError:
                            pass
                    
                    self._leader_election.handle_pre_vote_response(
                        voter=addr,
                        term=term,
                        granted=granted,
                    )
                    
                    return b'ack>' + self._udp_addr_slug
                    
                case _:
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

