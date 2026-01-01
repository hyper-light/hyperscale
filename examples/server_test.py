import asyncio
import math
import msgspec
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal, Callable
from pydantic import BaseModel, StrictStr
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.server import tcp, udp, task
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer

Message = Literal[
    b'ack', 
    b'nack', 
    b'join', 
    b'leave', 
    b'probe',
    b'ping-req',  # Indirect probe request (ask another node to probe target)
    b'ping-req-ack',  # Response from indirect probe
    b'suspect',  # Suspicion message
    b'alive',  # Refutation/alive message
    # Leadership messages
    b'leader-claim',     # Claim local leadership: leader-claim:term:lhm>addr
    b'leader-vote',      # Vote for candidate: leader-vote:term>candidate_addr
    b'leader-elected',   # Announce election win: leader-elected:term>leader_addr
    b'leader-heartbeat', # Leader heartbeat: leader-heartbeat:term>leader_addr
    b'leader-stepdown',  # Voluntary stepdown: leader-stepdown:term>addr
]
Status = Literal[b'JOIN', b'OK', b'SUSPECT', b'DEAD']
UpdateType = Literal['alive', 'suspect', 'dead', 'join', 'leave']
LeaderRole = Literal['follower', 'candidate', 'leader']

Nodes = dict[tuple[str, int], asyncio.Queue[tuple[int, Status]]]
Ctx = dict[Literal['nodes'], Nodes]


@dataclass
class LocalHealthMultiplier:
    """
    Lifeguard Local Health Multiplier (LHM).
    
    Tracks the node's own health state. A score of 0 indicates healthy,
    higher scores indicate potential issues with this node's ability
    to process messages in a timely manner.
    
    The score saturates at max_score to prevent unbounded growth.
    
    Events that increment LHM:
    - Missed nack (failed to respond in time)
    - Failed refutation (suspicion about self received)
    - Probe timeout when we initiated the probe
    
    Events that decrement LHM:
    - Successful probe round completion
    - Successful nack response received
    """
    score: int = 0
    max_score: int = 8  # Saturation limit 'S' from paper
    
    # Scoring weights for different events
    PROBE_TIMEOUT_PENALTY: int = 1
    REFUTATION_PENALTY: int = 2
    MISSED_NACK_PENALTY: int = 1
    SUCCESSFUL_PROBE_REWARD: int = 1
    SUCCESSFUL_NACK_REWARD: int = 1
    
    def increment(self, amount: int = 1) -> int:
        """
        Increment LHM score (node health is degrading).
        Returns the new score.
        """
        self.score = min(self.max_score, self.score + amount)
        return self.score
    
    def decrement(self, amount: int = 1) -> int:
        """
        Decrement LHM score (node health is improving).
        Returns the new score.
        """
        self.score = max(0, self.score - amount)
        return self.score
    
    def on_probe_timeout(self) -> int:
        """Called when a probe we sent times out."""
        return self.increment(self.PROBE_TIMEOUT_PENALTY)
    
    def on_refutation_needed(self) -> int:
        """Called when we receive a suspicion about ourselves."""
        return self.increment(self.REFUTATION_PENALTY)
    
    def on_missed_nack(self) -> int:
        """Called when we failed to respond in time."""
        return self.increment(self.MISSED_NACK_PENALTY)
    
    def on_successful_probe(self) -> int:
        """Called when a probe round completes successfully."""
        return self.decrement(self.SUCCESSFUL_PROBE_REWARD)
    
    def on_successful_nack(self) -> int:
        """Called when we successfully respond with a nack."""
        return self.decrement(self.SUCCESSFUL_NACK_REWARD)
    
    def get_multiplier(self) -> float:
        """
        Get the timeout multiplier based on current health score.
        Returns a value >= 1.0 that should multiply base timeouts.
        """
        # Linear scaling: healthy (0) = 1x, max unhealthy = 2x
        return 1.0 + (self.score / self.max_score)
    
    def is_healthy(self) -> bool:
        """Returns True if the node considers itself healthy."""
        return self.score == 0
    
    def reset(self) -> None:
        """Reset health score to 0 (healthy)."""
        self.score = 0


@dataclass
class NodeState:
    """
    Tracks the state of a known node in the SWIM membership.
    
    Includes status, incarnation number, and timing information
    for the suspicion subprotocol.
    """
    status: Status = b'OK'
    incarnation: int = 0
    last_update_time: float = 0.0
    
    def update(self, new_status: Status, new_incarnation: int, timestamp: float) -> bool:
        """
        Update node state if the new information is fresher.
        Returns True if the state was updated, False if ignored.
        
        Per SWIM protocol:
        - Higher incarnation always wins
        - Same incarnation: DEAD > SUSPECT > OK
        - Lower incarnation is always ignored
        """
        if new_incarnation > self.incarnation:
            self.status = new_status
            self.incarnation = new_incarnation
            self.last_update_time = timestamp
            return True
        elif new_incarnation == self.incarnation:
            # Same incarnation - apply status priority
            status_priority = {b'OK': 0, b'JOIN': 0, b'SUSPECT': 1, b'DEAD': 2}
            if status_priority.get(new_status, 0) > status_priority.get(self.status, 0):
                self.status = new_status
                self.last_update_time = timestamp
                return True
        return False


@dataclass 
class IncarnationTracker:
    """
    Tracks incarnation numbers for SWIM protocol.
    
    Each node maintains:
    - Its own incarnation number (incremented on refutation)
    - Known incarnation numbers for all other nodes
    
    Incarnation numbers are used to:
    - Order messages about the same node
    - Allow refutation of false suspicions
    - Prevent old messages from overriding newer state
    """
    self_incarnation: int = 0
    node_states: dict[tuple[str, int], NodeState] = field(default_factory=dict)
    
    def get_self_incarnation(self) -> int:
        """Get current incarnation number for this node."""
        return self.self_incarnation
    
    def increment_self_incarnation(self) -> int:
        """
        Increment own incarnation number.
        Called when refuting a suspicion about ourselves.
        Returns the new incarnation number.
        """
        self.self_incarnation += 1
        return self.self_incarnation
    
    def get_node_state(self, node: tuple[str, int]) -> NodeState | None:
        """Get the current state for a known node."""
        return self.node_states.get(node)
    
    def get_node_incarnation(self, node: tuple[str, int]) -> int:
        """Get the incarnation number for a node, or 0 if unknown."""
        state = self.node_states.get(node)
        return state.incarnation if state else 0
    
    def update_node(
        self, 
        node: tuple[str, int], 
        status: Status, 
        incarnation: int,
        timestamp: float,
    ) -> bool:
        """
        Update the state of a node.
        Returns True if the state was updated, False if the message was stale.
        """
        if node not in self.node_states:
            self.node_states[node] = NodeState(
                status=status,
                incarnation=incarnation,
                last_update_time=timestamp,
            )
            return True
        return self.node_states[node].update(status, incarnation, timestamp)
    
    def remove_node(self, node: tuple[str, int]) -> bool:
        """Remove a node from tracking. Returns True if it existed."""
        if node in self.node_states:
            del self.node_states[node]
            return True
        return False
    
    def get_all_nodes(self) -> list[tuple[tuple[str, int], NodeState]]:
        """Get all known nodes and their states."""
        return list(self.node_states.items())
    
    def is_message_fresh(
        self, 
        node: tuple[str, int], 
        incarnation: int, 
        status: Status,
    ) -> bool:
        """
        Check if a message about a node is fresh (should be processed).
        
        A message is fresh if:
        - We don't know about the node yet
        - It has a higher incarnation number
        - Same incarnation but higher priority status
        """
        state = self.node_states.get(node)
        if state is None:
            return True
        if incarnation > state.incarnation:
            return True
        if incarnation == state.incarnation:
            status_priority = {b'OK': 0, b'JOIN': 0, b'SUSPECT': 1, b'DEAD': 2}
            return status_priority.get(status, 0) > status_priority.get(state.status, 0)
        return False


@dataclass
class SuspicionState:
    """
    Tracks the suspicion state for a single node.
    
    Per Lifeguard paper, the suspicion timeout is dynamically calculated as:
    timeout = max(min_timeout, (max_timeout - min_timeout) * log(C+1) / log(N+1))
    
    Where:
    - C is the number of independent confirmations
    - N is the total number of members in the group
    
    The timeout decreases as more confirmations are received, but never
    goes below min_timeout.
    """
    node: tuple[str, int]
    incarnation: int
    start_time: float
    confirmers: set[tuple[str, int]] = field(default_factory=set)
    min_timeout: float = 1.0
    max_timeout: float = 10.0
    n_members: int = 1
    # Lifeguard re-gossip factor K: number of times to re-gossip suspicion
    regossip_factor: int = 3
    regossip_count: int = 0
    _timer_task: asyncio.Task | None = field(default=None, repr=False)
    
    def add_confirmation(self, from_node: tuple[str, int]) -> bool:
        """
        Add a confirmation from another node.
        Returns True if this is a new confirmation.
        """
        if from_node in self.confirmers:
            return False
        self.confirmers.add(from_node)
        return True
    
    @property
    def confirmation_count(self) -> int:
        """Number of independent confirmations received."""
        return len(self.confirmers)
    
    def calculate_timeout(self) -> float:
        """
        Calculate the current suspicion timeout based on confirmations.
        
        Uses the Lifeguard formula:
        timeout = max(min, (max - min) * log(C+1) / log(N+1))
        
        More confirmations = lower timeout (faster declaration of failure)
        """
        c = self.confirmation_count
        n = max(1, self.n_members)
        
        if n <= 1:
            return self.max_timeout
        
        # Lifeguard formula from the paper
        log_factor = math.log(c + 1) / math.log(n + 1)
        timeout = self.max_timeout - (self.max_timeout - self.min_timeout) * log_factor
        
        return max(self.min_timeout, timeout)
    
    def time_remaining(self) -> float:
        """Calculate time remaining before suspicion expires."""
        elapsed = time.monotonic() - self.start_time
        timeout = self.calculate_timeout()
        return max(0, timeout - elapsed)
    
    def is_expired(self) -> bool:
        """Check if the suspicion has expired (node should be marked DEAD)."""
        return self.time_remaining() <= 0
    
    def should_regossip(self) -> bool:
        """Check if we should re-gossip this suspicion."""
        return self.regossip_count < self.regossip_factor
    
    def mark_regossiped(self) -> None:
        """Mark that we've re-gossiped this suspicion."""
        self.regossip_count += 1
    
    def cancel_timer(self) -> None:
        """Cancel the expiration timer if running."""
        if self._timer_task and not self._timer_task.done():
            self._timer_task.cancel()
            self._timer_task = None


@dataclass
class SuspicionManager:
    """
    Manages suspicions for all nodes using the Lifeguard protocol.
    
    Key features:
    - Tracks active suspicions with confirmation counting
    - Calculates dynamic timeouts based on confirmations
    - Handles suspicion expiration and node death declaration
    - Supports refutation (clearing suspicion on higher incarnation)
    - Applies Local Health Multiplier to timeouts (Lifeguard)
    """
    suspicions: dict[tuple[str, int], SuspicionState] = field(default_factory=dict)
    min_timeout: float = 1.0
    max_timeout: float = 10.0
    _on_suspicion_expired: Callable[[tuple[str, int], int], None] | None = None
    _n_members_getter: Callable[[], int] | None = None
    _lhm_multiplier_getter: Callable[[], float] | None = None
    
    def set_callbacks(
        self,
        on_expired: Callable[[tuple[str, int], int], None],
        get_n_members: Callable[[], int],
        get_lhm_multiplier: Callable[[], float] | None = None,
    ) -> None:
        """Set callback functions for suspicion events."""
        self._on_suspicion_expired = on_expired
        self._n_members_getter = get_n_members
        self._lhm_multiplier_getter = get_lhm_multiplier
    
    def _get_lhm_multiplier(self) -> float:
        """Get the current LHM multiplier for timeout adjustment."""
        if self._lhm_multiplier_getter:
            return self._lhm_multiplier_getter()
        return 1.0
    
    def _get_n_members(self) -> int:
        """Get current member count."""
        if self._n_members_getter:
            return self._n_members_getter()
        return 1
    
    def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> SuspicionState:
        """
        Start or update a suspicion for a node.
        
        If suspicion already exists with same incarnation, add confirmation.
        If new suspicion or higher incarnation, create new suspicion state.
        
        Timeouts are adjusted by the Local Health Multiplier per Lifeguard.
        """
        existing = self.suspicions.get(node)
        
        if existing:
            if incarnation < existing.incarnation:
                # Stale suspicion message, ignore
                return existing
            elif incarnation == existing.incarnation:
                # Same suspicion, add confirmation
                existing.add_confirmation(from_node)
                # Recalculate timeout with new confirmation
                self._reschedule_timer(existing)
                return existing
            else:
                # Higher incarnation suspicion, replace
                existing.cancel_timer()
        
        # Apply LHM to timeouts - when we're unhealthy, extend timeouts
        # to reduce false positives caused by our own slow processing
        lhm_multiplier = self._get_lhm_multiplier()
        
        # Create new suspicion with LHM-adjusted timeouts
        state = SuspicionState(
            node=node,
            incarnation=incarnation,
            start_time=time.monotonic(),
            min_timeout=self.min_timeout * lhm_multiplier,
            max_timeout=self.max_timeout * lhm_multiplier,
            n_members=self._get_n_members(),
        )
        state.add_confirmation(from_node)
        self.suspicions[node] = state
        
        # Schedule expiration timer
        self._schedule_timer(state)
        
        return state
    
    def _schedule_timer(self, state: SuspicionState) -> None:
        """Schedule the expiration timer for a suspicion."""
        async def expire_suspicion():
            timeout = state.calculate_timeout()
            await asyncio.sleep(timeout)
            self._handle_expiration(state)
        
        state._timer_task = asyncio.create_task(expire_suspicion())
    
    def _reschedule_timer(self, state: SuspicionState) -> None:
        """Reschedule timer with updated timeout (after new confirmation)."""
        state.cancel_timer()
        remaining = state.time_remaining()
        if remaining > 0:
            async def expire_suspicion():
                await asyncio.sleep(remaining)
                self._handle_expiration(state)
            state._timer_task = asyncio.create_task(expire_suspicion())
        else:
            self._handle_expiration(state)
    
    def _handle_expiration(self, state: SuspicionState) -> None:
        """Handle suspicion expiration - declare node as DEAD."""
        if state.node in self.suspicions:
            del self.suspicions[state.node]
            if self._on_suspicion_expired:
                self._on_suspicion_expired(state.node, state.incarnation)
    
    def confirm_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """
        Add a confirmation to an existing suspicion.
        Returns True if the suspicion exists and confirmation was added.
        """
        state = self.suspicions.get(node)
        if state and state.incarnation == incarnation:
            if state.add_confirmation(from_node):
                self._reschedule_timer(state)
                return True
        return False
    
    def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """
        Refute a suspicion (node proved it's alive with higher incarnation).
        Returns True if a suspicion was cleared.
        """
        state = self.suspicions.get(node)
        if state and incarnation > state.incarnation:
            state.cancel_timer()
            del self.suspicions[node]
            return True
        return False
    
    def get_suspicion(self, node: tuple[str, int]) -> SuspicionState | None:
        """Get the current suspicion state for a node, if any."""
        return self.suspicions.get(node)
    
    def is_suspected(self, node: tuple[str, int]) -> bool:
        """Check if a node is currently suspected."""
        return node in self.suspicions
    
    def clear_all(self) -> None:
        """Clear all suspicions (e.g., on shutdown)."""
        for state in self.suspicions.values():
            state.cancel_timer()
        self.suspicions.clear()
    
    def get_suspicions_to_regossip(self) -> list[SuspicionState]:
        """Get suspicions that should be re-gossiped."""
        return [s for s in self.suspicions.values() if s.should_regossip()]


@dataclass
class PendingIndirectProbe:
    """
    Tracks a pending indirect probe request.
    
    When a direct probe to a target fails, we ask k other nodes to 
    probe the target on our behalf. This tracks those pending requests.
    """
    target: tuple[str, int]
    requester: tuple[str, int]
    start_time: float
    timeout: float
    proxies: set[tuple[str, int]] = field(default_factory=set)
    received_acks: int = 0
    _completed: bool = False
    
    def add_proxy(self, proxy: tuple[str, int]) -> None:
        """Add a proxy node that we asked to probe the target."""
        self.proxies.add(proxy)
    
    def record_ack(self) -> bool:
        """
        Record that we received an ack from one of the proxies.
        Returns True if this is the first ack (target is alive).
        """
        if self._completed:
            return False
        self.received_acks += 1
        if self.received_acks == 1:
            self._completed = True
            return True
        return False
    
    def is_expired(self) -> bool:
        """Check if the probe request has timed out."""
        return time.monotonic() - self.start_time > self.timeout
    
    def is_completed(self) -> bool:
        """Check if we've received an ack (probe succeeded)."""
        return self._completed


@dataclass
class IndirectProbeManager:
    """
    Manages indirect probe requests for SWIM protocol.
    
    When a direct probe to node B fails, node A asks k random other
    nodes to probe B on A's behalf. If any proxy gets a response from B,
    it forwards the ack back to A.
    
    This helps distinguish between:
    - B being actually failed
    - Network issues between A and B specifically
    """
    pending_probes: dict[tuple[str, int], PendingIndirectProbe] = field(default_factory=dict)
    # Number of proxy nodes to use for indirect probing
    k_proxies: int = 3
    
    def start_indirect_probe(
        self,
        target: tuple[str, int],
        requester: tuple[str, int],
        timeout: float,
    ) -> PendingIndirectProbe:
        """Start tracking an indirect probe request."""
        probe = PendingIndirectProbe(
            target=target,
            requester=requester,
            start_time=time.monotonic(),
            timeout=timeout,
        )
        self.pending_probes[target] = probe
        return probe
    
    def get_pending_probe(self, target: tuple[str, int]) -> PendingIndirectProbe | None:
        """Get the pending probe for a target, if any."""
        return self.pending_probes.get(target)
    
    def record_ack(self, target: tuple[str, int]) -> bool:
        """
        Record that the target responded to an indirect probe.
        Returns True if the probe was pending and this is the first ack.
        """
        probe = self.pending_probes.get(target)
        if probe and probe.record_ack():
            del self.pending_probes[target]
            return True
        return False
    
    def cancel_probe(self, target: tuple[str, int]) -> bool:
        """Cancel a pending probe (e.g., target confirmed dead)."""
        if target in self.pending_probes:
            del self.pending_probes[target]
            return True
        return False
    
    def get_expired_probes(self) -> list[PendingIndirectProbe]:
        """Get all probes that have timed out without an ack."""
        expired = []
        now = time.monotonic()
        to_remove = []
        for target, probe in self.pending_probes.items():
            if probe.is_expired():
                expired.append(probe)
                to_remove.append(target)
        for target in to_remove:
            del self.pending_probes[target]
        return expired
    
    def clear_all(self) -> None:
        """Clear all pending probes."""
        self.pending_probes.clear()


@dataclass
class PiggybackUpdate:
    """
    A membership update to be piggybacked on probe messages.
    
    In SWIM, membership updates are disseminated by "piggybacking" them
    onto the protocol messages (probes, acks). This achieves O(log n)
    dissemination without additional message overhead.
    """
    update_type: UpdateType
    node: tuple[str, int]
    incarnation: int
    timestamp: float
    # Number of times this update has been piggybacked
    broadcast_count: int = 0
    # Maximum number of times to piggyback (lambda * log(n))
    max_broadcasts: int = 10
    
    def should_broadcast(self) -> bool:
        """Check if this update should still be piggybacked."""
        return self.broadcast_count < self.max_broadcasts
    
    def mark_broadcast(self) -> None:
        """Mark that this update was piggybacked."""
        self.broadcast_count += 1
    
    def to_bytes(self) -> bytes:
        """Serialize update for transmission."""
        # Format: type:incarnation:host:port
        return (
            self.update_type.encode() + b':' +
            str(self.incarnation).encode() + b':' +
            self.node[0].encode() + b':' +
            str(self.node[1]).encode()
        )
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PiggybackUpdate | None':
        """Deserialize an update from bytes."""
        try:
            parts = data.decode().split(':')
            if len(parts) < 4:
                return None
            update_type = parts[0]
            incarnation = int(parts[1])
            host = parts[2]
            port = int(parts[3])
            return cls(
                update_type=update_type,
                node=(host, port),
                incarnation=incarnation,
                timestamp=time.monotonic(),
            )
        except (ValueError, UnicodeDecodeError):
            return None
    
    def __hash__(self) -> int:
        return hash((self.update_type, self.node, self.incarnation))
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PiggybackUpdate):
            return False
        return (
            self.update_type == other.update_type and
            self.node == other.node and
            self.incarnation == other.incarnation
        )


@dataclass
class GossipBuffer:
    """
    Buffer for membership updates to be piggybacked on messages.
    
    Maintains a priority queue of updates ordered by broadcast count,
    so that less-disseminated updates are sent first. Updates are
    removed after being broadcast lambda * log(n) times.
    """
    updates: dict[tuple[str, int], PiggybackUpdate] = field(default_factory=dict)
    # Multiplier for max broadcasts (lambda in the paper)
    broadcast_multiplier: int = 3
    
    def add_update(
        self,
        update_type: UpdateType,
        node: tuple[str, int],
        incarnation: int,
        n_members: int = 1,
    ) -> None:
        """
        Add or update a membership update in the buffer.
        
        If an update for the same node exists with lower incarnation,
        it is replaced. Updates with equal or higher incarnation are
        only replaced if the new status has higher priority.
        """
        # Calculate max broadcasts: lambda * log(n+1)
        max_broadcasts = max(1, int(
            self.broadcast_multiplier * math.log(n_members + 1)
        ))
        
        existing = self.updates.get(node)
        
        if existing is None:
            # New update
            self.updates[node] = PiggybackUpdate(
                update_type=update_type,
                node=node,
                incarnation=incarnation,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
        elif incarnation > existing.incarnation:
            # Higher incarnation replaces
            self.updates[node] = PiggybackUpdate(
                update_type=update_type,
                node=node,
                incarnation=incarnation,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
        elif incarnation == existing.incarnation:
            # Same incarnation - check status priority
            priority = {'alive': 0, 'join': 0, 'suspect': 1, 'dead': 2, 'leave': 2}
            if priority.get(update_type, 0) > priority.get(existing.update_type, 0):
                self.updates[node] = PiggybackUpdate(
                    update_type=update_type,
                    node=node,
                    incarnation=incarnation,
                    timestamp=time.monotonic(),
                    max_broadcasts=max_broadcasts,
                )
    
    def get_updates_to_piggyback(self, max_count: int = 5) -> list[PiggybackUpdate]:
        """
        Get updates to piggyback on the next message.
        
        Returns up to max_count updates, prioritizing those with
        the lowest broadcast count (least disseminated).
        """
        # Sort by broadcast count (ascending) to prioritize new updates
        candidates = sorted(
            [u for u in self.updates.values() if u.should_broadcast()],
            key=lambda u: u.broadcast_count,
        )
        
        return candidates[:max_count]
    
    def mark_broadcasts(self, updates: list[PiggybackUpdate]) -> None:
        """Mark updates as having been broadcast and remove if done."""
        for update in updates:
            if update.node in self.updates:
                self.updates[update.node].mark_broadcast()
                if not self.updates[update.node].should_broadcast():
                    del self.updates[update.node]
    
    def encode_piggyback(self, max_count: int = 5) -> bytes:
        """
        Get piggybacked updates as bytes to append to a message.
        Format: |update1|update2|update3
        """
        updates = self.get_updates_to_piggyback(max_count)
        if not updates:
            return b''
        
        self.mark_broadcasts(updates)
        return b'|' + b'|'.join(u.to_bytes() for u in updates)
    
    @staticmethod
    def decode_piggyback(data: bytes) -> list[PiggybackUpdate]:
        """
        Decode piggybacked updates from message suffix.
        """
        if not data or data[0:1] != b'|':
            return []
        
        updates = []
        parts = data[1:].split(b'|')
        for part in parts:
            if part:
                update = PiggybackUpdate.from_bytes(part)
                if update:
                    updates.append(update)
        return updates
    
    def clear(self) -> None:
        """Clear all pending updates."""
        self.updates.clear()


@dataclass
class ProbeScheduler:
    """
    Implements SWIM's randomized round-robin probing.
    
    In SWIM, members are probed in a randomized round-robin fashion:
    1. Shuffle the member list
    2. Probe each member in sequence
    3. When exhausted, reshuffle and repeat
    
    This ensures:
    - Each member is probed within a bounded time window
    - Probing is unpredictable (helps with network partition handling)
    - Even load distribution across members
    """
    members: list[tuple[str, int]] = field(default_factory=list)
    probe_index: int = 0
    protocol_period: float = 1.0  # Time between probes in seconds
    _running: bool = False
    _probe_task: asyncio.Task | None = field(default=None, repr=False)
    
    def update_members(self, members: list[tuple[str, int]]) -> None:
        """
        Update the member list and reshuffle.
        Called when membership changes.
        """
        self.members = list(members)
        random.shuffle(self.members)
        # Reset index if it's now out of bounds
        if self.probe_index >= len(self.members):
            self.probe_index = 0
    
    def get_next_target(self) -> tuple[str, int] | None:
        """
        Get the next member to probe.
        Returns None if no members available.
        """
        if not self.members:
            return None
        
        # If we've probed everyone, reshuffle
        if self.probe_index >= len(self.members):
            random.shuffle(self.members)
            self.probe_index = 0
        
        target = self.members[self.probe_index]
        self.probe_index += 1
        return target
    
    def remove_member(self, member: tuple[str, int]) -> None:
        """Remove a member from the probe list (e.g., when declared dead)."""
        if member in self.members:
            # Adjust index if needed
            idx = self.members.index(member)
            self.members.remove(member)
            if idx < self.probe_index:
                self.probe_index = max(0, self.probe_index - 1)
    
    def add_member(self, member: tuple[str, int]) -> None:
        """Add a new member to the probe list."""
        if member not in self.members:
            # Insert at random position for unpredictability
            if self.members:
                insert_idx = random.randint(0, len(self.members))
                self.members.insert(insert_idx, member)
                # Adjust probe index if we inserted before it
                if insert_idx <= self.probe_index:
                    self.probe_index += 1
            else:
                self.members.append(member)
    
    def get_probe_cycle_time(self) -> float:
        """
        Calculate time to complete one full probe cycle.
        This is the maximum time before a failure is detected.
        """
        return len(self.members) * self.protocol_period
    
    def stop(self) -> None:
        """Stop the probe scheduler."""
        self._running = False
        if self._probe_task and not self._probe_task.done():
            self._probe_task.cancel()
            self._probe_task = None


@dataclass
class LeaderEligibility:
    """
    Determines if a node can become or remain a leader.
    
    Integrates with Lifeguard's Local Health Multiplier (LHM) to ensure
    that overloaded nodes do not become leaders. This is critical for
    multi-datacenter deployments where nodes may consume high CPU/memory.
    
    A node is eligible for leadership if:
    1. Its status is ALIVE (not SUSPECT or DEAD)
    2. Its LHM score is below the threshold
    3. It is not currently suspected by others
    """
    # Maximum LHM score for a node to be eligible as leader
    # LHM ranges from 0 (healthy) to max_score (8 by default)
    max_leader_lhm: int = 2
    
    # Minimum members required for election (quorum)
    min_members_for_election: int = 1
    
    def is_eligible(
        self, 
        lhm_score: int, 
        status: Status,
        is_suspected: bool = False,
    ) -> bool:
        """Check if a node with given state can become leader."""
        return (
            status == b'OK' and
            lhm_score <= self.max_leader_lhm and
            not is_suspected
        )
    
    def should_step_down(self, current_lhm: int) -> bool:
        """
        Check if current leader should voluntarily step down.
        Called when leader's LHM increases due to load.
        """
        return current_lhm > self.max_leader_lhm
    
    def get_leader_priority(
        self, 
        node: tuple[str, int], 
        incarnation: int, 
        lhm_score: int,
    ) -> tuple[int, int, str, int]:
        """
        Get priority tuple for leader selection.
        Lower tuple = higher priority for leadership.
        
        Priority order:
        1. Lower LHM score (healthier nodes preferred)
        2. Higher incarnation (more "proven" nodes)
        3. Lower address (deterministic tie-breaker)
        """
        return (lhm_score, -incarnation, node[0], node[1])


@dataclass
class LeaderState:
    """
    Tracks the leadership state for a node.
    
    Implements a simplified Raft-like state machine:
    - FOLLOWER: Not a leader, following current leader
    - CANDIDATE: Running for election
    - LEADER: Currently the leader, must send heartbeats
    
    Integrates with Lifeguard for health-aware leadership.
    """
    role: LeaderRole = 'follower'
    current_term: int = 0
    
    # Current known leader (None if no leader)
    current_leader: tuple[str, int] | None = None
    leader_term: int = 0
    
    # Lease tracking
    leader_lease_start: float = 0.0
    lease_duration: float = 5.0  # Seconds
    
    # Election state
    votes_received: set[tuple[str, int]] = field(default_factory=set)
    voted_for: tuple[str, int] | None = None
    voted_in_term: int = -1
    election_timeout: float = 10.0  # Seconds
    last_heartbeat_time: float = 0.0
    
    # Callbacks (set by owner)
    _on_become_leader: Callable[[], None] | None = None
    _on_lose_leadership: Callable[[], None] | None = None
    _on_leader_change: Callable[[tuple[str, int] | None], None] | None = None
    
    def set_callbacks(
        self,
        on_become_leader: Callable[[], None] | None = None,
        on_lose_leadership: Callable[[], None] | None = None,
        on_leader_change: Callable[[tuple[str, int] | None], None] | None = None,
    ) -> None:
        """Set callback functions for leadership events."""
        self._on_become_leader = on_become_leader
        self._on_lose_leadership = on_lose_leadership
        self._on_leader_change = on_leader_change
    
    def is_leader(self) -> bool:
        """Check if this node is currently the leader."""
        return self.role == 'leader'
    
    def is_follower(self) -> bool:
        """Check if this node is currently a follower."""
        return self.role == 'follower'
    
    def is_candidate(self) -> bool:
        """Check if this node is currently a candidate."""
        return self.role == 'candidate'
    
    def has_leader(self) -> bool:
        """Check if there's a known leader."""
        return self.current_leader is not None and self.is_lease_valid()
    
    def is_lease_valid(self) -> bool:
        """Check if the current leader's lease is still valid."""
        if self.current_leader is None:
            return False
        return time.monotonic() < self.leader_lease_start + self.lease_duration
    
    def time_until_lease_expiry(self) -> float:
        """Get time until leader lease expires."""
        expiry = self.leader_lease_start + self.lease_duration
        return max(0, expiry - time.monotonic())
    
    def should_start_election(self) -> bool:
        """Check if we should start a new election."""
        if self.role == 'leader':
            return False
        return not self.is_lease_valid()
    
    def start_election(self, new_term: int) -> None:
        """Transition to candidate state and start election."""
        self.role = 'candidate'
        self.current_term = new_term
        self.votes_received.clear()
        self.voted_for = None
        self.voted_in_term = -1
    
    def record_vote(self, voter: tuple[str, int]) -> int:
        """Record a vote received. Returns total vote count."""
        self.votes_received.add(voter)
        return len(self.votes_received)
    
    def become_leader(self, term: int) -> None:
        """Transition to leader state."""
        was_leader = self.role == 'leader'
        self.role = 'leader'
        self.current_term = term
        self.leader_term = term
        self.leader_lease_start = time.monotonic()
        self.current_leader = None  # We are the leader, set by caller
        
        if not was_leader and self._on_become_leader:
            self._on_become_leader()
    
    def become_follower(self, term: int, leader: tuple[str, int] | None = None) -> None:
        """Transition to follower state."""
        was_leader = self.role == 'leader'
        old_leader = self.current_leader
        
        self.role = 'follower'
        self.current_term = max(self.current_term, term)
        self.votes_received.clear()
        
        if leader:
            self.current_leader = leader
            self.leader_term = term
            self.leader_lease_start = time.monotonic()
        
        if was_leader and self._on_lose_leadership:
            self._on_lose_leadership()
        
        if leader != old_leader and self._on_leader_change:
            self._on_leader_change(leader)
    
    def update_heartbeat(self, leader: tuple[str, int], term: int) -> None:
        """Update lease on receiving leader heartbeat."""
        if term >= self.leader_term:
            self.current_leader = leader
            self.leader_term = term
            self.leader_lease_start = time.monotonic()
            self.last_heartbeat_time = time.monotonic()
            
            if self.role != 'follower':
                self.become_follower(term, leader)
    
    def renew_lease(self) -> None:
        """Renew leader lease (called by leader on heartbeat send)."""
        if self.role == 'leader':
            self.leader_lease_start = time.monotonic()
    
    def can_vote_for(self, candidate: tuple[str, int], term: int) -> bool:
        """Check if we can vote for a candidate."""
        # Can't vote if already voted in this term for someone else
        if self.voted_in_term == term and self.voted_for != candidate:
            return False
        # Can only vote for higher or equal term
        return term >= self.current_term
    
    def vote_for(self, candidate: tuple[str, int], term: int) -> None:
        """Record that we voted for a candidate."""
        self.voted_for = candidate
        self.voted_in_term = term
        self.current_term = max(self.current_term, term)


@dataclass
class LocalLeaderElection:
    """
    Manages local (within-datacenter) leader election.
    
    Uses a lease-based approach with LHM-aware eligibility:
    1. Nodes monitor leader lease expiry
    2. When lease expires, eligible nodes can become candidates
    3. Candidates request votes from peers
    4. First candidate with majority wins
    5. Leader sends periodic heartbeats to renew lease
    
    This is designed for low-latency, single-datacenter operation.
    """
    state: LeaderState = field(default_factory=LeaderState)
    eligibility: LeaderEligibility = field(default_factory=LeaderEligibility)
    
    # Configuration
    heartbeat_interval: float = 2.0  # Seconds between leader heartbeats
    election_timeout_base: float = 5.0  # Base election timeout
    election_timeout_jitter: float = 2.0  # Random jitter added to timeout
    
    # Datacenter identification
    dc_id: str = "default"
    
    # Reference to node address (set by owner)
    self_addr: tuple[str, int] | None = None
    
    # Callbacks for sending messages (set by owner)
    _broadcast_message: Callable[[bytes], None] | None = None
    _get_member_count: Callable[[], int] | None = None
    _get_lhm_score: Callable[[], int] | None = None
    
    # Background tasks
    _heartbeat_task: asyncio.Task | None = field(default=None, repr=False)
    _election_task: asyncio.Task | None = field(default=None, repr=False)
    _running: bool = False
    
    def set_callbacks(
        self,
        broadcast_message: Callable[[bytes], None],
        get_member_count: Callable[[], int],
        get_lhm_score: Callable[[], int],
        self_addr: tuple[str, int],
    ) -> None:
        """Set callback functions for election operations."""
        self._broadcast_message = broadcast_message
        self._get_member_count = get_member_count
        self._get_lhm_score = get_lhm_score
        self.self_addr = self_addr
    
    def get_election_timeout(self) -> float:
        """Get randomized election timeout."""
        jitter = random.uniform(0, self.election_timeout_jitter)
        return self.election_timeout_base + jitter
    
    def is_self_eligible(self) -> bool:
        """Check if this node is eligible to become leader."""
        if not self._get_lhm_score:
            return True
        lhm = self._get_lhm_score()
        return self.eligibility.is_eligible(lhm, b'OK', False)
    
    def should_step_down(self) -> bool:
        """Check if leader should step down due to high load."""
        if not self.state.is_leader():
            return False
        if not self._get_lhm_score:
            return False
        return self.eligibility.should_step_down(self._get_lhm_score())
    
    async def start(self) -> None:
        """Start the leader election process."""
        self._running = True
        self._election_task = asyncio.create_task(self._election_loop())
    
    async def stop(self) -> None:
        """Stop the leader election process."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
            self._election_task = None
    
    async def _election_loop(self) -> None:
        """Main election monitoring loop."""
        while self._running:
            try:
                if self.state.is_leader():
                    # Leader: check if we should step down
                    if self.should_step_down():
                        await self._step_down()
                    else:
                        await asyncio.sleep(self.heartbeat_interval)
                        await self._send_heartbeat()
                
                elif self.state.should_start_election():
                    # No leader or lease expired: maybe start election
                    if self.is_self_eligible():
                        await self._run_election()
                    else:
                        # Not eligible, wait for someone else
                        await asyncio.sleep(self.get_election_timeout())
                
                else:
                    # Following a leader, wait for lease to expire
                    wait_time = self.state.time_until_lease_expiry()
                    await asyncio.sleep(min(wait_time + 0.5, self.heartbeat_interval))
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                import traceback
                print(f"Election loop error: {traceback.format_exc()}")
                await asyncio.sleep(1)
    
    async def _run_election(self) -> None:
        """Run a leader election."""
        if not self.self_addr or not self._broadcast_message:
            return
        
        # Start new term
        new_term = self.state.current_term + 1
        self.state.start_election(new_term)
        
        # Vote for self
        self.state.vote_for(self.self_addr, new_term)
        self.state.record_vote(self.self_addr)
        
        # Broadcast claim
        lhm = self._get_lhm_score() if self._get_lhm_score else 0
        claim_msg = (
            b'leader-claim:' + 
            str(new_term).encode() + b':' +
            str(lhm).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        self._broadcast_message(claim_msg)
        
        # Wait for votes
        await asyncio.sleep(self.get_election_timeout())
        
        # Check if we won
        if self.state.role == 'candidate':  # Still candidate
            n_members = self._get_member_count() if self._get_member_count else 1
            votes_needed = (n_members // 2) + 1
            
            if len(self.state.votes_received) >= votes_needed:
                # We won!
                self.state.become_leader(new_term)
                self.state.current_leader = self.self_addr
                
                # Announce victory
                elected_msg = (
                    b'leader-elected:' +
                    str(new_term).encode() + b'>' +
                    f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
                )
                self._broadcast_message(elected_msg)
                
                # Start heartbeating
                await self._send_heartbeat()
    
    async def _send_heartbeat(self) -> None:
        """Send leader heartbeat."""
        if not self.state.is_leader() or not self.self_addr or not self._broadcast_message:
            return
        
        self.state.renew_lease()
        
        heartbeat_msg = (
            b'leader-heartbeat:' +
            str(self.state.current_term).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        self._broadcast_message(heartbeat_msg)
    
    async def _step_down(self) -> None:
        """Voluntarily step down from leadership."""
        if not self.state.is_leader() or not self.self_addr or not self._broadcast_message:
            return
        
        stepdown_msg = (
            b'leader-stepdown:' +
            str(self.state.current_term).encode() + b'>' +
            f'{self.self_addr[0]}:{self.self_addr[1]}'.encode()
        )
        self._broadcast_message(stepdown_msg)
        
        self.state.become_follower(self.state.current_term)
    
    def handle_claim(
        self,
        candidate: tuple[str, int],
        term: int,
        candidate_lhm: int,
    ) -> bytes | None:
        """
        Handle a leader-claim message.
        Returns vote message if we vote for the candidate, None otherwise.
        """
        if not self.self_addr:
            return None
        
        # Ignore claims from lower terms
        if term < self.state.current_term:
            return None
        
        # Check if candidate is eligible (based on their LHM)
        if not self.eligibility.is_eligible(candidate_lhm, b'OK', False):
            return None
        
        # Check if we can vote for them
        if not self.state.can_vote_for(candidate, term):
            return None
        
        # Vote for the candidate
        self.state.vote_for(candidate, term)
        
        vote_msg = (
            b'leader-vote:' +
            str(term).encode() + b'>' +
            f'{candidate[0]}:{candidate[1]}'.encode()
        )
        return vote_msg
    
    def handle_vote(self, voter: tuple[str, int], term: int) -> bool:
        """
        Handle a leader-vote message.
        Returns True if this vote wins the election.
        """
        if term != self.state.current_term or not self.state.is_candidate():
            return False
        
        vote_count = self.state.record_vote(voter)
        n_members = self._get_member_count() if self._get_member_count else 1
        votes_needed = (n_members // 2) + 1
        
        return vote_count >= votes_needed
    
    def handle_elected(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-elected message."""
        if term >= self.state.current_term:
            self.state.become_follower(term, leader)
    
    def handle_heartbeat(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-heartbeat message."""
        self.state.update_heartbeat(leader, term)
    
    def handle_stepdown(self, leader: tuple[str, int], term: int) -> None:
        """Handle a leader-stepdown message."""
        if leader == self.state.current_leader:
            self.state.current_leader = None
            # Will trigger election on next loop iteration
    
    def get_current_leader(self) -> tuple[str, int] | None:
        """Get the current leader, if any."""
        if self.state.is_leader() and self.self_addr:
            return self.self_addr
        return self.state.current_leader if self.state.is_lease_valid() else None
    
    def get_status(self) -> dict:
        """Get current leadership status for debugging."""
        return {
            'role': self.state.role,
            'term': self.state.current_term,
            'leader': self.get_current_leader(),
            'lease_remaining': self.state.time_until_lease_expiry(),
            'eligible': self.is_self_eligible(),
            'votes': len(self.state.votes_received) if self.state.is_candidate() else 0,
        }


class TestServer(MercurySyncBaseServer[Ctx]):

    def __init__(self, *args, dc_id: str = "default", **kwargs):
        super().__init__(*args, **kwargs)
        self._local_health = LocalHealthMultiplier()
        self._incarnation_tracker = IncarnationTracker()
        self._suspicion_manager = SuspicionManager()
        self._indirect_probe_manager = IndirectProbeManager()
        self._gossip_buffer = GossipBuffer()
        self._probe_scheduler = ProbeScheduler()
        self._leader_election = LocalLeaderElection(dc_id=dc_id)
        
        # Set up suspicion manager callbacks
        self._suspicion_manager.set_callbacks(
            on_expired=self._on_suspicion_expired,
            get_n_members=self._get_member_count,
            get_lhm_multiplier=self._get_lhm_multiplier,
        )
    
    def _get_lhm_multiplier(self) -> float:
        """Get the current LHM timeout multiplier."""
        return self._local_health.get_multiplier()
    
    def _setup_leader_election(self) -> None:
        """Initialize leader election callbacks after server is started."""
        self._leader_election.set_callbacks(
            broadcast_message=self._broadcast_leadership_message,
            get_member_count=self._get_member_count,
            get_lhm_score=lambda: self._local_health.score,
            self_addr=self._get_self_udp_addr(),
        )
        
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
        print(f"[{self._udp_addr_slug.decode()}] Became LEADER (term {self._leader_election.state.current_term})")
    
    def _on_lose_leadership(self) -> None:
        """Called when this node loses leadership."""
        print(f"[{self._udp_addr_slug.decode()}] Lost leadership")
    
    def _on_leader_change(self, new_leader: tuple[str, int] | None) -> None:
        """Called when the known leader changes."""
        if new_leader:
            print(f"[{self._udp_addr_slug.decode()}] New leader: {new_leader[0]}:{new_leader[1]}")
        else:
            print(f"[{self._udp_addr_slug.decode()}] No leader currently")
    
    def _get_member_count(self) -> int:
        """Get the current number of known members."""
        nodes = self._context.read('nodes')
        return len(nodes) if nodes else 1
    
    def _on_suspicion_expired(self, node: tuple[str, int], incarnation: int) -> None:
        """
        Callback when a suspicion expires - mark node as DEAD.
        This is called by the SuspicionManager when the timeout elapses.
        """
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
        """
        Queue a membership update for piggybacking on future messages.
        
        Updates are disseminated by being attached to probe/ack messages
        until they have been broadcast lambda * log(n) times.
        """
        n_members = self._get_member_count()
        self._gossip_buffer.add_update(update_type, node, incarnation, n_members)
    
    def get_piggyback_data(self, max_updates: int = 5) -> bytes:
        """
        Get piggybacked membership updates to append to a message.
        Returns encoded bytes ready to append to message.
        """
        return self._gossip_buffer.encode_piggyback(max_updates)
    
    def process_piggyback_data(self, data: bytes) -> None:
        """
        Process piggybacked membership updates received in a message.
        Applies each update to local state if fresh.
        """
        updates = GossipBuffer.decode_piggyback(data)
        for update in updates:
            # Check if the update is fresh
            status_map = {
                'alive': b'OK',
                'join': b'OK', 
                'suspect': b'SUSPECT',
                'dead': b'DEAD',
                'leave': b'DEAD',
            }
            status = status_map.get(update.update_type, b'OK')
            
            if self.is_message_fresh(update.node, update.incarnation, status):
                # Apply the update
                self.update_node_state(
                    update.node,
                    status,
                    update.incarnation,
                    update.timestamp,
                )
                
                # If it's a suspicion, start/confirm it
                if update.update_type == 'suspect':
                    self_addr = self._get_self_udp_addr()
                    if update.node == self_addr:
                        # We're being suspected - this will trigger refutation
                        # in the message handler
                        pass
                    else:
                        # Someone else is suspected
                        self.start_suspicion(
                            update.node,
                            update.incarnation,
                            self_addr,  # We're the confirmer
                        )
                elif update.update_type == 'alive':
                    # Clear any suspicion
                    self.refute_suspicion(update.node, update.incarnation)
                
                # Re-queue for further dissemination
                self.queue_gossip_update(
                    update.update_type,
                    update.node,
                    update.incarnation,
                )

    def get_other_nodes(self, node: tuple[str, int]):
        target_host, target_port = node
        nodes: Nodes = self._context.read('nodes')

        return [
            (
                host,
                port,
            ) for host, port in nodes if target_host != host and target_port != port
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
            # Append piggybacked membership updates if enabled
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
            await self.send_if_ok(
                target, 
                b'ack>' + target,
            )

            await asyncio.sleep(
                self._context.read('udp_poll_interval', 1)
            )

            status = await self._context.read_with_lock(target)
    
    async def start_probe_cycle(self) -> None:
        """
        Start the SWIM randomized round-robin probe cycle.
        
        This is the main failure detection loop that:
        1. Gets the next member to probe from the scheduler
        2. Sends a probe and waits for response
        3. If no response, initiates indirect probing
        4. If still no response, starts suspicion
        5. Repeats after protocol_period
        """
        self._probe_scheduler._running = True
        
        # Initialize probe scheduler with current members
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
                import traceback
                print(f"Probe cycle error: {traceback.format_exc()}")
            
            # Wait for next protocol period
            await asyncio.sleep(protocol_period)
    
    async def _run_probe_round(self) -> None:
        """
        Execute a single probe round in the SWIM protocol.
        
        Steps:
        1. Select next target using round-robin
        2. Send direct probe
        3. If timeout, try indirect probing via k random nodes
        4. If still no response, start/confirm suspicion
        """
        # Get next target
        target = self._probe_scheduler.get_next_target()
        if target is None:
            return
        
        # Skip if target is us
        if self.udp_target_is_self(target):
            return
        
        # Get current incarnation for the target
        node_state = self._incarnation_tracker.get_node_state(target)
        incarnation = node_state.incarnation if node_state else 0
        
        # Get LHM-adjusted timeout
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        # Send direct probe
        target_addr = f'{target[0]}:{target[1]}'.encode()
        probe_msg = b'probe>' + target_addr + self.get_piggyback_data()
        
        try:
            # Try direct probe first
            response_received = await self._probe_with_timeout(target, probe_msg, timeout)
            
            if response_received:
                # Success - node is alive
                await self.decrease_failure_detector('successful_probe')
                return
            
            # Direct probe failed - try indirect probing
            await self.increase_failure_detector('probe_timeout')
            
            # Initiate indirect probing through k random nodes
            indirect_sent = await self.initiate_indirect_probe(target, incarnation)
            
            if indirect_sent:
                # Wait for indirect probe responses
                await asyncio.sleep(timeout)
                
                # Check if any proxy got a response
                probe = self._indirect_probe_manager.get_pending_probe(target)
                if probe and probe.is_completed():
                    # Indirect probe succeeded - node is alive
                    await self.decrease_failure_detector('successful_probe')
                    return
            
            # Both direct and indirect probes failed - start suspicion
            self_addr = self._get_self_udp_addr()
            self.start_suspicion(target, incarnation, self_addr)
            
            # Broadcast the suspicion
            await self.broadcast_suspicion(target, incarnation)
            
        except asyncio.CancelledError:
            raise
        except Exception:
            import traceback
            print(f"Probe round error: {traceback.format_exc()}")
    
    async def _probe_with_timeout(
        self, 
        target: tuple[str, int], 
        message: bytes,
        timeout: float,
    ) -> bool:
        """
        Send a probe message and wait for response.
        Returns True if we received a response, False on timeout.
        """
        # In a real implementation, this would track the pending request
        # and wait for the corresponding response. For now, we use
        # a simplified version that sends and waits.
        try:
            self._task_runner.run(
                self.send,
                target,
                message,
                timeout=timeout,
            )
            # Give time for response (simplified - real impl would use futures)
            await asyncio.sleep(timeout * 0.8)
            # Check if we got an ack (would need response tracking)
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
        # Setup callbacks (needs server to be started)
        self._setup_leader_election()
        
        # Start the election loop
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
        """
        Increase local health score based on event type.
        Uses the Local Health Multiplier (LHM) from Lifeguard.
        """
        if event_type == 'probe_timeout':
            self._local_health.on_probe_timeout()
        elif event_type == 'refutation':
            self._local_health.on_refutation_needed()
        elif event_type == 'missed_nack':
            self._local_health.on_missed_nack()
        else:
            self._local_health.increment()

    async def decrease_failure_detector(self, event_type: str = 'successful_probe'):
        """
        Decrease local health score based on event type.
        Uses the Local Health Multiplier (LHM) from Lifeguard.
        """
        if event_type == 'successful_probe':
            self._local_health.on_successful_probe()
        elif event_type == 'successful_nack':
            self._local_health.on_successful_nack()
        else:
            self._local_health.decrement()
    
    def get_lhm_adjusted_timeout(self, base_timeout: float) -> float:
        """
        Get timeout adjusted by Local Health Multiplier.
        When node is unhealthy, timeouts are extended to reduce false positives.
        """
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
        """
        Encode a SWIM message with incarnation number.
        Format: msg_type:incarnation>target_host:target_port
        """
        inc = incarnation if incarnation is not None else self.get_self_incarnation()
        msg = msg_type + b':' + str(inc).encode()
        if target:
            msg += b'>' + f'{target[0]}:{target[1]}'.encode()
        return msg
    
    def decode_message_with_incarnation(
        self, 
        data: bytes,
    ) -> tuple[bytes, int, tuple[str, int] | None]:
        """
        Decode a SWIM message with incarnation number.
        Returns: (msg_type, incarnation, target or None)
        """
        # Split on '>' first to separate message from target
        parts = data.split(b'>', maxsplit=1)
        msg_part = parts[0]
        
        target = None
        if len(parts) > 1:
            target_str = parts[1].decode()
            host, port = target_str.split(':', maxsplit=1)
            target = (host, int(port))
        
        # Split message part to get type and incarnation
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
        """
        Start suspecting a node or add confirmation to existing suspicion.
        Uses Lifeguard's dynamic timeout based on confirmation count.
        """
        # Update node state to SUSPECT
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
        """
        Refute a suspicion - the node proved it's alive.
        Returns True if a suspicion was cleared.
        """
        if self._suspicion_manager.refute_suspicion(node, incarnation):
            # Update node state back to OK
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
        """
        Get k random nodes to use as proxies for indirect probing.
        Excludes self and the target node.
        """
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        # Get all OK nodes except self and target
        candidates = [
            node for node, queue in nodes.items()
            if node != target and node != self_addr
        ]
        
        # Return up to k random candidates
        k = min(k, len(candidates))
        if k <= 0:
            return []
        return random.sample(candidates, k)
    
    def _get_self_udp_addr(self) -> tuple[str, int]:
        """Get this server's UDP address as a tuple."""
        # Parse from _udp_addr_slug which is formatted as "host:port"
        host, port = self._udp_addr_slug.decode().split(':')
        return (host, int(port))
    
    async def initiate_indirect_probe(
        self,
        target: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """
        Initiate indirect probing for a target node.
        
        Called when a direct probe times out. Asks k random nodes
        to probe the target on our behalf.
        
        Returns True if we sent at least one indirect probe request.
        """
        k = self._indirect_probe_manager.k_proxies
        proxies = self.get_random_proxy_nodes(target, k)
        
        if not proxies:
            # No proxies available, go straight to suspicion
            return False
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        # Start tracking the indirect probe
        probe = self._indirect_probe_manager.start_indirect_probe(
            target=target,
            requester=self._get_self_udp_addr(),
            timeout=timeout,
        )
        
        # Send ping-req to each proxy
        # Format: ping-req:incarnation>target_host:target_port
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
        """
        Handle response from an indirect probe.
        
        Called when we receive a ping-req-ack indicating whether
        the target responded to the proxy's probe.
        """
        if is_alive:
            # Target is alive - cancel any pending suspicion
            if self._indirect_probe_manager.record_ack(target):
                await self.decrease_failure_detector('successful_probe')
        # If not alive, we wait for the probe to expire
    
    async def broadcast_refutation(self) -> int:
        """
        Broadcast an alive message to refute any suspicions about this node.
        
        Per SWIM/Lifeguard protocol:
        1. Increment our incarnation number
        2. Broadcast alive message with new incarnation to all nodes
        
        Returns the new incarnation number.
        """
        # Increment incarnation to override any suspicion messages
        new_incarnation = self.increment_incarnation()
        
        # Get all known nodes to broadcast to
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        # Format: alive:incarnation>self_host:self_port
        self_addr_bytes = f'{self_addr[0]}:{self_addr[1]}'.encode()
        msg = b'alive:' + str(new_incarnation).encode() + b'>' + self_addr_bytes
        
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        # Broadcast to all known nodes
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
        """
        Broadcast a suspicion about a node to all other members.
        
        Per SWIM protocol, suspicions are gossiped to spread 
        the information and gather confirmations.
        """
        nodes: Nodes = self._context.read('nodes')
        self_addr = self._get_self_udp_addr()
        
        # Format: suspect:incarnation>target_host:target_port
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
    
    async def _send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        """
        Send a probe to target and wait for response.
        Returns True if target responds with ack, False otherwise.
        
        This is a simplified probe used for indirect probing where
        we need to synchronously wait for a response.
        """
        base_timeout = self._context.read('current_timeout')
        timeout = self.get_lhm_adjusted_timeout(base_timeout)
        
        target_addr = f'{target[0]}:{target[1]}'.encode()
        msg = b'probe>' + target_addr
        
        try:
            # This sends and expects a response - the exact mechanism 
            # depends on the underlying UDP implementation
            response = await asyncio.wait_for(
                self._send_and_receive(target, msg),
                timeout=timeout,
            )
            # Check if response indicates success
            return response and b'ack' in response
        except (asyncio.TimeoutError, Exception):
            return False
    
    async def _send_and_receive(
        self, 
        target: tuple[str, int], 
        message: bytes,
    ) -> bytes | None:
        """
        Send a message and wait for a response.
        Returns the response bytes or None if failed.
        """
        # Create a future to wait for the response
        response_future: asyncio.Future[bytes] = asyncio.get_event_loop().create_future()
        
        # Store the pending request (implementation depends on framework)
        # For now, we use the send mechanism and hope for a response
        self._task_runner.run(
            self.send,
            target,
            message,
            timeout=None,
        )
        
        # In a real implementation, we'd track pending requests and 
        # resolve the future when a response arrives.
        # For this simplified version, we return None and let the caller
        # handle it via timeout.
        try:
            # Give a short window for response
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
            # Piggyback format: main_message|update1|update2|...
            piggyback_idx = data.find(b'|')
            if piggyback_idx > 0:
                main_data = data[:piggyback_idx]
                piggyback_data = data[piggyback_idx:]
                # Process piggybacked updates
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

            match message:
                case b'ack' | b'nack':

                    if target not in nodes:
                        await self.increase_failure_detector('missed_nack')
                        return b'nack>' + self._udp_addr_slug
                    
                    # Successful ack/nack processing improves our health
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
                                message + b'>' + target_addr,

                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'OK'))
                        # self._task_runner.run(
                        #     self.poll_node,
                        #     target,
                        # )

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
                            # We're being probed - this indicates someone might suspect us
                            # Increment our LHM (we might be slow)
                            await self.increase_failure_detector('refutation')
                            
                            # Broadcast refutation with new incarnation
                            new_incarnation = await self.broadcast_refutation()
                            
                            # Return alive message with new incarnation
                            return b'alive:' + str(new_incarnation).encode() + b'>' + self._udp_addr_slug
                        
                        if target not in nodes:
                            # We missed something
                            return b'nack>' + self._udp_addr_slug
                        
                        base_timeout = self._context.read('current_timeout')
                        timeout = self.get_lhm_adjusted_timeout(base_timeout)

                        # Tell the suspect node to forward an ack.
                        self._task_runner.run(
                            self.send,
                            target,
                            b'ack>' + source_addr.encode(),
                            timeout=timeout,
                        )
                        
                        # Broadcast the suspicion
                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])
                            
                        return b'ack'
                
                case b'ping-req':
                    # Indirect probe request: another node is asking us to probe target
                    # Message format: ping-req:incarnation>target_host:target_port
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')
                        
                        if target is None:
                            return b'nack>' + self._udp_addr_slug
                        
                        if self.udp_target_is_self(target):
                            # We are the target - respond with ack
                            return b'ping-req-ack:alive>' + self._udp_addr_slug
                        
                        if target not in nodes:
                            # Unknown target
                            return b'ping-req-ack:unknown>' + self._udp_addr_slug
                        
                        base_timeout = self._context.read('current_timeout')
                        timeout = self.get_lhm_adjusted_timeout(base_timeout)
                        
                        # Send probe to target and wait for response
                        # We need to probe the target and forward result back to requester
                        try:
                            # Probe the target directly
                            result = await asyncio.wait_for(
                                self._send_probe_and_wait(target),
                                timeout=timeout,
                            )
                            if result:
                                # Target responded - send ack back to requester
                                return b'ping-req-ack:alive>' + target_addr
                            else:
                                # Target did not respond
                                return b'ping-req-ack:dead>' + target_addr
                        except asyncio.TimeoutError:
                            # Probe timed out
                            return b'ping-req-ack:timeout>' + target_addr
                
                case b'ping-req-ack':
                    # Response from an indirect probe we requested
                    # Message format: ping-req-ack:status>target_host:target_port
                    # Parse the status from message part
                    msg_parts = message.split(b':', maxsplit=1)
                    if len(msg_parts) > 1:
                        status_str = msg_parts[1]
                        if status_str == b'alive' and target:
                            # Target is alive - record this
                            await self.handle_indirect_probe_response(target, is_alive=True)
                            await self.decrease_failure_detector('successful_probe')
                            return b'ack>' + self._udp_addr_slug
                        elif status_str in (b'dead', b'timeout', b'unknown') and target:
                            # Target did not respond to this proxy
                            # Don't immediately fail - wait for other proxies or timeout
                            await self.handle_indirect_probe_response(target, is_alive=False)
                    return b'ack>' + self._udp_addr_slug
                
                case b'alive':
                    # Refutation message: a node is declaring itself alive
                    # Message format: alive:incarnation>node_host:node_port
                    # Parse incarnation from the message
                    msg_parts = message.split(b':', maxsplit=1)
                    msg_incarnation = 0
                    if len(msg_parts) > 1:
                        try:
                            msg_incarnation = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        # Check if this refutes a current suspicion
                        if self.is_message_fresh(target, msg_incarnation, b'OK'):
                            # Refutation is valid - clear suspicion
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
                    # Suspicion message: another node suspects the target
                    # Message format: suspect:incarnation>target_host:target_port
                    # Parse incarnation from the message
                    msg_parts = message.split(b':', maxsplit=1)
                    msg_incarnation = 0
                    if len(msg_parts) > 1:
                        try:
                            msg_incarnation = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        if self.udp_target_is_self(target):
                            # We are the suspect! Broadcast refutation immediately
                            await self.increase_failure_detector('refutation')
                            new_incarnation = await self.broadcast_refutation()
                            return b'alive:' + str(new_incarnation).encode() + b'>' + self._udp_addr_slug
                        
                        # Check if this suspicion is fresh
                        if self.is_message_fresh(target, msg_incarnation, b'SUSPECT'):
                            # Record/confirm the suspicion
                            self.start_suspicion(target, msg_incarnation, addr)
                            
                            # Re-gossip the suspicion if needed
                            suspicion = self._suspicion_manager.get_suspicion(target)
                            if suspicion and suspicion.should_regossip():
                                suspicion.mark_regossiped()
                                await self.broadcast_suspicion(target, msg_incarnation)
                    
                    return b'ack>' + self._udp_addr_slug
                
                # Leadership messages
                case b'leader-claim':
                    # Candidate is claiming leadership
                    # Format: leader-claim:term:lhm>candidate_addr
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
                        # Process the claim and maybe vote
                        vote_msg = self._leader_election.handle_claim(target, term, candidate_lhm)
                        if vote_msg:
                            # Send vote back to candidate
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
                    # Vote received for our candidacy
                    # Format: leader-vote:term>candidate_addr
                    msg_parts = message.split(b':', maxsplit=1)
                    term = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    # Record the vote
                    if self._leader_election.handle_vote(addr, term):
                        # We won the election!
                        self._leader_election.state.become_leader(term)
                        self._leader_election.state.current_leader = self._get_self_udp_addr()
                        
                        # Announce victory
                        self_addr = self._get_self_udp_addr()
                        elected_msg = (
                            b'leader-elected:' +
                            str(term).encode() + b'>' +
                            f'{self_addr[0]}:{self_addr[1]}'.encode()
                        )
                        self._broadcast_leadership_message(elected_msg)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-elected':
                    # New leader announced
                    # Format: leader-elected:term>leader_addr
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
                    # Leader heartbeat received
                    # Format: leader-heartbeat:term>leader_addr
                    msg_parts = message.split(b':', maxsplit=1)
                    term = 0
                    if len(msg_parts) >= 2:
                        try:
                            term = int(msg_parts[1].decode())
                        except ValueError:
                            pass
                    
                    if target:
                        self._leader_election.handle_heartbeat(target, term)
                    
                    return b'ack>' + self._udp_addr_slug
                
                case b'leader-stepdown':
                    # Leader stepping down
                    # Format: leader-stepdown:term>leader_addr
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
                    
                case _:
                    return b'nack'
                
        except Exception:
            import traceback
            print(traceback.format_exc())
