"""
Mock implementations for message_handling tests.

This module contains mock classes that implement the ServerInterface
protocol for testing handlers without a real HealthAwareServer.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MockLeaderElection:
    """Mock leader election component."""

    state: Any = field(default_factory=lambda: MockLeaderState())

    def handle_claim(
        self, target: tuple[str, int], term: int, candidate_lhm: int
    ) -> bytes | None:
        return b"leader-vote:1>127.0.0.1:9000"

    def handle_vote(self, addr: tuple[str, int], term: int) -> bool:
        return False

    def handle_discovered_leader(self, target: tuple[str, int], term: int) -> bool:
        return False

    def handle_pre_vote_request(
        self, candidate: tuple[str, int], term: int, candidate_lhm: int
    ) -> bytes | None:
        return b"pre-vote-resp:1:true>127.0.0.1:9000"

    def handle_pre_vote_response(
        self, voter: tuple[str, int], term: int, granted: bool
    ) -> None:
        pass

    async def handle_elected(self, target: tuple[str, int], term: int) -> None:
        pass

    async def handle_heartbeat(self, target: tuple[str, int], term: int) -> None:
        pass

    async def handle_stepdown(self, target: tuple[str, int], term: int) -> None:
        pass

    async def _step_down(self) -> None:
        pass


@dataclass
class MockLeaderState:
    """Mock leader state."""

    current_term: int = 1
    current_leader: tuple[str, int] | None = None
    pre_voting_in_progress: bool = False

    def is_leader(self) -> bool:
        return False

    def is_candidate(self) -> bool:
        return False

    def become_leader(self, term: int) -> None:
        self.current_term = term


@dataclass
class MockHierarchicalDetector:
    """Mock hierarchical failure detector."""

    _regossip_count: int = 0

    def should_regossip_global(self, node: tuple[str, int]) -> bool:
        return self._regossip_count < 1

    def mark_regossiped_global(self, node: tuple[str, int]) -> None:
        self._regossip_count += 1


@dataclass
class MockTaskRunner:
    """Mock task runner."""

    _tasks: list = field(default_factory=list)

    def run(self, coro_or_func, *args, **kwargs) -> None:
        self._tasks.append((coro_or_func, args, kwargs))


@dataclass
class MockProbeScheduler:
    """Mock probe scheduler."""

    _members: set = field(default_factory=set)

    def add_member(self, member: tuple[str, int]) -> None:
        self._members.add(member)

    def remove_member(self, member: tuple[str, int]) -> None:
        self._members.discard(member)


@dataclass
class MockIncarnationTracker:
    _nodes: dict = field(default_factory=dict)

    async def update_node(
        self,
        node: tuple[str, int],
        status: bytes,
        incarnation: int,
        timestamp: float,
    ) -> bool:
        self._nodes[node] = (status, incarnation, timestamp)
        return True

    def get_node_incarnation(self, node: tuple[str, int]) -> int:
        if node in self._nodes:
            return self._nodes[node][1]
        return 0


@dataclass
class MockAuditLog:
    """Mock audit log."""

    _events: list = field(default_factory=list)

    def record(self, event_type: Any, **kwargs) -> None:
        self._events.append((event_type, kwargs))


@dataclass
class MockIndirectProbeManager:
    """Mock indirect probe manager."""

    _pending_probes: dict = field(default_factory=dict)

    def get_pending_probe(self, target: tuple[str, int]) -> Any:
        return self._pending_probes.get(target)

    def add_pending_probe(self, target: tuple[str, int]) -> None:
        self._pending_probes[target] = True


@dataclass
class MockMetrics:
    """Mock metrics."""

    _counters: dict = field(default_factory=dict)

    def increment(self, name: str, value: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + value


class MockServerInterface:
    """
    Mock implementation of ServerInterface for testing handlers.

    Provides configurable behavior for all server operations.
    """

    def __init__(self) -> None:
        # Identity
        self._udp_addr_slug = b"127.0.0.1:9000"
        self._self_addr = ("127.0.0.1", 9000)

        # State
        self._nodes: dict[tuple[str, int], asyncio.Queue] = {}
        self._current_timeout = 1.0

        # Components
        self._leader_election = MockLeaderElection()
        self._hierarchical_detector = MockHierarchicalDetector()
        self._task_runner = MockTaskRunner()
        self._probe_scheduler = MockProbeScheduler()
        self._incarnation_tracker = MockIncarnationTracker()
        self._audit_log = MockAuditLog()
        self._indirect_probe_manager = MockIndirectProbeManager()
        self._metrics = MockMetrics()

        # Tracking
        self._confirmed_peers: set[tuple[str, int]] = set()
        self._pending_probe_acks: dict[tuple[str, int], asyncio.Future] = {}
        self._sent_messages: list[tuple[tuple[str, int], bytes]] = []
        self._errors: list[Exception] = []

        # Configurable behaviors
        self._validate_target_result = True
        self._is_message_fresh_result = True
        self._broadcast_refutation_incarnation = 2
        self._embedded_state: bytes | None = None

    # === Identity ===

    @property
    def udp_addr_slug(self) -> bytes:
        return self._udp_addr_slug

    def get_self_udp_addr(self) -> tuple[str, int]:
        return self._self_addr

    def udp_target_is_self(self, target: tuple[str, int]) -> bool:
        return target == self._self_addr

    # === State Access ===

    def read_nodes(self) -> dict[tuple[str, int], Any]:
        return self._nodes

    async def get_current_timeout(self) -> float:
        return self._current_timeout

    def get_other_nodes(
        self, exclude: tuple[str, int] | None = None
    ) -> list[tuple[str, int]]:
        nodes = list(self._nodes.keys())
        if exclude and exclude in nodes:
            nodes.remove(exclude)
        if self._self_addr in nodes:
            nodes.remove(self._self_addr)
        return nodes

    # === Peer Confirmation ===

    def confirm_peer(self, peer: tuple[str, int]) -> bool:
        if peer in self._confirmed_peers:
            return False
        self._confirmed_peers.add(peer)
        return True

    def is_peer_confirmed(self, peer: tuple[str, int]) -> bool:
        return peer in self._confirmed_peers

    # === Node State ===

    def update_node_state(
        self,
        node: tuple[str, int],
        status: bytes,
        incarnation: int,
        timestamp: float,
    ) -> None:
        self._incarnation_tracker.update_node(node, status, incarnation, timestamp)

    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: bytes,
    ) -> bool:
        return self._is_message_fresh_result

    # === Failure Detection ===

    async def increase_failure_detector(self, reason: str) -> None:
        pass

    async def decrease_failure_detector(self, reason: str) -> None:
        pass

    def get_lhm_adjusted_timeout(
        self,
        base_timeout: float,
        target_node_id: str | None = None,
    ) -> float:
        return base_timeout

    # === Suspicion ===

    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        return True

    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        return True

    async def broadcast_refutation(self) -> int:
        return self._broadcast_refutation_incarnation

    async def broadcast_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        pass

    # === Communication ===

    async def send(
        self,
        target: tuple[str, int],
        data: bytes,
        timeout: float | None = None,
    ) -> bytes | None:
        self._sent_messages.append((target, data))
        return b"ack"

    async def send_if_ok(
        self,
        target: tuple[str, int],
        data: bytes,
    ) -> bytes | None:
        self._sent_messages.append((target, data))
        return b"ack"

    # === Response Building ===

    def build_ack_with_state(self) -> bytes:
        return b"ack>" + self._udp_addr_slug

    def build_ack_with_state_for_addr(self, addr_slug: bytes) -> bytes:
        return b"ack>" + addr_slug

    # === Cross-Cluster Methods ===

    async def build_xprobe_response(
        self,
        source_addr: tuple[str, int],
        probe_data: bytes,
    ) -> bytes | None:
        """Build response to cross-cluster probe. Returns None for xnack."""
        return None  # Default: return xnack (not a DC leader)

    async def handle_xack_response(
        self,
        source_addr: tuple[str, int],
        response_data: bytes,
    ) -> None:
        """Handle cross-cluster health acknowledgment response."""
        pass  # Default: no-op

    def get_embedded_state(self) -> bytes | None:
        return self._embedded_state

    # === Error Handling ===

    async def handle_error(self, error: Exception) -> None:
        self._errors.append(error)

    # === Metrics ===

    def increment_metric(self, name: str, value: int = 1) -> None:
        self._metrics.increment(name, value)

    # === Component Access ===

    @property
    def leader_election(self) -> MockLeaderElection:
        return self._leader_election

    @property
    def hierarchical_detector(self) -> MockHierarchicalDetector:
        return self._hierarchical_detector

    @property
    def task_runner(self) -> MockTaskRunner:
        return self._task_runner

    @property
    def probe_scheduler(self) -> MockProbeScheduler:
        return self._probe_scheduler

    @property
    def incarnation_tracker(self) -> MockIncarnationTracker:
        return self._incarnation_tracker

    @property
    def audit_log(self) -> MockAuditLog:
        return self._audit_log

    @property
    def indirect_probe_manager(self) -> MockIndirectProbeManager:
        return self._indirect_probe_manager

    @property
    def pending_probe_acks(self) -> dict[tuple[str, int], asyncio.Future]:
        return self._pending_probe_acks

    @property
    def metrics(self) -> MockMetrics:
        return self._metrics

    # === Validation ===

    async def validate_target(
        self,
        target: tuple[str, int] | None,
        message_type: bytes,
        source_addr: tuple[str, int],
    ) -> bool:
        return self._validate_target_result

    # === Message Parsing ===

    async def parse_incarnation_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        # Parse incarnation from message like "alive:5>addr"
        try:
            parts = message.split(b":", maxsplit=1)
            if len(parts) > 1:
                inc_part = parts[1].split(b">")[0]
                return int(inc_part.decode())
        except (ValueError, IndexError):
            pass
        return 0

    async def parse_term_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        # Parse term from message like "leader-heartbeat:5>addr"
        try:
            parts = message.split(b":", maxsplit=1)
            if len(parts) > 1:
                term_part = parts[1].split(b">")[0]
                return int(term_part.decode())
        except (ValueError, IndexError):
            pass
        return 0

    async def parse_leadership_claim(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, int]:
        # Parse term and LHM from message like "leader-claim:5:100>addr"
        try:
            parts = message.split(b":", maxsplit=2)
            if len(parts) >= 3:
                term = int(parts[1].decode())
                lhm_part = parts[2].split(b">")[0]
                lhm = int(lhm_part.decode())
                return (term, lhm)
        except (ValueError, IndexError):
            pass
        return (0, 0)

    async def parse_pre_vote_response(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, bool]:
        # Parse term and granted from message like "pre-vote-resp:5:true>addr"
        try:
            parts = message.split(b":", maxsplit=2)
            if len(parts) >= 3:
                term = int(parts[1].decode())
                granted_part = parts[2].split(b">")[0]
                granted = granted_part == b"true"
                return (term, granted)
        except (ValueError, IndexError):
            pass
        return (0, False)

    # === Indirect Probing ===

    async def handle_indirect_probe_response(
        self, target: tuple[str, int], is_alive: bool
    ) -> None:
        pass

    async def send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        return True

    # === Gossip ===

    async def safe_queue_put(
        self,
        queue: Any,
        item: tuple[int, bytes],
        node: tuple[str, int],
    ) -> bool:
        if queue is not None:
            await queue.put(item)
        return True

    async def clear_stale_state(self, node: tuple[str, int]) -> None:
        pass

    def update_probe_scheduler_membership(self) -> None:
        pass

    # === Context Management ===

    async def context_with_value(self, target: tuple[str, int]) -> "MockContextManager":
        return MockContextManager()

    async def write_context(self, key: Any, value: Any) -> None:
        if key == "nodes":
            pass
        elif isinstance(key, tuple):
            if key not in self._nodes:
                self._nodes[key] = asyncio.Queue()

    # === Leadership Broadcasting ===

    def broadcast_leadership_message(self, message: bytes) -> None:
        for node in self._nodes:
            self._sent_messages.append((node, message))

    async def send_to_addr(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float | None = None,
    ) -> bool:
        self._sent_messages.append((target, message))
        return True

    # === Gather Operations ===

    async def gather_with_errors(
        self,
        coros: list[Any],
        operation: str,
        timeout: float,
    ) -> tuple[list[Any], list[Exception]]:
        results = []
        errors = []
        for coro in coros:
            try:
                result = await coro
                results.append(result)
            except Exception as e:
                errors.append(e)
        return (results, errors)

    # === Test Helpers ===

    def add_node(self, addr: tuple[str, int]) -> None:
        """Add a node to the membership."""
        self._nodes[addr] = asyncio.Queue()

    def set_as_leader(self) -> None:
        """Configure this server as leader."""
        self._leader_election.state = MockLeaderState()
        self._leader_election.state.current_leader = self._self_addr

    def set_as_candidate(self) -> None:
        """Configure this server as candidate."""

        class CandidateState(MockLeaderState):
            def is_candidate(self) -> bool:
                return True

        self._leader_election.state = CandidateState()

    def set_pre_voting(self) -> None:
        """Configure pre-voting in progress."""
        self._leader_election.state.pre_voting_in_progress = True


class MockContextManager:
    """Mock async context manager for context_with_value."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False
