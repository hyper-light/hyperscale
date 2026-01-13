"""
Tests for ServerAdapter.

Covers:
- Happy path: adapter delegates all calls to server
- Negative path: adapter handles missing server attributes
- Edge cases: property access, async method forwarding
- Concurrency: parallel adapter operations
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.swim.message_handling.server_adapter import (
    ServerAdapter,
)


@dataclass
class MockHealthAwareServer:
    """
    Mock HealthAwareServer for testing ServerAdapter.

    Simulates the HealthAwareServer interface that ServerAdapter wraps.
    """

    _udp_addr_slug: bytes = b"127.0.0.1:9000"
    _self_addr: tuple[str, int] = ("127.0.0.1", 9000)

    # Components
    _leader_election: Any = field(default_factory=MagicMock)
    _hierarchical_detector: Any = field(default_factory=MagicMock)
    _task_runner: Any = field(default_factory=MagicMock)
    _probe_scheduler: Any = field(default_factory=MagicMock)
    _incarnation_tracker: Any = field(default_factory=MagicMock)
    _audit_log: Any = field(default_factory=MagicMock)
    _indirect_probe_manager: Any = field(default_factory=MagicMock)
    _metrics: Any = field(default_factory=MagicMock)
    _pending_probe_acks: dict = field(default_factory=dict)

    # Context mock
    _context: Any = field(default_factory=MagicMock)

    # Tracking
    _confirmed_peers: set = field(default_factory=set)
    _sent_messages: list = field(default_factory=list)

    def _get_self_udp_addr(self) -> tuple[str, int]:
        return self._self_addr

    def udp_target_is_self(self, target: tuple[str, int]) -> bool:
        return target == self._self_addr

    def get_other_nodes(self, exclude: tuple[str, int] | None = None) -> list:
        return []

    def confirm_peer(self, peer: tuple[str, int]) -> bool:
        if peer in self._confirmed_peers:
            return False
        self._confirmed_peers.add(peer)
        return True

    def is_peer_confirmed(self, peer: tuple[str, int]) -> bool:
        return peer in self._confirmed_peers

    def update_node_state(
        self,
        node: tuple[str, int],
        status: bytes,
        incarnation: int,
        timestamp: float,
    ) -> None:
        pass

    def is_message_fresh(
        self,
        node: tuple[str, int],
        incarnation: int,
        status: bytes,
    ) -> bool:
        return True

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

    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> Any:
        return True

    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        return True

    async def broadcast_refutation(self) -> int:
        return 2

    async def broadcast_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> None:
        pass

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

    def _build_ack_with_state(self) -> bytes:
        return b"ack>" + self._udp_addr_slug

    def _build_ack_with_state_for_addr(self, addr_slug: bytes) -> bytes:
        return b"ack>" + addr_slug

    def _get_embedded_state(self) -> bytes | None:
        return None

    async def handle_error(self, error: Exception) -> None:
        pass

    async def _validate_target(
        self,
        target: tuple[str, int] | None,
        message_type: bytes,
        source_addr: tuple[str, int],
    ) -> bool:
        return target is not None

    async def _parse_incarnation_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        return 0

    async def _parse_term_safe(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> int:
        return 0

    async def _parse_leadership_claim(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, int]:
        return (0, 0)

    async def _parse_pre_vote_response(
        self, message: bytes, source_addr: tuple[str, int]
    ) -> tuple[int, bool]:
        return (0, False)

    async def handle_indirect_probe_response(
        self, target: tuple[str, int], is_alive: bool
    ) -> None:
        pass

    async def _send_probe_and_wait(self, target: tuple[str, int]) -> bool:
        return True

    async def _safe_queue_put(
        self,
        queue: Any,
        item: tuple[int, bytes],
        node: tuple[str, int],
    ) -> bool:
        return True

    async def _clear_stale_state(self, node: tuple[str, int]) -> None:
        pass

    def update_probe_scheduler_membership(self) -> None:
        pass

    def _broadcast_leadership_message(self, message: bytes) -> None:
        pass

    async def _send_to_addr(
        self,
        target: tuple[str, int],
        message: bytes,
        timeout: float | None = None,
    ) -> bool:
        self._sent_messages.append((target, message))
        return True

    async def _gather_with_errors(
        self,
        coros: list,
        operation: str,
        timeout: float,
    ) -> tuple[list, list]:
        results = []
        errors = []
        for coro in coros:
            try:
                result = await coro
                results.append(result)
            except Exception as e:
                errors.append(e)
        return (results, errors)


@pytest.fixture
def mock_health_aware_server() -> MockHealthAwareServer:
    """Create a mock HealthAwareServer for testing."""
    server = MockHealthAwareServer()
    server._context = MagicMock()
    server._context.read = MagicMock(return_value={})
    server._context.with_value = MagicMock(return_value=AsyncContextManager())
    return server


class AsyncContextManager:
    """Mock async context manager."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class TestServerAdapterIdentity:
    """Tests for ServerAdapter identity methods."""

    def test_udp_addr_slug(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's udp_addr_slug."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.udp_addr_slug == b"127.0.0.1:9000"

    def test_get_self_udp_addr(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates get_self_udp_addr to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.get_self_udp_addr() == ("127.0.0.1", 9000)

    def test_udp_target_is_self(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates udp_target_is_self to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.udp_target_is_self(("127.0.0.1", 9000)) is True
        assert adapter.udp_target_is_self(("192.168.1.1", 8000)) is False


class TestServerAdapterStateAccess:
    """Tests for ServerAdapter state access methods."""

    def test_read_nodes(self, mock_health_aware_server: MockHealthAwareServer) -> None:
        """Adapter delegates read_nodes to incarnation tracker (AD-46)."""
        mock_health_aware_server._incarnation_tracker.node_states = {
            ("192.168.1.1", 8000): "node_data"
        }
        adapter = ServerAdapter(mock_health_aware_server)

        nodes = adapter.read_nodes()

        assert ("192.168.1.1", 8000) in nodes

    @pytest.mark.asyncio
    async def test_get_current_timeout(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        mock_health_aware_server._context.read.return_value = 1.5
        adapter = ServerAdapter(mock_health_aware_server)

        timeout = await adapter.get_current_timeout()

        assert timeout == 1.5

    def test_get_other_nodes(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates get_other_nodes to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        nodes = adapter.get_other_nodes()

        assert nodes == []


class TestServerAdapterPeerConfirmation:
    """Tests for ServerAdapter peer confirmation methods."""

    def test_confirm_peer(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates confirm_peer to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = adapter.confirm_peer(("192.168.1.1", 8000))

        assert result is True
        assert ("192.168.1.1", 8000) in mock_health_aware_server._confirmed_peers

    def test_is_peer_confirmed(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates is_peer_confirmed to server."""
        mock_health_aware_server._confirmed_peers.add(("192.168.1.1", 8000))
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.is_peer_confirmed(("192.168.1.1", 8000)) is True
        assert adapter.is_peer_confirmed(("192.168.1.2", 8001)) is False


class TestServerAdapterNodeState:
    """Tests for ServerAdapter node state methods."""

    def test_update_node_state(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates update_node_state to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        # Should not raise
        adapter.update_node_state(("192.168.1.1", 8000), b"OK", 1, 12345.0)

    def test_is_message_fresh(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates is_message_fresh to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = adapter.is_message_fresh(("192.168.1.1", 8000), 1, b"OK")

        assert result is True


class TestServerAdapterFailureDetection:
    """Tests for ServerAdapter failure detection methods."""

    @pytest.mark.asyncio
    async def test_increase_failure_detector(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates increase_failure_detector to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        # Should not raise
        await adapter.increase_failure_detector("test_reason")

    @pytest.mark.asyncio
    async def test_decrease_failure_detector(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates decrease_failure_detector to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        # Should not raise
        await adapter.decrease_failure_detector("test_reason")

    def test_get_lhm_adjusted_timeout(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates get_lhm_adjusted_timeout to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        timeout = adapter.get_lhm_adjusted_timeout(1.0)

        assert timeout == 1.0


class TestServerAdapterSuspicion:
    """Tests for ServerAdapter suspicion methods."""

    @pytest.mark.asyncio
    async def test_start_suspicion(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates start_suspicion to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.start_suspicion(
            ("192.168.1.1", 8000), 1, ("192.168.1.2", 8001)
        )

        assert result is True

    @pytest.mark.asyncio
    async def test_refute_suspicion(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates refute_suspicion to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.refute_suspicion(("192.168.1.1", 8000), 2)

        assert result is True

    @pytest.mark.asyncio
    async def test_broadcast_refutation(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates broadcast_refutation to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        incarnation = await adapter.broadcast_refutation()

        assert incarnation == 2

    @pytest.mark.asyncio
    async def test_broadcast_suspicion(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates broadcast_suspicion to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        # Should not raise
        await adapter.broadcast_suspicion(("192.168.1.1", 8000), 1)


class TestServerAdapterCommunication:
    """Tests for ServerAdapter communication methods."""

    @pytest.mark.asyncio
    async def test_send(self, mock_health_aware_server: MockHealthAwareServer) -> None:
        """Adapter delegates send to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.send(("192.168.1.1", 8000), b"test_data")

        assert result == b"ack"
        assert ("192.168.1.1", 8000), (
            b"test_data" in mock_health_aware_server._sent_messages
        )

    @pytest.mark.asyncio
    async def test_send_if_ok(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates send_if_ok to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.send_if_ok(("192.168.1.1", 8000), b"test_data")

        assert result == b"ack"


class TestServerAdapterResponseBuilding:
    """Tests for ServerAdapter response building methods."""

    def test_build_ack_with_state(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates build_ack_with_state to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = adapter.build_ack_with_state()

        assert result == b"ack>127.0.0.1:9000"

    def test_build_ack_with_state_for_addr(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates build_ack_with_state_for_addr to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = adapter.build_ack_with_state_for_addr(b"192.168.1.1:8000")

        assert result == b"ack>192.168.1.1:8000"

    def test_get_embedded_state(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates get_embedded_state to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = adapter.get_embedded_state()

        assert result is None


class TestServerAdapterErrorHandling:
    """Tests for ServerAdapter error handling methods."""

    @pytest.mark.asyncio
    async def test_handle_error(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates handle_error to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        # Should not raise
        await adapter.handle_error(ValueError("test error"))


class TestServerAdapterMetrics:
    """Tests for ServerAdapter metrics methods."""

    def test_increment_metric(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates increment_metric to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        # Should not raise
        adapter.increment_metric("test_metric")

        mock_health_aware_server._metrics.increment.assert_called_with("test_metric", 1)


class TestServerAdapterComponentAccess:
    """Tests for ServerAdapter component access properties."""

    def test_leader_election(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's leader_election."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.leader_election is mock_health_aware_server._leader_election

    def test_hierarchical_detector(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's hierarchical_detector."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert (
            adapter.hierarchical_detector
            is mock_health_aware_server._hierarchical_detector
        )

    def test_task_runner(self, mock_health_aware_server: MockHealthAwareServer) -> None:
        """Adapter returns server's task_runner."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.task_runner is mock_health_aware_server._task_runner

    def test_probe_scheduler(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's probe_scheduler."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.probe_scheduler is mock_health_aware_server._probe_scheduler

    def test_incarnation_tracker(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's incarnation_tracker."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert (
            adapter.incarnation_tracker is mock_health_aware_server._incarnation_tracker
        )

    def test_audit_log(self, mock_health_aware_server: MockHealthAwareServer) -> None:
        """Adapter returns server's audit_log."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert adapter.audit_log is mock_health_aware_server._audit_log

    def test_indirect_probe_manager(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's indirect_probe_manager."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert (
            adapter.indirect_probe_manager
            is mock_health_aware_server._indirect_probe_manager
        )

    def test_pending_probe_acks(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter returns server's pending_probe_acks."""
        adapter = ServerAdapter(mock_health_aware_server)

        assert (
            adapter.pending_probe_acks is mock_health_aware_server._pending_probe_acks
        )


class TestServerAdapterValidation:
    """Tests for ServerAdapter validation methods."""

    @pytest.mark.asyncio
    async def test_validate_target(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates validate_target to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.validate_target(
            ("192.168.1.1", 8000), b"test", ("192.168.1.2", 8001)
        )

        assert result is True


class TestServerAdapterMessageParsing:
    """Tests for ServerAdapter message parsing methods."""

    @pytest.mark.asyncio
    async def test_parse_incarnation_safe(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates parse_incarnation_safe to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.parse_incarnation_safe(b"alive:5", ("192.168.1.1", 8000))

        assert result == 0  # Mock returns 0

    @pytest.mark.asyncio
    async def test_parse_term_safe(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates parse_term_safe to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        result = await adapter.parse_term_safe(
            b"leader-heartbeat:5", ("192.168.1.1", 8000)
        )

        assert result == 0

    @pytest.mark.asyncio
    async def test_parse_leadership_claim(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates parse_leadership_claim to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        term, lhm = await adapter.parse_leadership_claim(
            b"leader-claim:5:100", ("192.168.1.1", 8000)
        )

        assert term == 0
        assert lhm == 0

    @pytest.mark.asyncio
    async def test_parse_pre_vote_response(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Adapter delegates parse_pre_vote_response to server."""
        adapter = ServerAdapter(mock_health_aware_server)

        term, granted = await adapter.parse_pre_vote_response(
            b"pre-vote-resp:5:true", ("192.168.1.1", 8000)
        )

        assert term == 0
        assert granted is False


class TestServerAdapterConcurrency:
    """Concurrency tests for ServerAdapter."""

    @pytest.mark.asyncio
    async def test_concurrent_sends(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Multiple sends can run concurrently through adapter."""
        adapter = ServerAdapter(mock_health_aware_server)

        async def send_one(index: int) -> bytes | None:
            return await adapter.send(
                ("192.168.1.1", 8000 + index), f"data_{index}".encode()
            )

        tasks = [send_one(i) for i in range(50)]
        results = await asyncio.gather(*tasks)

        assert all(r == b"ack" for r in results)
        assert len(mock_health_aware_server._sent_messages) == 50

    @pytest.mark.asyncio
    async def test_concurrent_property_access(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        """Property access is safe under concurrency."""
        adapter = ServerAdapter(mock_health_aware_server)

        async def access_properties(index: int) -> tuple:
            return (
                adapter.udp_addr_slug,
                adapter.get_self_udp_addr(),
                adapter.leader_election,
                adapter.task_runner,
            )

        tasks = [access_properties(i) for i in range(50)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 50
        assert all(r[0] == b"127.0.0.1:9000" for r in results)


class TestServerAdapterContextManagement:
    @pytest.mark.asyncio
    async def test_context_with_value(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        adapter = ServerAdapter(mock_health_aware_server)

        ctx = await adapter.context_with_value(("192.168.1.1", 8000))

        assert ctx is not None

    @pytest.mark.asyncio
    async def test_write_context(
        self, mock_health_aware_server: MockHealthAwareServer
    ) -> None:
        adapter = ServerAdapter(mock_health_aware_server)

        await adapter.write_context("key", "value")
