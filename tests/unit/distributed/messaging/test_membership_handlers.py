"""
Tests for membership handlers (AckHandler, NackHandler, JoinHandler, LeaveHandler).

Covers:
- Happy path: normal message handling
- Negative path: invalid targets, missing data
- Edge cases: self-targeted messages, unknown nodes
- Concurrency: parallel handling
"""

import asyncio

import pytest

from hyperscale.distributed.swim.message_handling.membership import (
    AckHandler,
    NackHandler,
    JoinHandler,
    LeaveHandler,
)
from hyperscale.distributed.swim.message_handling.models import MessageContext

from tests.distributed.messaging.mocks import MockServerInterface


class TestAckHandlerHappyPath:
    """Happy path tests for AckHandler."""

    @pytest.mark.asyncio
    async def test_handle_ack_confirms_peer(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler confirms the peer."""
        handler = AckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        await handler.handle(context)

        assert mock_server.is_peer_confirmed(("192.168.1.1", 8000))

    @pytest.mark.asyncio
    async def test_handle_ack_updates_node_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler updates source node to OK state."""
        mock_server.add_node(("192.168.1.1", 8000))
        handler = AckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        await handler.handle(context)

        node_state = mock_server.incarnation_tracker._nodes.get(("192.168.1.1", 8000))
        assert node_state is not None
        assert node_state[0] == b"OK"

    @pytest.mark.asyncio
    async def test_handle_ack_completes_pending_future(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler completes pending probe future."""
        handler = AckHandler(mock_server)
        future = asyncio.get_event_loop().create_future()
        mock_server._pending_probe_acks[("192.168.1.1", 8000)] = future

        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        await handler.handle(context)

        assert future.done()
        assert future.result() is True

    @pytest.mark.asyncio
    async def test_handle_ack_returns_ack(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler returns ack response."""
        handler = AckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestAckHandlerNegativePath:
    """Negative path tests for AckHandler."""

    @pytest.mark.asyncio
    async def test_handle_ack_target_not_in_nodes(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler returns nack when target is unknown."""
        handler = AckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.99", 9000),
            target_addr_bytes=b"192.168.1.99:9000",
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response
        assert b"unknown" in result.response

    @pytest.mark.asyncio
    async def test_handle_ack_source_not_in_nodes(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler handles source not in nodes gracefully."""
        handler = AckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.99", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Should still return ack
        assert result.response.startswith(b"ack>")


class TestAckHandlerEdgeCases:
    """Edge case tests for AckHandler."""

    @pytest.mark.asyncio
    async def test_handle_ack_already_completed_future(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ack handler handles already completed future gracefully."""
        handler = AckHandler(mock_server)
        future = asyncio.get_event_loop().create_future()
        future.set_result(True)
        mock_server._pending_probe_acks[("192.168.1.1", 8000)] = future

        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"ack",
            message=b"ack",
            clock_time=12345,
        )

        # Should not raise
        result = await handler.handle(context)
        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """AckHandler has correct message_types."""
        handler = AckHandler(mock_server)

        assert handler.message_types == (b"ack",)


class TestNackHandlerHappyPath:
    """Happy path tests for NackHandler."""

    @pytest.mark.asyncio
    async def test_handle_nack_confirms_peer(
        self, mock_server: MockServerInterface
    ) -> None:
        """Nack handler confirms the peer (communication succeeded)."""
        handler = NackHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"nack",
            message=b"nack",
            clock_time=12345,
        )

        await handler.handle(context)

        assert mock_server.is_peer_confirmed(("192.168.1.1", 8000))

    @pytest.mark.asyncio
    async def test_handle_nack_updates_source_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Nack handler updates source node to OK state."""
        mock_server.add_node(("192.168.1.1", 8000))
        handler = NackHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"nack",
            message=b"nack",
            clock_time=12345,
        )

        await handler.handle(context)

        node_state = mock_server.incarnation_tracker._nodes.get(("192.168.1.1", 8000))
        assert node_state is not None
        assert node_state[0] == b"OK"

    @pytest.mark.asyncio
    async def test_handle_nack_returns_ack(
        self, mock_server: MockServerInterface
    ) -> None:
        """Nack handler returns ack response."""
        handler = NackHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"nack",
            message=b"nack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestNackHandlerEdgeCases:
    """Edge case tests for NackHandler."""

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """NackHandler has correct message_types."""
        handler = NackHandler(mock_server)

        assert handler.message_types == (b"nack",)


class TestJoinHandlerHappyPath:
    """Happy path tests for JoinHandler."""

    @pytest.mark.asyncio
    async def test_handle_join_increments_metric(
        self, mock_server: MockServerInterface
    ) -> None:
        """Join handler increments joins_received metric."""
        handler = JoinHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"v1.0|192.168.1.2:9001",
            message_type=b"join",
            message=b"join",
            clock_time=12345,
        )

        await handler.handle(context)

        assert mock_server.metrics._counters.get("joins_received", 0) >= 1

    @pytest.mark.asyncio
    async def test_handle_join_confirms_peers(
        self, mock_server: MockServerInterface
    ) -> None:
        """Join handler confirms both sender and joining node."""
        handler = JoinHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"v1.0|192.168.1.2:9001",
            message_type=b"join",
            message=b"join",
            clock_time=12345,
        )

        await handler.handle(context)

        # Both should be confirmed
        assert mock_server.is_peer_confirmed(("192.168.1.1", 8000))


class TestJoinHandlerNegativePath:
    """Negative path tests for JoinHandler."""

    @pytest.mark.asyncio
    async def test_handle_join_no_version(
        self, mock_server: MockServerInterface
    ) -> None:
        """Join handler rejects messages without version."""
        handler = JoinHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",  # No version prefix
            message_type=b"join",
            message=b"join",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response
        assert mock_server.metrics._counters.get("joins_rejected_no_version", 0) >= 1

    @pytest.mark.asyncio
    async def test_handle_join_invalid_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Join handler rejects invalid target."""
        mock_server._validate_target_result = False
        handler = JoinHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"v1.0|192.168.1.2:9001",
            message_type=b"join",
            message=b"join",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response


class TestJoinHandlerEdgeCases:
    """Edge case tests for JoinHandler."""

    @pytest.mark.asyncio
    async def test_handle_self_join(self, mock_server: MockServerInterface) -> None:
        """Join handler handles self-join specially."""
        handler = JoinHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),  # Self address
            target_addr_bytes=b"v1.0|127.0.0.1:9000",
            message_type=b"join",
            message=b"join",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Self-join returns ack without embedding state
        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """JoinHandler has correct message_types."""
        handler = JoinHandler(mock_server)

        assert handler.message_types == (b"join",)


class TestLeaveHandlerHappyPath:
    """Happy path tests for LeaveHandler."""

    @pytest.mark.asyncio
    async def test_handle_leave_known_node(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leave handler processes known node departure."""
        mock_server.add_node(("192.168.1.2", 9001))
        handler = LeaveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leave",
            message=b"leave",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestLeaveHandlerNegativePath:
    """Negative path tests for LeaveHandler."""

    @pytest.mark.asyncio
    async def test_handle_leave_invalid_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leave handler rejects invalid target."""
        mock_server._validate_target_result = False
        handler = LeaveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leave",
            message=b"leave",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response

    @pytest.mark.asyncio
    async def test_handle_leave_unknown_node(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leave handler rejects unknown node."""
        handler = LeaveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.99", 9001),  # Not in nodes
            target_addr_bytes=b"192.168.1.99:9001",
            message_type=b"leave",
            message=b"leave",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response


class TestLeaveHandlerEdgeCases:
    """Edge case tests for LeaveHandler."""

    @pytest.mark.asyncio
    async def test_handle_self_leave(self, mock_server: MockServerInterface) -> None:
        """Leave handler handles self-leave specially."""
        handler = LeaveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),  # Self address
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"leave",
            message=b"leave",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"leave>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """LeaveHandler has correct message_types."""
        handler = LeaveHandler(mock_server)

        assert handler.message_types == (b"leave",)


class TestMembershipHandlersConcurrency:
    """Concurrency tests for membership handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_ack_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple ack handlers can run concurrently."""
        handler = AckHandler(mock_server)

        async def handle_ack(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                target=None,
                target_addr_bytes=None,
                message_type=b"ack",
                message=b"ack",
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_ack(i) for i in range(50)]
        await asyncio.gather(*tasks)

        # All peers should be confirmed
        assert len(mock_server._confirmed_peers) == 50

    @pytest.mark.asyncio
    async def test_concurrent_join_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple join handlers can run concurrently."""
        handler = JoinHandler(mock_server)

        async def handle_join(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000),
                target=("127.0.0.1", 9000),  # Self join for simplicity
                target_addr_bytes=b"v1.0|127.0.0.1:9000",
                message_type=b"join",
                message=b"join",
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_join(i) for i in range(20)]
        await asyncio.gather(*tasks)

        # Metric should reflect all joins
        assert mock_server.metrics._counters.get("joins_received", 0) >= 20
