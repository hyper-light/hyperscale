"""
Tests for suspicion handlers (AliveHandler, SuspectHandler).

Covers:
- Happy path: normal suspicion handling
- Negative path: stale messages, invalid incarnations
- Edge cases: self-suspicion, regossip behavior
- Concurrency: parallel handling
"""

import asyncio

import pytest

from hyperscale.distributed.swim.message_handling.suspicion import (
    AliveHandler,
    SuspectHandler,
)
from hyperscale.distributed.swim.message_handling.models import MessageContext

from tests.distributed.messaging.mocks import MockServerInterface


class TestAliveHandlerHappyPath:
    """Happy path tests for AliveHandler."""

    @pytest.mark.asyncio
    async def test_handle_alive_confirms_peer(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler confirms the sender."""
        handler = AliveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:5",
            clock_time=12345,
        )

        await handler.handle(context)

        assert mock_server.is_peer_confirmed(("192.168.1.1", 8000))

    @pytest.mark.asyncio
    async def test_handle_alive_completes_pending_future(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler completes pending probe future."""
        handler = AliveHandler(mock_server)
        future = asyncio.get_event_loop().create_future()
        mock_server._pending_probe_acks[("192.168.1.1", 8000)] = future

        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:5",
            clock_time=12345,
        )

        await handler.handle(context)

        assert future.done()
        assert future.result() is True

    @pytest.mark.asyncio
    async def test_handle_alive_refutes_suspicion(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler refutes suspicion for fresh message."""
        handler = AliveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_alive_updates_node_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler updates node state to OK."""
        handler = AliveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:5",
            clock_time=12345,
        )

        await handler.handle(context)

        # Check node was updated
        node_state = mock_server.incarnation_tracker._nodes.get(("192.168.1.2", 9001))
        assert node_state is not None
        assert node_state[0] == b"OK"
        assert node_state[1] == 5  # Incarnation number


class TestAliveHandlerNegativePath:
    """Negative path tests for AliveHandler."""

    @pytest.mark.asyncio
    async def test_handle_alive_stale_message(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler ignores stale messages."""
        mock_server._is_message_fresh_result = False
        handler = AliveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:1",  # Stale incarnation
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Still returns ack but doesn't update state
        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_alive_no_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler handles missing target gracefully."""
        handler = AliveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"alive",
            message=b"alive:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Should still return ack
        assert result.response.startswith(b"ack>")


class TestAliveHandlerEdgeCases:
    """Edge case tests for AliveHandler."""

    @pytest.mark.asyncio
    async def test_handle_alive_already_completed_future(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler handles already completed future."""
        handler = AliveHandler(mock_server)
        future = asyncio.get_event_loop().create_future()
        future.set_result(True)
        mock_server._pending_probe_acks[("192.168.1.1", 8000)] = future

        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:5",
            clock_time=12345,
        )

        # Should not raise
        result = await handler.handle(context)
        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_alive_zero_incarnation(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler handles zero incarnation."""
        handler = AliveHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:0",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """AliveHandler has correct message_types."""
        handler = AliveHandler(mock_server)

        assert handler.message_types == (b"alive",)


class TestSuspectHandlerHappyPath:
    """Happy path tests for SuspectHandler."""

    @pytest.mark.asyncio
    async def test_handle_suspect_confirms_peer(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler confirms the sender."""
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        await handler.handle(context)

        assert mock_server.is_peer_confirmed(("192.168.1.1", 8000))

    @pytest.mark.asyncio
    async def test_handle_suspect_starts_suspicion(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler starts suspicion for fresh message."""
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_self_suspicion(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler refutes self-suspicion."""
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),  # Self
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Should return alive message with incremented incarnation
        assert b"alive:" in result.response
        assert mock_server.udp_addr_slug in result.response


class TestSuspectHandlerNegativePath:
    """Negative path tests for SuspectHandler."""

    @pytest.mark.asyncio
    async def test_handle_suspect_stale_message(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler ignores stale messages."""
        mock_server._is_message_fresh_result = False
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"suspect",
            message=b"suspect:1",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_suspect_no_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler handles missing target gracefully."""
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestSuspectHandlerEdgeCases:
    """Edge case tests for SuspectHandler."""

    @pytest.mark.asyncio
    async def test_handle_self_suspicion_with_embedded_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Self-suspicion includes embedded state."""
        mock_server._embedded_state = b"state_data"
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"alive:" in result.response
        assert b"#|s" in result.response

    @pytest.mark.asyncio
    async def test_handle_suspect_regossip(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler regossips suspicion if needed."""
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        await handler.handle(context)

        # After first suspicion, regossip count should be 1
        assert mock_server.hierarchical_detector._regossip_count == 1

    @pytest.mark.asyncio
    async def test_handle_suspect_no_regossip_second_time(
        self, mock_server: MockServerInterface
    ) -> None:
        """Suspect handler doesn't regossip if already done."""
        mock_server.hierarchical_detector._regossip_count = 1  # Already regossiped
        handler = SuspectHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"suspect",
            message=b"suspect:5",
            clock_time=12345,
        )

        await handler.handle(context)

        # Count should remain 1
        assert mock_server.hierarchical_detector._regossip_count == 1

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """SuspectHandler has correct message_types."""
        handler = SuspectHandler(mock_server)

        assert handler.message_types == (b"suspect",)


class TestSuspicionHandlersConcurrency:
    """Concurrency tests for suspicion handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_alive_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple alive handlers can run concurrently."""
        handler = AliveHandler(mock_server)

        async def handle_alive(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                target=("192.168.1.2", 9001),
                target_addr_bytes=b"192.168.1.2:9001",
                message_type=b"alive",
                message=f"alive:{index}".encode(),
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_alive(i) for i in range(30)]
        await asyncio.gather(*tasks)

        # All senders should be confirmed
        assert len(mock_server._confirmed_peers) == 30

    @pytest.mark.asyncio
    async def test_concurrent_suspect_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple suspect handlers can run concurrently."""
        handler = SuspectHandler(mock_server)

        async def handle_suspect(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                target=("192.168.1.2", 9001),
                target_addr_bytes=b"192.168.1.2:9001",
                message_type=b"suspect",
                message=f"suspect:{index}".encode(),
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_suspect(i) for i in range(30)]
        await asyncio.gather(*tasks)

        assert len(mock_server._confirmed_peers) == 30


class TestSuspicionHandlersFailureModes:
    """Failure mode tests for suspicion handlers."""

    @pytest.mark.asyncio
    async def test_alive_handler_continues_after_error(
        self, mock_server: MockServerInterface
    ) -> None:
        """Alive handler continues after failed operations."""
        handler = AliveHandler(mock_server)

        # First call
        context1 = MessageContext(
            source_addr=("192.168.1.1", 8001),
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"alive",
            message=b"alive:5",
            clock_time=1,
        )
        result1 = await handler.handle(context1)

        # Second call
        context2 = MessageContext(
            source_addr=("192.168.1.1", 8002),
            target=("192.168.1.3", 9002),
            target_addr_bytes=b"192.168.1.3:9002",
            message_type=b"alive",
            message=b"alive:6",
            clock_time=2,
        )
        result2 = await handler.handle(context2)

        # Both should succeed
        assert result1.response.startswith(b"ack>")
        assert result2.response.startswith(b"ack>")
