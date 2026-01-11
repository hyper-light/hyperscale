"""
Tests for cross-cluster handlers (XProbeHandler, XAckHandler, XNackHandler).

Covers:
- Happy path: normal cross-cluster operations
- Negative path: rejected probes
- Edge cases: binary data handling
- Concurrency: parallel handling
"""

import asyncio

import pytest

from hyperscale.distributed_rewrite.swim.message_handling.cross_cluster import (
    XProbeHandler,
    XAckHandler,
    XNackHandler,
)
from hyperscale.distributed_rewrite.swim.message_handling.models import MessageContext

from tests.integration.test_message_handling.mocks import MockServerInterface


class TestXProbeHandlerHappyPath:
    """Happy path tests for XProbeHandler."""

    @pytest.mark.asyncio
    async def test_handle_xprobe_default_returns_xnack(
        self, mock_server: MockServerInterface
    ) -> None:
        """Default XProbeHandler returns xnack (not a DC leader)."""
        handler = XProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"\x80\x04\x95\x10\x00",  # Binary pickle data
            message_type=b"xprobe",
            message=b"xprobe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"xnack>" in result.response
        assert mock_server.udp_addr_slug in result.response

    @pytest.mark.asyncio
    async def test_handle_xprobe_with_binary_data(
        self, mock_server: MockServerInterface
    ) -> None:
        """XProbeHandler handles binary probe data."""
        handler = XProbeHandler(mock_server)
        binary_data = bytes([0x80, 0x04, 0x95, 0x10, 0x00, 0xff, 0xfe])
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=binary_data,
            message_type=b"xprobe",
            message=b"xprobe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Default implementation returns xnack
        assert b"xnack>" in result.response


class TestXProbeHandlerCustomResponder:
    """Tests for XProbeHandler with custom server responder."""

    @pytest.mark.asyncio
    async def test_handle_xprobe_custom_response(
        self, mock_server: MockServerInterface
    ) -> None:
        """XProbeHandler uses server's build_xprobe_response for custom xack response."""
        # Configure mock server to return custom response
        async def custom_build_xprobe_response(
            source_addr: tuple[str, int], probe_data: bytes
        ) -> bytes | None:
            return b"custom_ack_data"

        mock_server.build_xprobe_response = custom_build_xprobe_response

        handler = XProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"probe_data",
            message_type=b"xprobe",
            message=b"xprobe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"xack>" in result.response
        assert b"custom_ack_data" in result.response


class TestXProbeHandlerEdgeCases:
    """Edge case tests for XProbeHandler."""

    @pytest.mark.asyncio
    async def test_handle_xprobe_empty_target_addr_bytes(
        self, mock_server: MockServerInterface
    ) -> None:
        """XProbeHandler handles empty target_addr_bytes."""
        handler = XProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=None,
            message_type=b"xprobe",
            message=b"xprobe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"xnack>" in result.response

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """XProbeHandler has correct message_types."""
        handler = XProbeHandler(mock_server)

        assert handler.message_types == (b"xprobe",)


class TestXAckHandlerHappyPath:
    """Happy path tests for XAckHandler."""

    @pytest.mark.asyncio
    async def test_handle_xack_default_no_op(
        self, mock_server: MockServerInterface
    ) -> None:
        """Default XAckHandler is a no-op and returns empty response."""
        handler = XAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"\x80\x04\x95\x20\x00",  # Binary pickle data
            message_type=b"xack",
            message=b"xack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Default returns empty response
        assert result.response == b""

    @pytest.mark.asyncio
    async def test_handle_xack_with_binary_data(
        self, mock_server: MockServerInterface
    ) -> None:
        """XAckHandler handles binary ack data."""
        handler = XAckHandler(mock_server)
        binary_data = bytes([0x80, 0x04, 0x95, 0x20, 0x00, 0xff, 0xfe])
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=binary_data,
            message_type=b"xack",
            message=b"xack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response == b""


class TestXAckHandlerCustomProcessor:
    """Tests for XAckHandler with custom server processor."""

    @pytest.mark.asyncio
    async def test_handle_xack_custom_processing(
        self, mock_server: MockServerInterface
    ) -> None:
        """XAckHandler uses server's handle_xack_response for custom processing."""
        processed_data = []

        # Configure mock server to capture processed data
        async def custom_handle_xack_response(
            source_addr: tuple[str, int], response_data: bytes
        ) -> None:
            processed_data.append((source_addr, response_data))

        mock_server.handle_xack_response = custom_handle_xack_response

        handler = XAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"ack_data",
            message_type=b"xack",
            message=b"xack",
            clock_time=12345,
        )

        await handler.handle(context)

        assert len(processed_data) == 1
        assert processed_data[0] == (("192.168.1.1", 8000), b"ack_data")


class TestXAckHandlerEdgeCases:
    """Edge case tests for XAckHandler."""

    @pytest.mark.asyncio
    async def test_handle_xack_empty_target_addr_bytes(
        self, mock_server: MockServerInterface
    ) -> None:
        """XAckHandler handles empty target_addr_bytes."""
        handler = XAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=None,
            message_type=b"xack",
            message=b"xack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response == b""

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """XAckHandler has correct message_types."""
        handler = XAckHandler(mock_server)

        assert handler.message_types == (b"xack",)


class TestXNackHandlerHappyPath:
    """Happy path tests for XNackHandler."""

    @pytest.mark.asyncio
    async def test_handle_xnack_returns_empty(
        self, mock_server: MockServerInterface
    ) -> None:
        """XNackHandler returns empty response (probe will timeout)."""
        handler = XNackHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"xnack",
            message=b"xnack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response == b""

    @pytest.mark.asyncio
    async def test_handle_xnack_ignores_rejection(
        self, mock_server: MockServerInterface
    ) -> None:
        """XNackHandler ignores rejection - probe will timeout naturally."""
        handler = XNackHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=None,
            target_addr_bytes=None,
            message_type=b"xnack",
            message=b"xnack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # No errors logged, just ignored
        assert result.response == b""
        assert len(mock_server._errors) == 0


class TestXNackHandlerEdgeCases:
    """Edge case tests for XNackHandler."""

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """XNackHandler has correct message_types."""
        handler = XNackHandler(mock_server)

        assert handler.message_types == (b"xnack",)


class TestCrossClusterHandlersConcurrency:
    """Concurrency tests for cross-cluster handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_xprobe_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple xprobe handlers can run concurrently."""
        handler = XProbeHandler(mock_server)

        async def handle_xprobe(index: int) -> bytes:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                target=("127.0.0.1", 9000),
                target_addr_bytes=f"probe_{index}".encode(),
                message_type=b"xprobe",
                message=b"xprobe",
                clock_time=index,
            )
            result = await handler.handle(context)
            return result.response

        tasks = [handle_xprobe(i) for i in range(30)]
        results = await asyncio.gather(*tasks)

        # All should return xnack
        assert all(b"xnack>" in r for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_xack_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple xack handlers can run concurrently."""
        handler = XAckHandler(mock_server)

        async def handle_xack(index: int) -> bytes:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                target=("127.0.0.1", 9000),
                target_addr_bytes=f"ack_{index}".encode(),
                message_type=b"xack",
                message=b"xack",
                clock_time=index,
            )
            result = await handler.handle(context)
            return result.response

        tasks = [handle_xack(i) for i in range(30)]
        results = await asyncio.gather(*tasks)

        # All should return empty
        assert all(r == b"" for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_xnack_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple xnack handlers can run concurrently."""
        handler = XNackHandler(mock_server)

        async def handle_xnack(index: int) -> bytes:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                target=("127.0.0.1", 9000),
                target_addr_bytes=f"nack_{index}".encode(),
                message_type=b"xnack",
                message=b"xnack",
                clock_time=index,
            )
            result = await handler.handle(context)
            return result.response

        tasks = [handle_xnack(i) for i in range(30)]
        results = await asyncio.gather(*tasks)

        # All should return empty
        assert all(r == b"" for r in results)


class TestCrossClusterHandlersFailureModes:
    """Failure mode tests for cross-cluster handlers."""

    @pytest.mark.asyncio
    async def test_xprobe_handler_handles_large_binary_data(
        self, mock_server: MockServerInterface
    ) -> None:
        """XProbeHandler handles large binary data."""
        handler = XProbeHandler(mock_server)
        large_data = bytes(range(256)) * 100  # 25.6KB of binary data
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=large_data,
            message_type=b"xprobe",
            message=b"xprobe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"xnack>" in result.response

    @pytest.mark.asyncio
    async def test_xack_handler_handles_null_bytes(
        self, mock_server: MockServerInterface
    ) -> None:
        """XAckHandler handles data with null bytes."""
        handler = XAckHandler(mock_server)
        null_data = b"data\x00with\x00nulls\x00"
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            target=("127.0.0.1", 9000),
            target_addr_bytes=null_data,
            message_type=b"xack",
            message=b"xack",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Should not crash
        assert result.response == b""

    @pytest.mark.asyncio
    async def test_handlers_are_stateless(
        self, mock_server: MockServerInterface
    ) -> None:
        """Cross-cluster handlers are stateless between calls."""
        xprobe = XProbeHandler(mock_server)
        xack = XAckHandler(mock_server)
        xnack = XNackHandler(mock_server)

        for i in range(5):
            probe_ctx = MessageContext(
                source_addr=("192.168.1.1", 8000 + i),
                target=("127.0.0.1", 9000),
                target_addr_bytes=f"data_{i}".encode(),
                message_type=b"xprobe",
                message=b"xprobe",
                clock_time=i,
            )
            ack_ctx = MessageContext(
                source_addr=("192.168.1.2", 8000 + i),
                target=("127.0.0.1", 9000),
                target_addr_bytes=f"ack_{i}".encode(),
                message_type=b"xack",
                message=b"xack",
                clock_time=i,
            )
            nack_ctx = MessageContext(
                source_addr=("192.168.1.3", 8000 + i),
                target=("127.0.0.1", 9000),
                target_addr_bytes=f"nack_{i}".encode(),
                message_type=b"xnack",
                message=b"xnack",
                clock_time=i,
            )

            probe_result = await xprobe.handle(probe_ctx)
            ack_result = await xack.handle(ack_ctx)
            nack_result = await xnack.handle(nack_ctx)

            assert b"xnack>" in probe_result.response
            assert ack_result.response == b""
            assert nack_result.response == b""
