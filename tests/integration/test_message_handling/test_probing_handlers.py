"""
Tests for probing handlers (ProbeHandler, PingReqHandler, PingReqAckHandler).

Covers:
- Happy path: normal probing operations
- Negative path: invalid targets, unknown nodes
- Edge cases: self-targeted probes, timeouts
- Concurrency: parallel handling
"""

import asyncio

import pytest

from hyperscale.distributed_rewrite.swim.message_handling.probing import (
    ProbeHandler,
    PingReqHandler,
    PingReqAckHandler,
)
from hyperscale.distributed_rewrite.swim.message_handling.models import MessageContext

from tests.integration.test_message_handling.mocks import MockServerInterface


class TestProbeHandlerHappyPath:
    """Happy path tests for ProbeHandler."""

    @pytest.mark.asyncio
    async def test_handle_probe_confirms_peer(
        self, mock_server: MockServerInterface
    ) -> None:
        """Probe handler confirms the sender."""
        mock_server.add_node(("192.168.1.2", 9001))
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        await handler.handle(context)

        assert mock_server.is_peer_confirmed(("192.168.1.1", 8000))

    @pytest.mark.asyncio
    async def test_handle_probe_known_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Probe handler processes probe for known target."""
        mock_server.add_node(("192.168.1.2", 9001))
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_self_probe(self, mock_server: MockServerInterface) -> None:
        """Probe about self returns alive message with refutation."""
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("127.0.0.1", 9000),  # Self address
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"alive:" in result.response
        assert mock_server.udp_addr_slug in result.response


class TestProbeHandlerNegativePath:
    """Negative path tests for ProbeHandler."""

    @pytest.mark.asyncio
    async def test_handle_probe_invalid_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Probe handler rejects invalid target."""
        mock_server._validate_target_result = False
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response

    @pytest.mark.asyncio
    async def test_handle_probe_unknown_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Probe handler returns nack for unknown target."""
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.99", 9001),  # Unknown node
            target_addr_bytes=b"192.168.1.99:9001",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response
        assert b"unknown" in result.response


class TestProbeHandlerEdgeCases:
    """Edge case tests for ProbeHandler."""

    @pytest.mark.asyncio
    async def test_handle_self_probe_with_embedded_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Self-probe includes embedded state if available."""
        mock_server._embedded_state = b"test_state_data"
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"alive:" in result.response
        assert b"#|s" in result.response  # State separator

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """ProbeHandler has correct message_types."""
        handler = ProbeHandler(mock_server)

        assert handler.message_types == (b"probe",)


class TestPingReqHandlerHappyPath:
    """Happy path tests for PingReqHandler."""

    @pytest.mark.asyncio
    async def test_handle_ping_req_known_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req handler probes known target and returns alive."""
        mock_server.add_node(("192.168.1.2", 9001))
        handler = PingReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"ping-req",
            message=b"ping-req",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"ping-req-ack:alive>" in result.response

    @pytest.mark.asyncio
    async def test_handle_ping_req_self_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req for self returns alive immediately."""
        handler = PingReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("127.0.0.1", 9000),  # Self
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"ping-req",
            message=b"ping-req",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"ping-req-ack:alive>" in result.response


class TestPingReqHandlerNegativePath:
    """Negative path tests for PingReqHandler."""

    @pytest.mark.asyncio
    async def test_handle_ping_req_null_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req handler rejects null target."""
        handler = PingReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"ping-req",
            message=b"ping-req",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"nack" in result.response
        assert b"invalid" in result.response

    @pytest.mark.asyncio
    async def test_handle_ping_req_unknown_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req handler returns unknown for missing target."""
        handler = PingReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.99", 9001),  # Unknown
            target_addr_bytes=b"192.168.1.99:9001",
            message_type=b"ping-req",
            message=b"ping-req",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"ping-req-ack:unknown>" in result.response


class TestPingReqHandlerEdgeCases:
    """Edge case tests for PingReqHandler."""

    @pytest.mark.asyncio
    async def test_handle_ping_req_self_with_embedded_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req for self includes embedded state."""
        mock_server._embedded_state = b"state_data"
        handler = PingReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("127.0.0.1", 9000),
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"ping-req",
            message=b"ping-req",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert b"ping-req-ack:alive>" in result.response
        assert b"#|s" in result.response

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """PingReqHandler has correct message_types."""
        handler = PingReqHandler(mock_server)

        assert handler.message_types == (b"ping-req",)


class TestPingReqAckHandlerHappyPath:
    """Happy path tests for PingReqAckHandler."""

    @pytest.mark.asyncio
    async def test_handle_ping_req_ack_alive(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req-ack with alive status processes correctly."""
        mock_server.indirect_probe_manager.add_pending_probe(("192.168.1.2", 9001))
        handler = PingReqAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"ping-req-ack",
            message=b"ping-req-ack:alive",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_ping_req_ack_dead(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req-ack with dead status processes correctly."""
        mock_server.indirect_probe_manager.add_pending_probe(("192.168.1.2", 9001))
        handler = PingReqAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"ping-req-ack",
            message=b"ping-req-ack:dead",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_ping_req_ack_timeout(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req-ack with timeout status processes correctly."""
        mock_server.indirect_probe_manager.add_pending_probe(("192.168.1.2", 9001))
        handler = PingReqAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"ping-req-ack",
            message=b"ping-req-ack:timeout",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestPingReqAckHandlerNegativePath:
    """Negative path tests for PingReqAckHandler."""

    @pytest.mark.asyncio
    async def test_handle_ping_req_ack_no_pending_probe(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req-ack without pending probe logs error."""
        handler = PingReqAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"ping-req-ack",
            message=b"ping-req-ack:alive",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Still returns ack but logs error
        assert result.response.startswith(b"ack>")
        assert len(mock_server._errors) >= 1


class TestPingReqAckHandlerEdgeCases:
    """Edge case tests for PingReqAckHandler."""

    @pytest.mark.asyncio
    async def test_handle_ping_req_ack_unknown_status(
        self, mock_server: MockServerInterface
    ) -> None:
        """Ping-req-ack with unknown status in message."""
        mock_server.indirect_probe_manager.add_pending_probe(("192.168.1.2", 9001))
        handler = PingReqAckHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"ping-req-ack",
            message=b"ping-req-ack:unknown",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_parse_status_alive(self, mock_server: MockServerInterface) -> None:
        """Parse status correctly extracts alive."""
        handler = PingReqAckHandler(mock_server)

        status = handler._parse_status(b"ping-req-ack:alive>127.0.0.1:9000")

        assert status == b"alive"

    @pytest.mark.asyncio
    async def test_parse_status_dead(self, mock_server: MockServerInterface) -> None:
        """Parse status correctly extracts dead."""
        handler = PingReqAckHandler(mock_server)

        status = handler._parse_status(b"ping-req-ack:dead>127.0.0.1:9000")

        assert status == b"dead"

    @pytest.mark.asyncio
    async def test_parse_status_timeout(self, mock_server: MockServerInterface) -> None:
        """Parse status correctly extracts timeout."""
        handler = PingReqAckHandler(mock_server)

        status = handler._parse_status(b"ping-req-ack:timeout>127.0.0.1:9000")

        assert status == b"timeout"

    @pytest.mark.asyncio
    async def test_parse_status_empty_message(
        self, mock_server: MockServerInterface
    ) -> None:
        """Parse status handles empty message."""
        handler = PingReqAckHandler(mock_server)

        status = handler._parse_status(b"ping-req-ack")

        assert status == b""

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """PingReqAckHandler has correct message_types."""
        handler = PingReqAckHandler(mock_server)

        assert handler.message_types == (b"ping-req-ack",)


class TestProbingHandlersConcurrency:
    """Concurrency tests for probing handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_probe_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple probes can run concurrently."""
        mock_server.add_node(("192.168.1.2", 9001))
        handler = ProbeHandler(mock_server)

        async def handle_probe(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                source_addr_string=f"192.168.1.1:{8000 + index}",
                target=("192.168.1.2", 9001),
                target_addr_bytes=b"192.168.1.2:9001",
                message_type=b"probe",
                message=b"probe",
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_probe(i) for i in range(30)]
        await asyncio.gather(*tasks)

        # All senders should be confirmed
        assert len(mock_server._confirmed_peers) == 30

    @pytest.mark.asyncio
    async def test_concurrent_ping_req_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple ping-reqs can run concurrently."""
        handler = PingReqHandler(mock_server)

        async def handle_ping_req(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000),
                source_addr_string="192.168.1.1:8000",
                target=("127.0.0.1", 9000),  # Self
                target_addr_bytes=b"127.0.0.1:9000",
                message_type=b"ping-req",
                message=b"ping-req",
                clock_time=index,
            )
            result = await handler.handle(context)
            assert b"ping-req-ack:alive>" in result.response

        tasks = [handle_ping_req(i) for i in range(30)]
        await asyncio.gather(*tasks)


class TestProbingHandlersFailureModes:
    """Failure mode tests for probing handlers."""

    @pytest.mark.asyncio
    async def test_probe_forwards_to_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Probe handler forwards probe to target via task runner."""
        mock_server.add_node(("192.168.1.2", 9001))
        handler = ProbeHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"probe",
            message=b"probe",
            clock_time=12345,
        )

        await handler.handle(context)

        # Task should be submitted
        assert len(mock_server.task_runner._tasks) >= 1
