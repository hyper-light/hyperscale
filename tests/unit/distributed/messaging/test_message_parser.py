"""
Tests for MessageParser.

Covers:
- Happy path: parsing various message formats
- Negative path: malformed messages
- Edge cases: empty data, boundary conditions
- Piggyback extraction
"""

import pytest

from hyperscale.distributed.swim.message_handling.core import MessageParser
from hyperscale.distributed.swim.message_handling.models import MessageContext

from tests.unit.distributed.messaging.mocks import MockServerInterface


class TestMessageParserHappyPath:
    """Happy path tests for MessageParser."""

    def test_parse_simple_ack_message(self, mock_server: MockServerInterface) -> None:
        """Parse a simple ack message."""
        parser = MessageParser(mock_server)
        source_addr = ("192.168.1.1", 8000)
        data = b"ack>127.0.0.1:9000"
        clock_time = 12345

        result = parser.parse(source_addr, data, clock_time)

        assert result.context.source_addr == source_addr
        assert result.context.message_type == b"ack"
        assert result.context.target == ("127.0.0.1", 9000)
        assert result.context.clock_time == clock_time
        assert result.context.source_addr_string == "192.168.1.1:8000"

    def test_parse_message_with_incarnation(
        self, mock_server: MockServerInterface
    ) -> None:
        """Parse message with incarnation number."""
        parser = MessageParser(mock_server)
        data = b"alive:5>127.0.0.1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"alive"
        assert result.context.message == b"alive:5"
        assert result.context.get_message_payload() == b"5"

    def test_parse_join_message_with_version(
        self, mock_server: MockServerInterface
    ) -> None:
        """Parse join message with version prefix."""
        parser = MessageParser(mock_server)
        data = b"join>v1.0|192.168.1.2:9001"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"join"
        assert result.context.target_addr_bytes == b"v1.0|192.168.1.2:9001"

    def test_parse_probe_message(self, mock_server: MockServerInterface) -> None:
        """Parse probe message."""
        parser = MessageParser(mock_server)
        data = b"probe>192.168.1.2:9001"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"probe"
        assert result.context.target == ("192.168.1.2", 9001)

    def test_parse_leadership_message(self, mock_server: MockServerInterface) -> None:
        """Parse leadership message with term."""
        parser = MessageParser(mock_server)
        data = b"leader-heartbeat:5>192.168.1.2:9001"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"leader-heartbeat"
        assert result.context.message == b"leader-heartbeat:5"


class TestMessageParserPiggyback:
    """Tests for piggyback extraction."""

    def test_extract_health_piggyback(self, mock_server: MockServerInterface) -> None:
        """Extract health gossip piggyback."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1:9000#|hentry1;entry2"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.health_piggyback == b"#|hentry1;entry2"
        assert result.context.message_type == b"ack"

    def test_extract_membership_piggyback(
        self, mock_server: MockServerInterface
    ) -> None:
        """Extract membership piggyback."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1:9000#|mOK:1:192.168.1.2:9001"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.membership_piggyback == b"#|mOK:1:192.168.1.2:9001"
        assert result.context.message_type == b"ack"

    def test_extract_both_piggybacks(self, mock_server: MockServerInterface) -> None:
        """Extract both health and membership piggyback."""
        parser = MessageParser(mock_server)
        # Health comes after membership in real protocol
        data = b"ack>127.0.0.1:9000#|mOK:1:192.168.1.2:9001#|hentry1"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        # Health is extracted first, then membership from remaining
        assert result.health_piggyback == b"#|hentry1"
        assert result.membership_piggyback == b"#|mOK:1:192.168.1.2:9001"

    def test_no_piggyback(self, mock_server: MockServerInterface) -> None:
        """Message without piggyback."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.health_piggyback is None
        assert result.membership_piggyback is None


class TestMessageParserCrossCluster:
    """Tests for cross-cluster message parsing."""

    def test_parse_xprobe_message(self, mock_server: MockServerInterface) -> None:
        """Parse xprobe message - binary data not parsed as host:port."""
        parser = MessageParser(mock_server)
        source_addr = ("192.168.1.1", 8000)
        data = b"xprobe>\x80\x04\x95\x10\x00"  # Binary pickle data

        result = parser.parse(source_addr, data, 0)

        assert result.context.message_type == b"xprobe"
        # Target should be source for response routing
        assert result.context.target == source_addr
        assert result.context.target_addr_bytes == b"\x80\x04\x95\x10\x00"

    def test_parse_xack_message(self, mock_server: MockServerInterface) -> None:
        """Parse xack message."""
        parser = MessageParser(mock_server)
        source_addr = ("192.168.1.1", 8000)
        data = b"xack>\x80\x04\x95\x20\x00"

        result = parser.parse(source_addr, data, 0)

        assert result.context.message_type == b"xack"
        assert result.context.target == source_addr

    def test_parse_xnack_message(self, mock_server: MockServerInterface) -> None:
        """Parse xnack message."""
        parser = MessageParser(mock_server)
        source_addr = ("192.168.1.1", 8000)
        data = b"xnack>127.0.0.1:9000"

        result = parser.parse(source_addr, data, 0)

        assert result.context.message_type == b"xnack"


class TestMessageParserEmbeddedState:
    """Tests for embedded state extraction."""

    def test_extract_embedded_state(self, mock_server: MockServerInterface) -> None:
        """Extract base64 embedded state from message."""
        processed_states = []

        def callback(state_data: bytes, source: tuple[str, int]) -> None:
            processed_states.append((state_data, source))

        parser = MessageParser(mock_server, process_embedded_state=callback)
        # SGVsbG8= is base64 for "Hello"
        data = b"ack>127.0.0.1:9000#|sSGVsbG8="

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert len(processed_states) == 1
        assert processed_states[0][0] == b"Hello"
        assert processed_states[0][1] == ("192.168.1.1", 8000)
        # Target address should have state stripped
        assert result.context.target == ("127.0.0.1", 9000)

    def test_invalid_base64_state_ignored(
        self, mock_server: MockServerInterface
    ) -> None:
        """Invalid base64 state is silently ignored."""
        processed_states = []

        def callback(state_data: bytes, source: tuple[str, int]) -> None:
            processed_states.append(state_data)

        parser = MessageParser(mock_server, process_embedded_state=callback)
        data = b"ack>127.0.0.1:9000#|s!!!invalid!!!"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        # Should not crash, state ignored
        assert len(processed_states) == 0
        assert result.context.message_type == b"ack"


class TestMessageParserNegativePath:
    """Negative path tests for MessageParser."""

    def test_message_without_target(self, mock_server: MockServerInterface) -> None:
        """Parse message without target address."""
        parser = MessageParser(mock_server)
        data = b"ack"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"ack"
        assert result.context.target is None
        assert result.context.target_addr_bytes is None

    def test_message_with_invalid_port(self, mock_server: MockServerInterface) -> None:
        """Parse message with invalid port number."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1:invalid"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"ack"
        assert result.context.target is None  # Invalid port

    def test_message_with_missing_port(self, mock_server: MockServerInterface) -> None:
        """Parse message with missing port."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.target is None

    def test_empty_message_type(self, mock_server: MockServerInterface) -> None:
        """Parse message with empty type."""
        parser = MessageParser(mock_server)
        data = b">127.0.0.1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b""


class TestMessageParserEdgeCases:
    """Edge case tests for MessageParser."""

    def test_empty_data(self, mock_server: MockServerInterface) -> None:
        """Parse empty data."""
        parser = MessageParser(mock_server)
        data = b""

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b""
        assert result.context.target is None

    def test_very_long_message(self, mock_server: MockServerInterface) -> None:
        """Parse very long message."""
        parser = MessageParser(mock_server)
        long_payload = b"x" * 10000
        data = b"probe>" + long_payload

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"probe"
        assert result.context.target_addr_bytes == long_payload

    def test_message_with_multiple_colons(
        self, mock_server: MockServerInterface
    ) -> None:
        """Parse message with multiple colons in payload."""
        parser = MessageParser(mock_server)
        data = b"leader-claim:5:100:extra>127.0.0.1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.message_type == b"leader-claim"
        # Only first colon splits type from payload
        assert result.context.get_message_payload() == b"5:100:extra"

    def test_message_with_ipv6_address(self, mock_server: MockServerInterface) -> None:
        """Parse message with IPv6-like address."""
        parser = MessageParser(mock_server)
        # IPv6 addresses have multiple colons, need special handling
        # Current implementation expects host:port format
        data = b"ack>::1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        # Should parse but target may be invalid due to IPv6 format
        assert result.context.message_type == b"ack"

    def test_unicode_in_address(self, mock_server: MockServerInterface) -> None:
        """Parse message with unicode in address (should fail gracefully)."""
        parser = MessageParser(mock_server)
        data = "ack>127.0.0.1:9000".encode() + b"\xff\xfe"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        # Should not crash
        assert result.context.message_type == b"ack"

    def test_zero_clock_time(self, mock_server: MockServerInterface) -> None:
        """Parse with zero clock time."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, 0)

        assert result.context.clock_time == 0

    def test_negative_clock_time(self, mock_server: MockServerInterface) -> None:
        """Parse with negative clock time (edge case)."""
        parser = MessageParser(mock_server)
        data = b"ack>127.0.0.1:9000"

        result = parser.parse(("192.168.1.1", 8000), data, -1)

        assert result.context.clock_time == -1


class TestMessageParserConcurrency:
    """Concurrency tests for MessageParser."""

    @pytest.mark.asyncio
    async def test_concurrent_parsing(self, mock_server: MockServerInterface) -> None:
        """Parse messages concurrently."""
        import asyncio

        parser = MessageParser(mock_server)

        async def parse_message(msg_id: int) -> MessageContext:
            data = f"probe>192.168.1.{msg_id}:9000".encode()
            result = parser.parse(("192.168.1.1", 8000), data, msg_id)
            return result.context

        # Parse 100 messages concurrently
        tasks = [parse_message(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        # Verify all parsed correctly
        assert len(results) == 100
        for i, ctx in enumerate(results):
            assert ctx.message_type == b"probe"
            assert ctx.clock_time == i

    @pytest.mark.asyncio
    async def test_parser_is_stateless(self, mock_server: MockServerInterface) -> None:
        """Verify parser is stateless between calls."""
        parser = MessageParser(mock_server)

        # Parse different message types
        r1 = parser.parse(("192.168.1.1", 8000), b"ack>127.0.0.1:9000", 1)
        r2 = parser.parse(("192.168.1.2", 8001), b"probe>127.0.0.1:9001", 2)
        r3 = parser.parse(("192.168.1.3", 8002), b"join>v1.0|127.0.0.1:9002", 3)

        # Each result should be independent
        assert r1.context.message_type == b"ack"
        assert r2.context.message_type == b"probe"
        assert r3.context.message_type == b"join"
        assert r1.context.source_addr != r2.context.source_addr
