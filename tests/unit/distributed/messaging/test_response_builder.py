"""
Tests for ResponseBuilder.

Covers:
- Happy path: building ack and nack responses
- Negative path: edge cases in response building
- Edge cases: empty reasons, various handler results
"""

import pytest

from hyperscale.distributed.swim.message_handling.core import ResponseBuilder
from hyperscale.distributed.swim.message_handling.models import HandlerResult

from tests.distributed.messaging.mocks import MockServerInterface


class TestResponseBuilderHappyPath:
    """Happy path tests for ResponseBuilder."""

    def test_build_ack_with_state(self, mock_server: MockServerInterface) -> None:
        """Build ack with embedded state."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_ack(embed_state=True)

        assert result.startswith(b"ack>")
        assert mock_server.udp_addr_slug in result

    def test_build_ack_without_state(self, mock_server: MockServerInterface) -> None:
        """Build ack without embedded state."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_ack(embed_state=False)

        assert result == b"ack>" + mock_server.udp_addr_slug

    def test_build_nack_with_reason(self, mock_server: MockServerInterface) -> None:
        """Build nack with reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack(reason=b"test_reason")

        assert result == b"nack:test_reason>" + mock_server.udp_addr_slug

    def test_build_nack_without_reason(self, mock_server: MockServerInterface) -> None:
        """Build nack without reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack()

        assert result == b"nack>" + mock_server.udp_addr_slug

    def test_finalize_ack_result(self, mock_server: MockServerInterface) -> None:
        """Finalize handler result with ack response."""
        builder = ResponseBuilder(mock_server)
        handler_result = HandlerResult(
            response=b"ack>127.0.0.1:9000",
            embed_state=False,
        )

        result = builder.finalize(handler_result)

        assert result == b"ack>127.0.0.1:9000"

    def test_finalize_nack_result(self, mock_server: MockServerInterface) -> None:
        """Finalize handler result with nack response."""
        builder = ResponseBuilder(mock_server)
        handler_result = HandlerResult(
            response=b"nack:reason>127.0.0.1:9000",
            embed_state=False,
            is_error=True,
        )

        result = builder.finalize(handler_result)

        assert result == b"nack:reason>127.0.0.1:9000"

    def test_finalize_empty_result(self, mock_server: MockServerInterface) -> None:
        """Finalize handler result with empty response."""
        builder = ResponseBuilder(mock_server)
        handler_result = HandlerResult(
            response=b"",
            embed_state=False,
        )

        result = builder.finalize(handler_result)

        assert result == b""


class TestResponseBuilderNackReasons:
    """Tests for various nack reasons."""

    def test_nack_unknown_reason(self, mock_server: MockServerInterface) -> None:
        """Nack with unknown reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack(reason=b"unknown")

        assert b"nack:unknown>" in result

    def test_nack_version_mismatch(self, mock_server: MockServerInterface) -> None:
        """Nack with version_mismatch reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack(reason=b"version_mismatch")

        assert b"nack:version_mismatch>" in result

    def test_nack_error_reason(self, mock_server: MockServerInterface) -> None:
        """Nack with error reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack(reason=b"error")

        assert b"nack:error>" in result


class TestResponseBuilderEdgeCases:
    """Edge case tests for ResponseBuilder."""

    def test_build_ack_default_embeds_state(
        self, mock_server: MockServerInterface
    ) -> None:
        """Default build_ack embeds state."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_ack()

        # Mock server's build_ack_with_state returns ack>addr_slug
        assert result.startswith(b"ack>")

    def test_build_nack_empty_bytes_reason(
        self, mock_server: MockServerInterface
    ) -> None:
        """Nack with empty bytes reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack(reason=b"")

        assert result == b"nack>" + mock_server.udp_addr_slug

    def test_finalize_with_embed_state_true(
        self, mock_server: MockServerInterface
    ) -> None:
        """Finalize with embed_state=True returns response as-is."""
        builder = ResponseBuilder(mock_server)
        handler_result = HandlerResult(
            response=b"ack>127.0.0.1:9000",
            embed_state=True,
        )

        result = builder.finalize(handler_result)

        # Current implementation returns response as-is
        assert result == b"ack>127.0.0.1:9000"

    def test_build_nack_binary_reason(self, mock_server: MockServerInterface) -> None:
        """Nack with binary data in reason."""
        builder = ResponseBuilder(mock_server)

        result = builder.build_nack(reason=b"\x00\xff\xfe")

        assert b"nack:\x00\xff\xfe>" in result

    def test_build_nack_long_reason(self, mock_server: MockServerInterface) -> None:
        """Nack with long reason."""
        builder = ResponseBuilder(mock_server)
        long_reason = b"a" * 1000

        result = builder.build_nack(reason=long_reason)

        assert b"nack:" in result
        assert long_reason in result


class TestResponseBuilderConcurrency:
    """Concurrency tests for ResponseBuilder."""

    @pytest.mark.asyncio
    async def test_concurrent_build_ack(
        self, mock_server: MockServerInterface
    ) -> None:
        """Building acks concurrently is safe."""
        import asyncio

        builder = ResponseBuilder(mock_server)

        async def build_ack_async(index: int) -> bytes:
            return builder.build_ack(embed_state=index % 2 == 0)

        tasks = [build_ack_async(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 100
        assert all(r.startswith(b"ack>") for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_build_nack(
        self, mock_server: MockServerInterface
    ) -> None:
        """Building nacks concurrently is safe."""
        import asyncio

        builder = ResponseBuilder(mock_server)

        async def build_nack_async(index: int) -> bytes:
            reason = f"reason_{index}".encode() if index % 2 == 0 else b""
            return builder.build_nack(reason=reason)

        tasks = [build_nack_async(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 100
        assert all(b"nack" in r for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_finalize(
        self, mock_server: MockServerInterface
    ) -> None:
        """Finalizing results concurrently is safe."""
        import asyncio

        builder = ResponseBuilder(mock_server)

        async def finalize_async(index: int) -> bytes:
            handler_result = HandlerResult(
                response=f"ack>127.0.0.{index}:9000".encode(),
                embed_state=False,
            )
            return builder.finalize(handler_result)

        tasks = [finalize_async(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 100


class TestResponseBuilderFailureModes:
    """Failure mode tests for ResponseBuilder."""

    def test_builder_uses_server_slug(
        self, mock_server: MockServerInterface
    ) -> None:
        """Builder always uses server's udp_addr_slug."""
        mock_server._udp_addr_slug = b"192.168.1.100:9999"
        builder = ResponseBuilder(mock_server)

        ack = builder.build_ack(embed_state=False)
        nack = builder.build_nack()

        assert b"192.168.1.100:9999" in ack
        assert b"192.168.1.100:9999" in nack

    def test_finalize_preserves_is_error_flag(
        self, mock_server: MockServerInterface
    ) -> None:
        """Finalize preserves response regardless of is_error flag."""
        builder = ResponseBuilder(mock_server)

        error_result = HandlerResult(
            response=b"nack>addr",
            embed_state=False,
            is_error=True,
        )
        normal_result = HandlerResult(
            response=b"ack>addr",
            embed_state=False,
            is_error=False,
        )

        assert builder.finalize(error_result) == b"nack>addr"
        assert builder.finalize(normal_result) == b"ack>addr"
