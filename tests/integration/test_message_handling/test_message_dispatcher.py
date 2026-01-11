"""
Tests for MessageDispatcher.

Covers:
- Happy path: routing messages to handlers
- Negative path: unknown message types, handler errors
- Edge cases: registration conflicts, empty handlers
- Concurrency: parallel dispatching
"""

import asyncio
from typing import ClassVar

import pytest

from hyperscale.distributed_rewrite.swim.message_handling.core import (
    BaseHandler,
    MessageDispatcher,
    MessageParser,
    ResponseBuilder,
)
from hyperscale.distributed_rewrite.swim.message_handling.models import (
    HandlerResult,
    MessageContext,
)

from tests.integration.test_message_handling.mocks import MockServerInterface


class MockHandler(BaseHandler):
    """Simple mock handler for testing."""

    message_types: ClassVar[tuple[bytes, ...]] = (b"test",)

    def __init__(self, server: MockServerInterface) -> None:
        super().__init__(server)
        self.handled_contexts: list[MessageContext] = []

    async def handle(self, context: MessageContext) -> HandlerResult:
        self.handled_contexts.append(context)
        return self._ack()


class MockHandlerMultipleTypes(BaseHandler):
    """Handler that processes multiple message types."""

    message_types: ClassVar[tuple[bytes, ...]] = (b"type-a", b"type-b", b"type-c")

    async def handle(self, context: MessageContext) -> HandlerResult:
        return self._ack()


class FailingHandler(BaseHandler):
    """Handler that raises an exception."""

    message_types: ClassVar[tuple[bytes, ...]] = (b"fail",)

    async def handle(self, context: MessageContext) -> HandlerResult:
        raise ValueError("Handler intentionally failed")


class NackHandler(BaseHandler):
    """Handler that returns a nack."""

    message_types: ClassVar[tuple[bytes, ...]] = (b"nack-test",)

    async def handle(self, context: MessageContext) -> HandlerResult:
        return self._nack(b"test_reason")


class EmptyResponseHandler(BaseHandler):
    """Handler that returns empty response."""

    message_types: ClassVar[tuple[bytes, ...]] = (b"empty",)

    async def handle(self, context: MessageContext) -> HandlerResult:
        return self._empty()


class TestMessageDispatcherHappyPath:
    """Happy path tests for MessageDispatcher."""

    @pytest.mark.asyncio
    async def test_dispatch_routes_to_handler(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch routes message to registered handler."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandler(mock_server)
        dispatcher.register(handler)

        result = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"test>127.0.0.1:9000", 12345
        )

        assert len(handler.handled_contexts) == 1
        assert handler.handled_contexts[0].message_type == b"test"
        assert result.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_dispatch_multiple_message_types(
        self, mock_server: MockServerInterface
    ) -> None:
        """Handler with multiple message types receives all."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandlerMultipleTypes(mock_server)
        dispatcher.register(handler)

        await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"type-a>127.0.0.1:9000", 0
        )
        await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"type-b>127.0.0.1:9000", 0
        )
        await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"type-c>127.0.0.1:9000", 0
        )

        assert dispatcher.get_handler(b"type-a") is handler
        assert dispatcher.get_handler(b"type-b") is handler
        assert dispatcher.get_handler(b"type-c") is handler

    @pytest.mark.asyncio
    async def test_registered_types_property(
        self, mock_server: MockServerInterface
    ) -> None:
        """Verify registered_types returns all message types."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(MockHandler(mock_server))
        dispatcher.register(MockHandlerMultipleTypes(mock_server))

        registered = dispatcher.registered_types

        assert b"test" in registered
        assert b"type-a" in registered
        assert b"type-b" in registered
        assert b"type-c" in registered

    @pytest.mark.asyncio
    async def test_get_handler_returns_correct_handler(
        self, mock_server: MockServerInterface
    ) -> None:
        """get_handler returns the registered handler."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandler(mock_server)
        dispatcher.register(handler)

        retrieved = dispatcher.get_handler(b"test")

        assert retrieved is handler

    @pytest.mark.asyncio
    async def test_unregister_handler(
        self, mock_server: MockServerInterface
    ) -> None:
        """Unregister removes handler."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandler(mock_server)
        dispatcher.register(handler)

        result = dispatcher.unregister(b"test")

        assert result is True
        assert dispatcher.get_handler(b"test") is None


class TestMessageDispatcherNegativePath:
    """Negative path tests for MessageDispatcher."""

    @pytest.mark.asyncio
    async def test_dispatch_unknown_message_type(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch returns nack for unknown message type."""
        dispatcher = MessageDispatcher(mock_server)

        result = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"unknown>127.0.0.1:9000", 0
        )

        assert b"nack" in result
        assert len(mock_server._errors) == 1

    @pytest.mark.asyncio
    async def test_dispatch_handler_exception(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch catches handler exceptions and returns nack."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(FailingHandler(mock_server))

        result = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"fail>127.0.0.1:9000", 0
        )

        assert b"nack" in result
        assert b"error" in result
        assert len(mock_server._errors) == 1
        assert isinstance(mock_server._errors[0], ValueError)

    def test_register_duplicate_message_type(
        self, mock_server: MockServerInterface
    ) -> None:
        """Register raises error for duplicate message type."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(MockHandler(mock_server))

        with pytest.raises(ValueError) as exc_info:
            dispatcher.register(MockHandler(mock_server))

        assert b"test" in str(exc_info.value).encode()
        assert "already registered" in str(exc_info.value)

    def test_unregister_nonexistent_type(
        self, mock_server: MockServerInterface
    ) -> None:
        """Unregister returns False for nonexistent type."""
        dispatcher = MessageDispatcher(mock_server)

        result = dispatcher.unregister(b"nonexistent")

        assert result is False

    @pytest.mark.asyncio
    async def test_get_handler_nonexistent(
        self, mock_server: MockServerInterface
    ) -> None:
        """get_handler returns None for nonexistent type."""
        dispatcher = MessageDispatcher(mock_server)

        result = dispatcher.get_handler(b"nonexistent")

        assert result is None


class TestMessageDispatcherEdgeCases:
    """Edge case tests for MessageDispatcher."""

    @pytest.mark.asyncio
    async def test_dispatch_empty_message(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch handles empty message."""
        dispatcher = MessageDispatcher(mock_server)

        result = await dispatcher.dispatch(("192.168.1.1", 8000), b"", 0)

        # Empty message type is unknown
        assert b"nack" in result

    @pytest.mark.asyncio
    async def test_dispatch_handler_returns_nack(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch properly returns handler nack response."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(NackHandler(mock_server))

        result = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"nack-test>127.0.0.1:9000", 0
        )

        assert b"nack" in result
        assert b"test_reason" in result

    @pytest.mark.asyncio
    async def test_dispatch_handler_returns_empty(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch properly returns empty response."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(EmptyResponseHandler(mock_server))

        result = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"empty>127.0.0.1:9000", 0
        )

        assert result == b""

    @pytest.mark.asyncio
    async def test_custom_parser_and_builder(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatcher uses custom parser and builder if provided."""
        parser = MessageParser(mock_server)
        builder = ResponseBuilder(mock_server)
        dispatcher = MessageDispatcher(
            mock_server, parser=parser, response_builder=builder
        )

        assert dispatcher._parser is parser
        assert dispatcher._response_builder is builder

    @pytest.mark.asyncio
    async def test_dispatch_preserves_clock_time(
        self, mock_server: MockServerInterface
    ) -> None:
        """Dispatch passes clock_time to parser."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandler(mock_server)
        dispatcher.register(handler)

        clock_time = 987654321
        await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"test>127.0.0.1:9000", clock_time
        )

        assert handler.handled_contexts[0].clock_time == clock_time


class TestMessageDispatcherConcurrency:
    """Concurrency tests for MessageDispatcher."""

    @pytest.mark.asyncio
    async def test_concurrent_dispatch(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple dispatches can run concurrently."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandler(mock_server)
        dispatcher.register(handler)

        async def dispatch_one(index: int) -> bytes:
            return await dispatcher.dispatch(
                ("192.168.1.1", 8000 + index),
                f"test>127.0.0.{index}:9000".encode(),
                index,
            )

        # Dispatch 50 messages concurrently
        tasks = [dispatch_one(i) for i in range(50)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 50
        assert all(r.startswith(b"ack>") for r in results)
        assert len(handler.handled_contexts) == 50

    @pytest.mark.asyncio
    async def test_concurrent_register_and_dispatch(
        self, mock_server: MockServerInterface
    ) -> None:
        """Registration and dispatch can interleave safely."""
        dispatcher = MessageDispatcher(mock_server)

        # Register handler for type-a
        dispatcher.register(MockHandler(mock_server))

        async def dispatch_test() -> bytes:
            return await dispatcher.dispatch(
                ("192.168.1.1", 8000), b"test>127.0.0.1:9000", 0
            )

        # Run multiple dispatches
        tasks = [dispatch_test() for _ in range(20)]
        results = await asyncio.gather(*tasks)

        assert all(r.startswith(b"ack>") for r in results)

    @pytest.mark.asyncio
    async def test_dispatcher_is_stateless(
        self, mock_server: MockServerInterface
    ) -> None:
        """Each dispatch is independent."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(MockHandler(mock_server))

        # Dispatch different messages
        r1 = await dispatcher.dispatch(
            ("192.168.1.1", 8001), b"test>127.0.0.1:9001", 1
        )
        r2 = await dispatcher.dispatch(
            ("192.168.1.2", 8002), b"test>127.0.0.2:9002", 2
        )
        r3 = await dispatcher.dispatch(
            ("192.168.1.3", 8003), b"test>127.0.0.3:9003", 3
        )

        # All should succeed independently
        assert r1.startswith(b"ack>")
        assert r2.startswith(b"ack>")
        assert r3.startswith(b"ack>")


class TestMessageDispatcherFailureModes:
    """Failure mode tests for MessageDispatcher."""

    @pytest.mark.asyncio
    async def test_handler_error_does_not_crash_dispatcher(
        self, mock_server: MockServerInterface
    ) -> None:
        """Handler error is caught, dispatcher continues working."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(FailingHandler(mock_server))
        dispatcher.register(MockHandler(mock_server))

        # This should fail
        r1 = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"fail>127.0.0.1:9000", 0
        )

        # But this should succeed
        r2 = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"test>127.0.0.1:9000", 0
        )

        assert b"nack" in r1
        assert r2.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_multiple_handler_errors(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple handler errors are all logged."""
        dispatcher = MessageDispatcher(mock_server)
        dispatcher.register(FailingHandler(mock_server))

        # Trigger multiple errors
        for _ in range(5):
            await dispatcher.dispatch(
                ("192.168.1.1", 8000), b"fail>127.0.0.1:9000", 0
            )

        assert len(mock_server._errors) == 5

    @pytest.mark.asyncio
    async def test_unregister_while_dispatching(
        self, mock_server: MockServerInterface
    ) -> None:
        """Unregistering during dispatch is safe."""
        dispatcher = MessageDispatcher(mock_server)
        handler = MockHandler(mock_server)
        dispatcher.register(handler)

        # Start a dispatch
        result = await dispatcher.dispatch(
            ("192.168.1.1", 8000), b"test>127.0.0.1:9000", 0
        )

        # Unregister after dispatch
        dispatcher.unregister(b"test")

        # Verify dispatch succeeded
        assert result.startswith(b"ack>")
        # And handler is now unregistered
        assert dispatcher.get_handler(b"test") is None
