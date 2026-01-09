"""
Tests for leadership handlers.

Handlers tested:
- LeaderClaimHandler
- LeaderVoteHandler
- LeaderElectedHandler
- LeaderHeartbeatHandler
- LeaderStepdownHandler
- PreVoteReqHandler
- PreVoteRespHandler

Covers:
- Happy path: normal leadership operations
- Negative path: unexpected messages, invalid states
- Edge cases: split-brain detection, self-targeted messages
- Concurrency: parallel handling
"""

import asyncio

import pytest

from hyperscale.distributed_rewrite.swim.message_handling.leadership import (
    LeaderClaimHandler,
    LeaderVoteHandler,
    LeaderElectedHandler,
    LeaderHeartbeatHandler,
    LeaderStepdownHandler,
    PreVoteReqHandler,
    PreVoteRespHandler,
)
from hyperscale.distributed_rewrite.swim.message_handling.models import MessageContext

from tests.integration.test_message_handling.mocks import MockServerInterface, MockLeaderState


class TestLeaderClaimHandlerHappyPath:
    """Happy path tests for LeaderClaimHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_claim(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader claim handler processes claim and returns vote."""
        handler = LeaderClaimHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leader-claim",
            message=b"leader-claim:5:100",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")
        # Vote should be scheduled via task runner
        assert len(mock_server.task_runner._tasks) >= 1

    @pytest.mark.asyncio
    async def test_handle_leader_claim_no_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader claim handler handles missing target gracefully."""
        handler = LeaderClaimHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"leader-claim",
            message=b"leader-claim:5:100",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """LeaderClaimHandler has correct message_types."""
        handler = LeaderClaimHandler(mock_server)

        assert handler.message_types == (b"leader-claim",)


class TestLeaderVoteHandlerHappyPath:
    """Happy path tests for LeaderVoteHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_vote_as_candidate(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader vote handler processes vote when candidate."""
        mock_server.set_as_candidate()
        handler = LeaderVoteHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"leader-vote",
            message=b"leader-vote:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestLeaderVoteHandlerNegativePath:
    """Negative path tests for LeaderVoteHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_vote_not_candidate(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader vote handler logs error if not candidate."""
        # Not a candidate by default
        handler = LeaderVoteHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"leader-vote",
            message=b"leader-vote:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Still returns ack but logs error
        assert result.response.startswith(b"ack>")
        assert len(mock_server._errors) >= 1

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """LeaderVoteHandler has correct message_types."""
        handler = LeaderVoteHandler(mock_server)

        assert handler.message_types == (b"leader-vote",)


class TestLeaderElectedHandlerHappyPath:
    """Happy path tests for LeaderElectedHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_elected(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader elected handler processes elected message."""
        handler = LeaderElectedHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leader-elected",
            message=b"leader-elected:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestLeaderElectedHandlerNegativePath:
    """Negative path tests for LeaderElectedHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_elected_self_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader elected handler logs error if target is self."""
        handler = LeaderElectedHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("127.0.0.1", 9000),  # Self
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"leader-elected",
            message=b"leader-elected:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Still returns ack but logs error
        assert result.response.startswith(b"ack>")
        assert len(mock_server._errors) >= 1

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """LeaderElectedHandler has correct message_types."""
        handler = LeaderElectedHandler(mock_server)

        assert handler.message_types == (b"leader-elected",)


class TestLeaderHeartbeatHandlerHappyPath:
    """Happy path tests for LeaderHeartbeatHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_heartbeat(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader heartbeat handler processes heartbeat."""
        handler = LeaderHeartbeatHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leader-heartbeat",
            message=b"leader-heartbeat:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")
        assert mock_server.metrics._counters.get("heartbeats_received", 0) >= 1


class TestLeaderHeartbeatHandlerNegativePath:
    """Negative path tests for LeaderHeartbeatHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_heartbeat_self_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader heartbeat handler logs error if target is self and source different."""
        handler = LeaderHeartbeatHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),  # Different from self
            source_addr_string="192.168.1.1:8000",
            target=("127.0.0.1", 9000),  # Self
            target_addr_bytes=b"127.0.0.1:9000",
            message_type=b"leader-heartbeat",
            message=b"leader-heartbeat:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Still returns ack but logs error
        assert result.response.startswith(b"ack>")
        assert len(mock_server._errors) >= 1


class TestLeaderHeartbeatHandlerEdgeCases:
    """Edge case tests for LeaderHeartbeatHandler."""

    @pytest.mark.asyncio
    async def test_handle_heartbeat_split_brain_detection(
        self, mock_server: MockServerInterface
    ) -> None:
        """Heartbeat handler detects split-brain scenario."""

        # Make this server think it's the leader
        class LeaderState(MockLeaderState):
            def is_leader(self) -> bool:
                return True

        mock_server._leader_election.state = LeaderState()
        mock_server._leader_election.state.current_leader = ("127.0.0.1", 9000)

        handler = LeaderHeartbeatHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),  # Different leader
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leader-heartbeat",
            message=b"leader-heartbeat:10",  # Higher term
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Should return ack
        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """LeaderHeartbeatHandler has correct message_types."""
        handler = LeaderHeartbeatHandler(mock_server)

        assert handler.message_types == (b"leader-heartbeat",)


class TestLeaderStepdownHandlerHappyPath:
    """Happy path tests for LeaderStepdownHandler."""

    @pytest.mark.asyncio
    async def test_handle_leader_stepdown(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader stepdown handler processes stepdown message."""
        handler = LeaderStepdownHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"leader-stepdown",
            message=b"leader-stepdown:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_handle_leader_stepdown_no_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Leader stepdown handler handles missing target gracefully."""
        handler = LeaderStepdownHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"leader-stepdown",
            message=b"leader-stepdown:5",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """LeaderStepdownHandler has correct message_types."""
        handler = LeaderStepdownHandler(mock_server)

        assert handler.message_types == (b"leader-stepdown",)


class TestPreVoteReqHandlerHappyPath:
    """Happy path tests for PreVoteReqHandler."""

    @pytest.mark.asyncio
    async def test_handle_pre_vote_req(
        self, mock_server: MockServerInterface
    ) -> None:
        """Pre-vote request handler processes request."""
        handler = PreVoteReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=("192.168.1.2", 9001),
            target_addr_bytes=b"192.168.1.2:9001",
            message_type=b"pre-vote-req",
            message=b"pre-vote-req:5:100",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")
        # Response should be scheduled
        assert len(mock_server.task_runner._tasks) >= 1

    @pytest.mark.asyncio
    async def test_handle_pre_vote_req_no_target(
        self, mock_server: MockServerInterface
    ) -> None:
        """Pre-vote request handler handles missing target gracefully."""
        handler = PreVoteReqHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"pre-vote-req",
            message=b"pre-vote-req:5:100",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """PreVoteReqHandler has correct message_types."""
        handler = PreVoteReqHandler(mock_server)

        assert handler.message_types == (b"pre-vote-req",)


class TestPreVoteRespHandlerHappyPath:
    """Happy path tests for PreVoteRespHandler."""

    @pytest.mark.asyncio
    async def test_handle_pre_vote_resp_during_pre_voting(
        self, mock_server: MockServerInterface
    ) -> None:
        """Pre-vote response handler processes response during pre-voting."""
        mock_server.set_pre_voting()
        handler = PreVoteRespHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"pre-vote-resp",
            message=b"pre-vote-resp:5:true",
            clock_time=12345,
        )

        result = await handler.handle(context)

        assert result.response.startswith(b"ack>")


class TestPreVoteRespHandlerNegativePath:
    """Negative path tests for PreVoteRespHandler."""

    @pytest.mark.asyncio
    async def test_handle_pre_vote_resp_not_pre_voting(
        self, mock_server: MockServerInterface
    ) -> None:
        """Pre-vote response handler logs error if not pre-voting."""
        # Not pre-voting by default
        handler = PreVoteRespHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"pre-vote-resp",
            message=b"pre-vote-resp:5:true",
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Still returns ack but logs error
        assert result.response.startswith(b"ack>")
        assert len(mock_server._errors) >= 1

    @pytest.mark.asyncio
    async def test_message_types_class_variable(
        self, mock_server: MockServerInterface
    ) -> None:
        """PreVoteRespHandler has correct message_types."""
        handler = PreVoteRespHandler(mock_server)

        assert handler.message_types == (b"pre-vote-resp",)


class TestLeadershipHandlersConcurrency:
    """Concurrency tests for leadership handlers."""

    @pytest.mark.asyncio
    async def test_concurrent_heartbeat_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple heartbeat handlers can run concurrently."""
        handler = LeaderHeartbeatHandler(mock_server)

        async def handle_heartbeat(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + index),
                source_addr_string=f"192.168.1.1:{8000 + index}",
                target=("192.168.1.2", 9001),
                target_addr_bytes=b"192.168.1.2:9001",
                message_type=b"leader-heartbeat",
                message=f"leader-heartbeat:{index}".encode(),
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_heartbeat(i) for i in range(30)]
        await asyncio.gather(*tasks)

        # All heartbeats should be counted
        assert mock_server.metrics._counters.get("heartbeats_received", 0) >= 30

    @pytest.mark.asyncio
    async def test_concurrent_claim_handling(
        self, mock_server: MockServerInterface
    ) -> None:
        """Multiple claim handlers can run concurrently."""
        handler = LeaderClaimHandler(mock_server)

        async def handle_claim(index: int) -> None:
            context = MessageContext(
                source_addr=("192.168.1.1", 8000),
                source_addr_string="192.168.1.1:8000",
                target=("192.168.1.2", 9001),
                target_addr_bytes=b"192.168.1.2:9001",
                message_type=b"leader-claim",
                message=f"leader-claim:{index}:100".encode(),
                clock_time=index,
            )
            await handler.handle(context)

        tasks = [handle_claim(i) for i in range(20)]
        await asyncio.gather(*tasks)

        # All claims should schedule votes
        assert len(mock_server.task_runner._tasks) >= 20


class TestLeadershipHandlersFailureModes:
    """Failure mode tests for leadership handlers."""

    @pytest.mark.asyncio
    async def test_heartbeat_continues_after_error(
        self, mock_server: MockServerInterface
    ) -> None:
        """Heartbeat handler continues after failed operations."""
        handler = LeaderHeartbeatHandler(mock_server)

        for i in range(5):
            context = MessageContext(
                source_addr=("192.168.1.1", 8000 + i),
                source_addr_string=f"192.168.1.1:{8000 + i}",
                target=("192.168.1.2", 9001),
                target_addr_bytes=b"192.168.1.2:9001",
                message_type=b"leader-heartbeat",
                message=b"leader-heartbeat:5",
                clock_time=i,
            )
            result = await handler.handle(context)
            assert result.response.startswith(b"ack>")

        assert mock_server.metrics._counters.get("heartbeats_received", 0) == 5

    @pytest.mark.asyncio
    async def test_vote_handler_handles_parse_failure(
        self, mock_server: MockServerInterface
    ) -> None:
        """Vote handler handles malformed term gracefully."""
        mock_server.set_as_candidate()
        handler = LeaderVoteHandler(mock_server)
        context = MessageContext(
            source_addr=("192.168.1.1", 8000),
            source_addr_string="192.168.1.1:8000",
            target=None,
            target_addr_bytes=None,
            message_type=b"leader-vote",
            message=b"leader-vote",  # No term
            clock_time=12345,
        )

        result = await handler.handle(context)

        # Should return ack without crashing
        assert result.response.startswith(b"ack>")
