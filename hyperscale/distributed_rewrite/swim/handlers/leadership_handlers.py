"""
SWIM Leadership Message Handlers.

Handles: leader-claim, leader-vote, leader-elected, leader-heartbeat,
         leader-stepdown, pre-vote-req, pre-vote-resp
"""

from typing import TYPE_CHECKING

from .base import BaseHandler, MessageContext, HandlerResult

from ..core.errors import UnexpectedMessageError, SplitBrainError
from ..core.audit import AuditEventType
from hyperscale.logging.hyperscale_logging_models import ServerInfo

if TYPE_CHECKING:
    from ..health_aware_server import HealthAwareServer


class LeaderClaimHandler(BaseHandler):
    """Handles leader-claim messages (election start)."""

    def __init__(self) -> None:
        super().__init__((b'leader-claim',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        term, candidate_lhm = await server._parse_leadership_claim(message, addr)

        if target:
            vote_msg = server._leader_election.handle_claim(target, term, candidate_lhm)
            if vote_msg:
                server._task_runner.run(
                    server.send,
                    target,
                    vote_msg,
                    timeout=server.get_lhm_adjusted_timeout(
                        server._context.read('current_timeout')
                    ),
                )

        return self.build_ack(server)


class LeaderVoteHandler(BaseHandler):
    """Handles leader-vote messages."""

    def __init__(self) -> None:
        super().__init__((b'leader-vote',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        message = ctx.message

        # Verify we're actually expecting votes
        if not server._leader_election.state.is_candidate():
            await server.handle_error(
                UnexpectedMessageError(
                    msg_type=b'leader-vote',
                    expected=[b'probe', b'ack', b'leader-heartbeat'],
                    source=addr,
                )
            )
            return self.build_ack(server)

        term = await server._parse_term_safe(message, addr)

        if server._leader_election.handle_vote(addr, term):
            server._leader_election.state.become_leader(term)
            server._leader_election.state.current_leader = server._get_self_udp_addr()

            self_addr = server._get_self_udp_addr()
            elected_msg = (
                b'leader-elected:' +
                str(term).encode() + b'>' +
                f'{self_addr[0]}:{self_addr[1]}'.encode()
            )
            server._broadcast_leadership_message(elected_msg)

        return self.build_ack(server)


class LeaderElectedHandler(BaseHandler):
    """Handles leader-elected messages."""

    def __init__(self) -> None:
        super().__init__((b'leader-elected',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        term = await server._parse_term_safe(message, addr)

        if target:
            # Check if we received our own election announcement
            self_addr = server._get_self_udp_addr()
            if target == self_addr:
                await server.handle_error(
                    UnexpectedMessageError(
                        msg_type=b'leader-elected',
                        expected=None,
                        source=addr,
                    )
                )
                return self.build_ack(server)

            await server._leader_election.handle_elected(target, term)

        return self.build_ack(server)


class LeaderHeartbeatHandler(BaseHandler):
    """Handles leader-heartbeat messages."""

    def __init__(self) -> None:
        super().__init__((b'leader-heartbeat',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        server._metrics.increment('heartbeats_received')
        term = await server._parse_term_safe(message, addr)

        # Check if we received our own heartbeat
        if target:
            self_addr = server._get_self_udp_addr()
            if target == self_addr and addr != self_addr:
                await server.handle_error(
                    UnexpectedMessageError(
                        msg_type=b'leader-heartbeat',
                        expected=None,
                        source=addr,
                    )
                )
                return self.build_ack(server)

        if target:
            self_addr = server._get_self_udp_addr()
            if server._leader_election.state.is_leader() and target != self_addr:
                should_yield = server._leader_election.handle_discovered_leader(
                    target, term
                )

                server._udp_logger.log(
                    ServerInfo(
                        message=f"[{server._node_id.short}] Received heartbeat from "
                                f"leader {target} term={term}, yield={should_yield}",
                        node_host=server._host,
                        node_port=server._udp_port,
                        node_id=server._node_id.short,
                    )
                )

                if should_yield:
                    server._udp_logger.log(
                        ServerInfo(
                            message=f"[SPLIT-BRAIN] Detected other leader {target} "
                                    f"with term {term}, stepping down",
                            node_host=server._host,
                            node_port=server._udp_port,
                            node_id=server._node_id.short,
                        )
                    )

                    # Record split brain in audit log
                    server._audit_log.record(
                        AuditEventType.SPLIT_BRAIN_DETECTED,
                        node=self_addr,
                        other_leader=target,
                        self_term=server._leader_election.state.current_term,
                        other_term=term,
                    )
                    server._metrics.increment('split_brain_events')

                    await server.handle_error(
                        SplitBrainError(
                            self_addr,
                            target,
                            server._leader_election.state.current_term,
                            term,
                        )
                    )
                    server._task_runner.run(server._leader_election._step_down)

            await server._leader_election.handle_heartbeat(target, term)

        return self.build_ack(server)


class LeaderStepdownHandler(BaseHandler):
    """Handles leader-stepdown messages."""

    def __init__(self) -> None:
        super().__init__((b'leader-stepdown',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        term = await server._parse_term_safe(message, addr)

        if target:
            await server._leader_election.handle_stepdown(target, term)

        return self.build_ack(server)


class PreVoteReqHandler(BaseHandler):
    """Handles pre-vote-req messages (Raft pre-voting)."""

    def __init__(self) -> None:
        super().__init__((b'pre-vote-req',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        target = ctx.target
        message = ctx.message

        term, candidate_lhm = await server._parse_leadership_claim(message, addr)

        if target:
            resp = server._leader_election.handle_pre_vote_request(
                candidate=target,
                term=term,
                candidate_lhm=candidate_lhm,
            )
            if resp:
                server._task_runner.run(
                    server._send_to_addr,
                    target,
                    resp,
                )

        return self.build_ack(server)


class PreVoteRespHandler(BaseHandler):
    """Handles pre-vote-resp messages."""

    def __init__(self) -> None:
        super().__init__((b'pre-vote-resp',))

    async def handle(
        self,
        ctx: MessageContext,
        server: 'HealthAwareServer',
    ) -> HandlerResult:
        addr = ctx.source_addr
        message = ctx.message

        # Verify we're in a pre-voting phase
        if not server._leader_election.state.pre_voting_in_progress:
            await server.handle_error(
                UnexpectedMessageError(
                    msg_type=b'pre-vote-resp',
                    expected=None,
                    source=addr,
                )
            )
            return self.build_ack(server)

        term, granted = await server._parse_pre_vote_response(message, addr)

        server._leader_election.handle_pre_vote_response(
            voter=addr,
            term=term,
            granted=granted,
        )

        return self.build_ack(server)
