"""
SWIM Protocol Message Handling.

This module provides a compositional approach to handling SWIM protocol
messages. Instead of a monolithic receive() function with 700+ lines,
messages are routed to specialized handlers.

Architecture:
- MessageContext: Immutable context for each message (addr, target, data, etc.)
- HandlerResult: Result from handler (response + metadata)
- BaseHandler: Abstract base class for handlers
- MessageDispatcher: Routes messages to appropriate handlers
- MessageParser: Parses raw UDP data into MessageContext

Handler Categories:
- Membership: ack, nack, join, leave
- Probing: probe, ping-req, ping-req-ack
- Suspicion: alive, suspect
- Leadership: leader-claim, leader-vote, leader-elected, leader-heartbeat,
              leader-stepdown, pre-vote-req, pre-vote-resp
- CrossCluster: xprobe, xack, xnack

Usage:
    from hyperscale.distributed_rewrite.swim.message_handling import (
        MessageDispatcher,
        register_default_handlers,
    )

    dispatcher = MessageDispatcher(server)
    register_default_handlers(dispatcher, server)

    # In receive():
    response = await dispatcher.dispatch(addr, data, clock_time)
"""

from .models import (
    MessageContext,
    HandlerResult,
    ParseResult,
    ServerInterface,
)
from .core import (
    BaseHandler,
    MessageParser,
    MessageDispatcher,
    ResponseBuilder,
)
from .membership import (
    AckHandler,
    NackHandler,
    JoinHandler,
    LeaveHandler,
)
from .probing import (
    ProbeHandler,
    PingReqHandler,
    PingReqAckHandler,
)
from .suspicion import (
    AliveHandler,
    SuspectHandler,
)
from .leadership import (
    LeaderClaimHandler,
    LeaderVoteHandler,
    LeaderElectedHandler,
    LeaderHeartbeatHandler,
    LeaderStepdownHandler,
    PreVoteReqHandler,
    PreVoteRespHandler,
)
from .cross_cluster import (
    XProbeHandler,
    XAckHandler,
    XNackHandler,
)
from .server_adapter import ServerAdapter


def register_default_handlers(
    dispatcher: MessageDispatcher, server: ServerInterface
) -> None:
    """
    Register all default SWIM message handlers.

    Args:
        dispatcher: Dispatcher to register handlers with.
        server: Server interface for handler initialization.
    """
    # Membership handlers
    dispatcher.register(AckHandler(server))
    dispatcher.register(NackHandler(server))
    dispatcher.register(JoinHandler(server))
    dispatcher.register(LeaveHandler(server))

    # Probing handlers
    dispatcher.register(ProbeHandler(server))
    dispatcher.register(PingReqHandler(server))
    dispatcher.register(PingReqAckHandler(server))

    # Suspicion handlers
    dispatcher.register(AliveHandler(server))
    dispatcher.register(SuspectHandler(server))

    # Leadership handlers
    dispatcher.register(LeaderClaimHandler(server))
    dispatcher.register(LeaderVoteHandler(server))
    dispatcher.register(LeaderElectedHandler(server))
    dispatcher.register(LeaderHeartbeatHandler(server))
    dispatcher.register(LeaderStepdownHandler(server))
    dispatcher.register(PreVoteReqHandler(server))
    dispatcher.register(PreVoteRespHandler(server))

    # Cross-cluster handlers
    dispatcher.register(XProbeHandler(server))
    dispatcher.register(XAckHandler(server))
    dispatcher.register(XNackHandler(server))


__all__ = [
    # Models
    "MessageContext",
    "HandlerResult",
    "ParseResult",
    "ServerInterface",
    # Core
    "BaseHandler",
    "MessageParser",
    "MessageDispatcher",
    "ResponseBuilder",
    # Membership
    "AckHandler",
    "NackHandler",
    "JoinHandler",
    "LeaveHandler",
    # Probing
    "ProbeHandler",
    "PingReqHandler",
    "PingReqAckHandler",
    # Suspicion
    "AliveHandler",
    "SuspectHandler",
    # Leadership
    "LeaderClaimHandler",
    "LeaderVoteHandler",
    "LeaderElectedHandler",
    "LeaderHeartbeatHandler",
    "LeaderStepdownHandler",
    "PreVoteReqHandler",
    "PreVoteRespHandler",
    # Cross-cluster
    "XProbeHandler",
    "XAckHandler",
    "XNackHandler",
    # Adapter
    "ServerAdapter",
    # Registration
    "register_default_handlers",
]
