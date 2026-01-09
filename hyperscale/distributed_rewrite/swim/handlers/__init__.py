"""
SWIM Protocol Message Handlers.

This module provides a compositional approach to handling SWIM protocol
messages. Instead of a monolithic receive() function with 600+ lines,
messages are routed to specialized handlers.

Architecture:
- MessageContext: Immutable context for each message (addr, target, data, etc.)
- MessageHandler: Protocol for individual message type handlers
- MessageDispatcher: Routes messages to appropriate handlers
- MessageParser: Parses raw UDP data into MessageContext

Handler Categories:
- Membership: ack, nack, join, leave
- Probing: probe, ping-req, ping-req-ack, alive, suspect
- Leadership: leader-claim, leader-vote, leader-elected, leader-heartbeat, etc.
- CrossCluster: xprobe, xack, xnack
"""

from .base import MessageContext, MessageHandler, HandlerResult
from .message_parser import MessageParser
from .message_dispatcher import MessageDispatcher

__all__ = [
    'MessageContext',
    'MessageHandler',
    'HandlerResult',
    'MessageParser',
    'MessageDispatcher',
]
