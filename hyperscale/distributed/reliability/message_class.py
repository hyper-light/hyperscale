"""
Message Classification for Explicit Backpressure Policy (AD-37).

Defines message classes that determine backpressure and load shedding behavior.
Each class maps to a priority level for the InFlightTracker (AD-32).

Message Classes:
- CONTROL: Never backpressured (SWIM probes/acks, cancellation, leadership transfer)
- DISPATCH: Shed under overload, bounded by priority (job submission, workflow dispatch)
- DATA: Explicit backpressure + batching (workflow progress, stats updates)
- TELEMETRY: Shed first under overload (debug stats, detailed metrics)

See AD-37 in docs/architecture.md for full specification.
"""

from enum import Enum, auto

from hyperscale.distributed.server.protocol.in_flight_tracker import (
    _CONTROL_HANDLERS,
    _DATA_HANDLERS,
    _DISPATCH_HANDLERS,
    _TELEMETRY_HANDLERS,
)


class MessageClass(Enum):
    """
    Message classification for backpressure policy (AD-37).

    Determines how messages are handled under load:
    - CONTROL: Critical control plane - never backpressured or shed
    - DISPATCH: Work dispatch - bounded by AD-32, shed under extreme load
    - DATA: Data plane updates - explicit backpressure, batching under load
    - TELEMETRY: Observability - shed first, lowest priority
    """

    CONTROL = auto()  # SWIM probes/acks, cancellation, leadership transfer
    DISPATCH = auto()  # Job submission, workflow dispatch, state sync
    DATA = auto()  # Workflow progress, stats updates
    TELEMETRY = auto()  # Debug stats, detailed metrics



# Handler names that belong to each message class -- defined once, beside
# the protocol-layer admission that classifies every request by them.
CONTROL_HANDLERS: frozenset[str] = _CONTROL_HANDLERS
DISPATCH_HANDLERS: frozenset[str] = _DISPATCH_HANDLERS
DATA_HANDLERS: frozenset[str] = _DATA_HANDLERS
TELEMETRY_HANDLERS: frozenset[str] = _TELEMETRY_HANDLERS


def classify_handler(handler_name: str) -> MessageClass:
    """
    Classify a handler by its AD-37 message class.

    Uses explicit handler name matching for known handlers,
    defaults to DATA for unknown handlers (conservative approach).

    Args:
        handler_name: Name of the handler being invoked.

    Returns:
        MessageClass for the handler.
    """
    if handler_name in CONTROL_HANDLERS:
        return MessageClass.CONTROL
    if handler_name in DISPATCH_HANDLERS:
        return MessageClass.DISPATCH
    if handler_name in DATA_HANDLERS:
        return MessageClass.DATA
    if handler_name in TELEMETRY_HANDLERS:
        return MessageClass.TELEMETRY

    # Default to DATA for unknown handlers (moderate priority)
    return MessageClass.DATA

