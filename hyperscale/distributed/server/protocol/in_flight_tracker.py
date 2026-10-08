"""
Priority-Aware In-Flight Task Tracker (AD-32, AD-37).

Provides bounded immediate execution with priority-based load shedding for
server-side incoming request handling. Ungrouped CRITICAL traffic keeps the
legacy unlimited behavior; grouped CRITICAL traffic, such as SWIM, gets a
dedicated reserve that is isolated from DATA/NORMAL overload but still bounded.

Key Design Points:
- All operations are sync-safe (GIL-protected integer operations)
- Called from sync protocol callbacks (datagram_received, etc.)
- Ungrouped CRITICAL priority ALWAYS succeeds; grouped CRITICAL traffic can
  have its own hard cap (for example SWIM's dedicated admission reserve)
- Lower priorities shed first under load (LOW → NORMAL → HIGH)

AD-37 Integration:
- MessagePriority corresponds one-to-one to AD-37 MessageClass:
- CONTROL (MessageClass) → CRITICAL (MessagePriority) - never shed unless
  the hook opts into a bounded admission group
- DISPATCH → HIGH - shed under overload
- DATA → NORMAL - explicit backpressure
- TELEMETRY → LOW - shed first

Usage:
    tracker = InFlightTracker(limits=PriorityLimits(...))

    # In protocol callback (sync context) - direct priority
    if tracker.try_acquire(MessagePriority.NORMAL):
        task = asyncio.ensure_future(handle_message(data))
        task.add_done_callback(lambda t: tracker.release(MessagePriority.NORMAL))
    else:
        # Message shed - log and drop
        pass

    # AD-37 compliant usage - handler name classification
    if tracker.try_acquire_for_handler("receive_workflow_progress"):
        task = asyncio.ensure_future(handle_message(data))
        task.add_done_callback(lambda t: tracker.release_for_handler("receive_workflow_progress"))

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from enum import IntEnum

from .message_priority import _CONTROL_HANDLERS
from .message_priority import _DISPATCH_HANDLERS
from .message_priority import _DATA_HANDLERS
from .message_priority import _TELEMETRY_HANDLERS
from .message_priority import _classify_handler_to_priority
from .message_priority import MessagePriority
from .priority_limits import PriorityLimits
from .protocol_in_flight_tracker import ProtocolInFlightTracker

_REHOMED = (
    MessagePriority,
    PriorityLimits,
    ProtocolInFlightTracker,
    _classify_handler_to_priority,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
