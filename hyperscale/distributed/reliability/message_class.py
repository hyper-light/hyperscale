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
    MessagePriority,
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


# Mapping from MessageClass to MessagePriority for InFlightTracker (AD-32)
MESSAGE_CLASS_TO_PRIORITY: dict[MessageClass, MessagePriority] = {
    MessageClass.CONTROL: MessagePriority.CRITICAL,
    MessageClass.DISPATCH: MessagePriority.HIGH,
    MessageClass.DATA: MessagePriority.NORMAL,
    MessageClass.TELEMETRY: MessagePriority.LOW,
}


# Handler names that belong to each message class
# Used for automatic classification of incoming requests
CONTROL_HANDLERS: frozenset[str] = frozenset(
    {
        # SWIM protocol
        "ping",
        "ping_req",
        "ack",
        "nack",
        "indirect_ping",
        "indirect_ack",
        # Cancellation (AD-20)
        "cancel_workflow",
        "cancel_job",
        "workflow_cancelled",
        "job_cancellation_complete",
        # Leadership transfer
        "leadership_transfer",
        "job_leader_transfer",
        "receive_job_leader_transfer",
        "job_leader_worker_transfer",
        # Failure detection
        "suspect",
        "alive",
        "dead",
        "leave",
    }
)

DISPATCH_HANDLERS: frozenset[str] = frozenset(
    {
        # Job dispatch
        "submit_job",
        "receive_submit_job",
        "dispatch_workflow",
        "receive_workflow_dispatch",
        # State sync
        "state_sync_request",
        "state_sync_response",
        "request_state_sync",
        # Registration
        "worker_register",
        "receive_worker_register",
        "manager_register",
        "receive_manager_register",
        # Workflow commands
        "workflow_dispatch_ack",
        "workflow_final_result",
    }
)

DATA_HANDLERS: frozenset[str] = frozenset(
    {
        # Progress updates
        "workflow_progress",
        "receive_workflow_progress",
        "workflow_progress_ack",
        # Stats updates
        "receive_stats_update",
        "send_stats_update",
        # AD-34 timeout coordination
        "receive_job_progress_report",
        "receive_job_timeout_report",
        "receive_job_global_timeout",
        "receive_job_final_status",
        # Heartbeats (non-SWIM)
        "heartbeat",
        "manager_heartbeat",
        "worker_heartbeat",
        # Job progress (gate handlers)
        "receive_job_progress",
    }
)

TELEMETRY_HANDLERS: frozenset[str] = frozenset(
    {
        # Metrics
        "metrics_report",
        "debug_stats",
        "trace_event",
        # Health probes (non-critical)
        "health_check",
        "readiness_check",
        "liveness_check",
        # Federated health (best-effort)
        "xprobe",
        "xack",
    }
)


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


def get_priority_for_handler(handler_name: str) -> MessagePriority:
    """
    Get the MessagePriority for a handler name.

    Convenience function that classifies and maps to priority in one call.

    Args:
        handler_name: Name of the handler being invoked.

    Returns:
        MessagePriority for the InFlightTracker.
    """
    message_class = classify_handler(handler_name)
    return MESSAGE_CLASS_TO_PRIORITY[message_class]


def is_control_message(handler_name: str) -> bool:
    """Check if a handler is a control message (never backpressured)."""
    return handler_name in CONTROL_HANDLERS


def is_data_message(handler_name: str) -> bool:
    """Check if a handler is a data message (explicit backpressure)."""
    return handler_name in DATA_HANDLERS


def is_shedable(handler_name: str) -> bool:
    """Check if a handler can be shed under load (non-CONTROL)."""
    return handler_name not in CONTROL_HANDLERS
