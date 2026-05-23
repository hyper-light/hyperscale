"""
SWIM admission parsing helpers.

These helpers inspect raw SWIM payload bytes without applying membership,
health, worker-state, extension, or Vivaldi piggybacks. They are used before
handler dispatch so admission control can distinguish probe traffic from
lifecycle and gossip traffic without moving authorization out of handlers.
"""

from typing import Literal


SwimAdmissionClass = Literal[
    "probe_response",
    "probe_request",
    "lifecycle_direct",
    "membership_gossip",
    "leadership",
    "unknown",
]


_AUXILIARY_SEPARATORS: tuple[bytes, ...] = (
    b"#|s",
    b"#|m",
    b"#|h",
    b"#|w",
    b"#|x",
    b"#|o",
    b"#|v",
)

_PROBE_RESPONSE_TYPES: frozenset[bytes] = frozenset(
    {
        b"ack",
        b"nack",
        b"alive",
        b"ping-req-ack",
    }
)

_PROBE_REQUEST_TYPES: frozenset[bytes] = frozenset(
    {
        b"probe",
        b"ping-req",
    }
)

_MEMBERSHIP_GOSSIP_TYPES: frozenset[bytes] = frozenset(
    {
        b"suspect",
        b"dead",
        b"leave",
        b"join",
    }
)

_LEADERSHIP_TYPES: frozenset[bytes] = frozenset(
    {
        b"pre-vote",
        b"leader-claim",
        b"leader-elected",
        b"leader-heartbeat",
        b"leader-stepdown",
        b"vote-grant",
        b"vote-deny",
    }
)


def has_auxiliary_piggyback(payload: bytes) -> bool:
    """Return whether a SWIM payload carries auxiliary piggyback data."""
    return any(separator in payload for separator in _AUXILIARY_SEPARATORS)


def get_swim_message_type(payload: bytes) -> bytes:
    """Return the SWIM message type prefix from raw payload bytes."""
    message_prefix = payload.split(b">", maxsplit=1)[0]
    return message_prefix.split(b":", maxsplit=1)[0]


def parse_standard_target(payload: bytes) -> tuple[str, int] | None:
    """Parse ``message>host:port`` targets after stripping piggyback suffixes."""
    parsed = payload.split(b">", maxsplit=1)
    if len(parsed) != 2:
        return None

    target_bytes = strip_payload_extensions(parsed[1])
    try:
        host, port = target_bytes.decode().split(":", maxsplit=1)
        return (host, int(port))
    except (UnicodeDecodeError, ValueError):
        return None


def parse_join_target(payload: bytes) -> tuple[str, int] | None:
    """Parse current JOIN payload target from ``join>vX.Y|role|host:port|i:N``."""
    parsed = payload.split(b">", maxsplit=1)
    if len(parsed) != 2:
        return None

    target_bytes = strip_payload_extensions(parsed[1])
    if b"|" not in target_bytes:
        return parse_standard_target(payload)

    parts = target_bytes.split(b"|", maxsplit=3)
    if len(parts) >= 3:
        address_bytes = parts[2]
    elif len(parts) >= 2:
        address_bytes = parts[1]
    else:
        return None

    try:
        host, port = address_bytes.decode().split(":", maxsplit=1)
        return (host, int(port))
    except (UnicodeDecodeError, ValueError):
        return None


def parse_leave_node_id(payload: bytes) -> str | None:
    """Parse ``leave:{incarnation}:{node_id}`` metadata from raw payload."""
    message = payload.split(b">", maxsplit=1)[0]
    parts = message.split(b":", maxsplit=2)
    if len(parts) != 3 or parts[0] != b"leave":
        return None

    try:
        return parts[2].decode() or None
    except UnicodeDecodeError:
        return None


def strip_payload_extensions(payload: bytes) -> bytes:
    """Strip SWIM auxiliary suffixes from payload address bytes."""
    payload_end = len(payload)
    for separator in _AUXILIARY_SEPARATORS:
        separator_index = payload.find(separator)
        if separator_index >= 0:
            payload_end = min(payload_end, separator_index)
    return payload[:payload_end]


def classify_swim_payload(
    source_addr: tuple[str, int],
    payload: bytes,
    registered_node_id: str | None = None,
) -> SwimAdmissionClass:
    """Classify raw SWIM payload for receive-side admission control."""
    message_type = get_swim_message_type(payload)

    if message_type in _PROBE_RESPONSE_TYPES:
        return "probe_response"

    if message_type in _PROBE_REQUEST_TYPES:
        return "probe_request"

    if message_type in _LEADERSHIP_TYPES:
        return "leadership"

    if message_type == b"leave":
        target = parse_standard_target(payload)
        node_id = parse_leave_node_id(payload)
        if target == source_addr and node_id and node_id == registered_node_id:
            return "lifecycle_direct"
        return "membership_gossip"

    if message_type == b"join":
        target = parse_join_target(payload)
        if target == source_addr:
            return "lifecycle_direct"
        return "membership_gossip"

    if message_type in _MEMBERSHIP_GOSSIP_TYPES:
        return "membership_gossip"

    return "unknown"
