"""
Message parser for SWIM protocol.

Extracts piggyback data, parses message format, and builds MessageContext.
"""

import base64
from dataclasses import dataclass

from .base import MessageContext


@dataclass(slots=True)
class ParseResult:
    """Result of parsing a raw UDP message."""
    context: MessageContext

    # Extracted piggyback data (to be processed separately)
    health_piggyback: bytes | None = None
    membership_piggyback: bytes | None = None


class MessageParser:
    """
    Parses raw UDP data into structured MessageContext.

    Handles:
    - Health gossip piggyback extraction (#|h...)
    - Membership piggyback extraction (#|m...)
    - Message type and target extraction
    - Embedded state extraction (Serf-style #|sbase64)
    - Cross-cluster message detection (xprobe/xack/xnack)

    All piggyback uses consistent #|x pattern for unambiguous parsing.
    """

    # Piggyback separators - all use consistent #|x pattern
    STATE_SEPARATOR = b'#|s'       # State piggyback
    MEMBERSHIP_SEPARATOR = b'#|m'  # Membership piggyback
    HEALTH_SEPARATOR = b'#|h'      # Health piggyback

    def __init__(
        self,
        process_embedded_state_callback,
    ) -> None:
        """
        Args:
            process_embedded_state_callback: Function to call when embedded
                state is extracted. Signature: (state_data: bytes, source: tuple) -> None
        """
        self._process_embedded_state = process_embedded_state_callback

    def parse(
        self,
        source_addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> ParseResult:
        """
        Parse raw UDP data into a MessageContext.

        Args:
            source_addr: The (host, port) of the sender.
            data: Raw UDP message bytes.
            clock_time: Clock time from UDP layer.

        Returns:
            ParseResult containing MessageContext and extracted piggyback data.
        """
        health_piggyback: bytes | None = None
        membership_piggyback: bytes | None = None

        # Extract health gossip piggyback first (format: #|hentry1;entry2;...)
        health_idx = data.find(self.HEALTH_SEPARATOR)
        if health_idx > 0:
            health_piggyback = data[health_idx:]
            data = data[:health_idx]

        # Extract membership piggyback (format: #|mtype:inc:host:port|...)
        piggyback_idx = data.find(self.MEMBERSHIP_SEPARATOR)
        if piggyback_idx > 0:
            membership_piggyback = data[piggyback_idx:]
            data = data[:piggyback_idx]

        # Parse message structure: msg_type>target_addr
        parsed = data.split(b'>', maxsplit=1)
        message = data
        target: tuple[str, int] | None = None
        target_addr_bytes: bytes | None = None

        if len(parsed) > 1:
            msg_prefix = parsed[0]

            # Handle cross-cluster messages specially
            # These have binary data after > that shouldn't be parsed as host:port
            if msg_prefix in (b'xprobe', b'xack', b'xnack'):
                message = msg_prefix
                target_addr_bytes = parsed[1]  # Keep as raw bytes
                target = source_addr  # Use source for response routing
            else:
                message = parsed[0]
                target_addr_bytes = parsed[1]

                # Extract embedded state from address portion (Serf-style)
                # Format: host:port#|sbase64_state
                if self.STATE_SEPARATOR in target_addr_bytes:
                    addr_part, state_part = target_addr_bytes.split(
                        self.STATE_SEPARATOR, 1
                    )
                    target_addr_bytes = addr_part

                    # Process embedded state
                    try:
                        state_data = base64.b64decode(state_part)
                        self._process_embedded_state(state_data, source_addr)
                    except Exception:
                        pass  # Invalid state, ignore

                # Parse target address
                try:
                    host, port = target_addr_bytes.decode().split(':', maxsplit=1)
                    target = (host, int(port))
                except (ValueError, UnicodeDecodeError):
                    target = None

        # Extract message type (before first colon)
        msg_type = message.split(b':', maxsplit=1)[0]

        context = MessageContext(
            source_addr=source_addr,
            target=target,
            target_addr_bytes=target_addr_bytes,
            message_type=msg_type,
            message=message,
            clock_time=clock_time,
        )

        return ParseResult(
            context=context,
            health_piggyback=health_piggyback,
            membership_piggyback=membership_piggyback,
        )
