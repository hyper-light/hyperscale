"""
Message parser for SWIM protocol.

Extracts piggyback data, parses message format, and builds MessageContext.
"""

from base64 import b64decode
from typing import Callable

from hyperscale.distributed_rewrite.swim.message_handling.models import (
    MessageContext,
    ParseResult,
    ServerInterface,
)


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

    STATE_SEPARATOR = b"#|s"
    MEMBERSHIP_SEPARATOR = b"#|m"
    HEALTH_SEPARATOR = b"#|h"

    CROSS_CLUSTER_PREFIXES = (b"xprobe", b"xack", b"xnack")

    def __init__(
        self,
        server: ServerInterface,
        process_embedded_state: Callable[[bytes, tuple[str, int]], None] | None = None,
    ) -> None:
        """
        Initialize parser.

        Args:
            server: Server interface for state processing.
            process_embedded_state: Callback for embedded state.
                If None, uses server's default processing.
        """
        self._server = server
        self._process_embedded_state = process_embedded_state

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
        parsed = data.split(b">", maxsplit=1)
        message = data
        target: tuple[str, int] | None = None
        target_addr_bytes: bytes | None = None

        if len(parsed) > 1:
            msg_prefix = parsed[0]

            # Handle cross-cluster messages specially
            # These have binary data after > that shouldn't be parsed as host:port
            if msg_prefix in self.CROSS_CLUSTER_PREFIXES:
                message = msg_prefix
                target_addr_bytes = parsed[1]
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

                    # Process embedded state from sender
                    self._decode_and_process_state(state_part, source_addr)

                # Parse target address
                target = self._parse_target_address(target_addr_bytes)

        # Extract message type (before first colon)
        msg_type = message.split(b":", maxsplit=1)[0]

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

    def _decode_and_process_state(
        self, state_part: bytes, source_addr: tuple[str, int]
    ) -> None:
        """
        Decode and process embedded state.

        Args:
            state_part: Base64-encoded state data.
            source_addr: Source address for context.
        """
        if self._process_embedded_state is None:
            return

        try:
            state_data = b64decode(state_part)
            self._process_embedded_state(state_data, source_addr)
        except Exception:
            pass  # Invalid state, ignore

    def _parse_target_address(
        self, target_addr_bytes: bytes
    ) -> tuple[str, int] | None:
        """
        Parse target address from bytes.

        Args:
            target_addr_bytes: Address bytes (e.g., b'127.0.0.1:9000').

        Returns:
            Parsed address tuple or None if invalid.
        """
        try:
            addr_str = target_addr_bytes.decode()
            host, port_str = addr_str.split(":", maxsplit=1)
            return (host, int(port_str))
        except (ValueError, UnicodeDecodeError):
            return None
