from ssl import SSLContext
from typing import Iterator, List, Optional, Sequence, Tuple

from hyperscale.core.engines.client.http.protocols import HTTPConnection
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import SocketConfig

from ..models.websocket.constants import (
    CLOSE_NORMAL,
    CLOSE_PROTOCOL_ERROR,
    CONTROL_FRAME_MAX_PAYLOAD,
    OPCODE_BINARY,
    OPCODE_CLOSE,
    OPCODE_CONTINUATION,
    OPCODE_PING,
    OPCODE_PONG,
    OPCODE_TEXT,
)
from ..models.websocket.utils import encode_frame, mask_payload


class WebsocketConnection(HTTPConnection):
    def __init__(self, reset_connections: bool = False) -> None:
        super().__init__(reset_connections=reset_connections)

    async def connect_to_any(
        self,
        target: Tuple[str, str, str],
        hostname: str,
        addresses: Sequence[Tuple[str, SocketConfig]],
        port: int,
        address_rotation: Iterator[int],
        ssl: Optional[SSLContext] = None,
    ) -> Tuple[Optional[str], Optional[SocketConfig], bool]:
        """
        Reuse the open WebSocket when it is ``target``'s (scheme, authority
        and resource). Otherwise one session per connection, as a WebSocket
        is a stateful session with one resource: the session open on another
        resource closes (with a close frame) before this one connects.
        """
        if (cached := self._reader_and_writer.get(target)) is not None:
            self.reader, self.writer = cached
            return None, None, False

        if self._reader_and_writer:
            self.close()

        return await super().connect_to_any(
            target,
            hostname,
            addresses,
            port,
            address_rotation,
            ssl=ssl,
        )

    def send_message(self, opcode: int, payload: bytes | List[bytes]) -> None:
        """
        One message: a single frame, or for a list of chunks one fragmented
        message (RFC 6455, 5.4) -- the first chunk carries the opcode, the
        rest are continuations, the last is final.
        """
        if isinstance(payload, list) and len(payload) > 1:
            last_index = len(payload) - 1
            self.writer.write(
                b"".join(
                    encode_frame(
                        opcode if index == 0 else OPCODE_CONTINUATION,
                        chunk,
                        final=index == last_index,
                    )
                    for index, chunk in enumerate(payload)
                )
            )

        elif isinstance(payload, list):
            self.writer.write(encode_frame(opcode, payload[0] if payload else b""))

        else:
            self.writer.write(encode_frame(opcode, payload))

    async def read_message(self) -> Tuple[int, bytes, Optional[int]]:
        """
        The next data message (RFC 6455, 5.4-5.6): its opcode and payload,
        continuation frames joined. Pings are answered and pongs skipped on
        the way. A close from the server is answered and returned as
        (OPCODE_CLOSE, reason, status code); so is a protocol violation,
        after this side closes with 1002 (7.1.7).
        """
        reader = self.reader
        fragments: List[bytes] = []
        message_opcode: Optional[int] = None

        while True:
            header = await reader.readexactly(2)
            final = header[0] & 0x80
            opcode = header[0] & 0x0F
            length = header[1] & 0x7F

            if length == 126:
                length = int.from_bytes(await reader.readexactly(2), "big")

            elif length == 127:
                length = int.from_bytes(await reader.readexactly(8), "big")

            if header[1] & 0x80:
                # Servers never mask (5.1); unmask rather than misread.
                mask = await reader.readexactly(4)
                payload = mask_payload(await reader.readexactly(length), mask)

            else:
                payload = await reader.readexactly(length) if length else b""

            if opcode >= OPCODE_CLOSE and (final == 0 or length > CONTROL_FRAME_MAX_PAYLOAD):
                return self._fail(f"fragmented or oversized control frame (opcode {opcode})")

            if opcode == OPCODE_PING:
                self.writer.write(encode_frame(OPCODE_PONG, payload))
                continue

            if opcode == OPCODE_PONG:
                continue

            if opcode == OPCODE_CLOSE:
                code = int.from_bytes(payload[:2], "big") if len(payload) >= 2 else None
                # Answer with the same status code (5.5.1).
                self.writer.write(encode_frame(OPCODE_CLOSE, payload[:2]))
                return OPCODE_CLOSE, payload[2:], code

            if opcode == OPCODE_CONTINUATION:
                if message_opcode is None:
                    return self._fail("continuation frame with no message to continue")

                fragments.append(payload)

            elif opcode in (OPCODE_TEXT, OPCODE_BINARY):
                if message_opcode is not None:
                    return self._fail("new message inside an unfinished fragmented message")

                message_opcode = opcode
                fragments.append(payload)

            else:
                return self._fail(f"reserved opcode {opcode}")

            if final:
                return message_opcode, fragments[0] if len(fragments) == 1 else b"".join(fragments), None

    def _fail(self, reason: str) -> Tuple[int, bytes, int]:
        """Fail the WebSocket connection (RFC 6455, 7.1.7): close with 1002, and report why."""
        self.writer.write(encode_frame(OPCODE_CLOSE, CLOSE_PROTOCOL_ERROR.to_bytes(2, "big")))
        return OPCODE_CLOSE, reason.encode(), CLOSE_PROTOCOL_ERROR

    def close(self):
        # Each open WebSocket closes with a close frame (RFC 6455, 5.5.1)
        # before its transport goes.
        close_frame = encode_frame(OPCODE_CLOSE, CLOSE_NORMAL.to_bytes(2, "big"))
        for _, writer in self._reader_and_writer.values():
            if writer.is_closing() is False:
                writer.write(close_frame)

        super().close()
