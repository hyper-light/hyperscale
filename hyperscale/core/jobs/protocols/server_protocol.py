import asyncio
from typing import Callable, Tuple

from .message_limits import MAX_COMPRESSED_SIZE


class MercurySyncTCPServerProtocol(asyncio.Protocol):
    def __init__(self, callback: Callable[[bytes, Tuple[str, int]], bytes]):
        super().__init__()
        self.callback = callback
        self.transport: asyncio.Transport = None
        self.loop = asyncio.get_event_loop()
        self.on_con_lost = self.loop.create_future()
        # Bytes of a message still arriving.
        self._receive_buffer = bytearray()

    def connection_made(self, transport) -> str:
        self.transport = transport

    def data_received(self, data: bytes):
        # Messages arrive length-prefixed -- a 4-byte big-endian length, then
        # the message, as the distributed server frames them -- so each whole
        # message is delivered however TCP splits or joins the writes.
        buffer = self._receive_buffer
        buffer += data
        buffered = len(buffer)
        offset = 0

        while buffered - offset >= 4:
            message_length = int.from_bytes(buffer[offset : offset + 4], "big")
            if message_length > MAX_COMPRESSED_SIZE:
                # Longer than any message may be: not this protocol's stream.
                buffer.clear()
                self.transport.close()
                return

            message_end = offset + 4 + message_length
            if message_end > buffered:
                break

            self.callback(bytes(memoryview(buffer)[offset + 4 : message_end]), self.transport)
            offset = message_end

        if offset:
            del buffer[:offset]

    def connection_lost(self, exc: Exception | None) -> None:
        self._receive_buffer.clear()
        self.on_con_lost.set_result(True)
