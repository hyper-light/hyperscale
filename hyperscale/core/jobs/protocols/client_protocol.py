import asyncio
from typing import Any, Callable

from .message_limits import MAX_COMPRESSED_SIZE


class MercurySyncTCPClientProtocol(asyncio.Protocol):
    def __init__(self, callback: Callable[[Any], bytes]):
        super().__init__()
        self.transport: asyncio.Transport = None
        self.loop = asyncio.get_event_loop()
        self.callback = callback

        self.on_con_lost = self.loop.create_future()
        # Bytes of a message still arriving.
        self._receive_buffer = bytearray()

    def connection_made(self, transport: asyncio.Transport) -> str:
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

    def connection_lost(self, exc):
        self._receive_buffer.clear()
        self.on_con_lost.set_result(True)
