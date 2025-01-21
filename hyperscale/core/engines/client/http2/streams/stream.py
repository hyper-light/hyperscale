import struct
from typing import Optional

from hyperscale.core.engines.client.http2.frames import Frame, FrameBuffer
from hyperscale.core.engines.client.http2.windows import WindowManager
from hyperscale.core.engines.client.shared.protocols import (
    Reader,
    Writer,
)

READ_NUM_BYTES = 65536


class Stream:
    __slots__ = (
        "stream_id",
        "reader",
        "writer",
        "inbound",
        "outbound",
        "reset_connections",
        "max_inbound_frame_size",
        "max_outbound_frame_size",
        "current_outbound_window_size",
        "content_length",
        "expected_content_length",
        "_STRUCT_HBBBL",
        "_STRUCT_L",
        "frame_buffer",
        "window_frame",
    )

    def __init__(
        self,
        stream_id: int = 1,
        reset_connections: bool = False,
    ) -> None:
        self.stream_id = stream_id
        self.reader: Optional[Reader] = None
        self.writer: Optional[Writer] = None
        self.inbound: WindowManager = None
        self.outbound: WindowManager = None
        self.reset_connections = reset_connections

        self.max_inbound_frame_size = 0
        self.max_outbound_frame_size = 0
        self.current_outbound_window_size = 0
        self.content_length = 0
        self.expected_content_length = 0

        self._STRUCT_HBBBL = struct.Struct(">HBBBL")
        self._STRUCT_L = struct.Struct(">L")
        self.frame_buffer = FrameBuffer()

        self.window_frame = Frame(stream_id, 0x08, window_increment=65536)

    def update_stream_id(self):
        self.stream_id += 2  # self.concurrency
        if self.stream_id % 2 == 0:
            self.stream_id += 1

        self.window_frame.stream_id = self.stream_id
        self.frame_buffer = FrameBuffer()

        self.window_frame = Frame(self.stream_id, 0x08, window_increment=65536)

    def write(self, data: bytes):
        self.writer._transport.write(data)

    def read(self, msg_length: int = READ_NUM_BYTES):
        return self.reader.read(msg_length)

    def get_raw_buffer(self) -> bytearray:
        return self.reader._buffer

    def write_window_update_frame(
        self, stream_id: int = None, window_increment: int = None
    ):
        if stream_id is None:
            stream_id = self.stream_id

        body = self._STRUCT_L.pack(window_increment & 0x7FFFFFFF)
        body_len = len(body)

        type = 0x08

        # Build the common frame header.
        # First, get the flags.
        flags = 0

        header = self._STRUCT_HBBBL.pack(
            (body_len >> 8) & 0xFFFF,  # Length spread over top 24 bits
            body_len & 0xFF,
            type,
            flags,
            stream_id & 0x7FFFFFFF,  # Stream ID is 32 bits.
        )

        self.writer.write(header + body)
