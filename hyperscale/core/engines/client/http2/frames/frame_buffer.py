# -*- coding: utf-8 -*-
"""
h2/frame_buffer
~~~~~~~~~~~~~~~

A data structure that provides a way to iterate over a byte buffer in terms of
frames.
"""

import struct

from .exceptions import InvalidDataError, InvalidFrameError
from .types import *
from .types.attributes import (
    _STRUCT_H,
    _STRUCT_HBBBL,
    _STRUCT_HL,
    _STRUCT_L,
    _STRUCT_LB,
    _STRUCT_LL,
)
from .types.base_frame import Frame


class FrameBuffer:
    __slots__ = ("data", "max_frame_size", "_headers_buffer", "frames_map")

    """
    This is a data structure that expects to act as a buffer for HTTP/2 data
    that allows iteraton in terms of H2 frames.
    """

    def __init__(self):
        self.data = bytearray()
        self.max_frame_size = 0
        self._headers_buffer = []

    # The methods below support the iterator protocol.
    def __iter__(self):
        while len(self.data) >= 9:
            try:
                length_high, length_low, frame_type, flags, stream_id = (
                    _STRUCT_HBBBL.unpack_from(self.data)
                )

            except (InvalidDataError, InvalidFrameError) as e:  # pragma: no cover
                raise Exception("Received frame with invalid header: %s" % str(e))

            # First 24 bits are frame length.
            length = (length_high << 8) + length_low

            # Next, check that we have enough length to parse the frame body. If
            # not, bail, leaving the frame header data in the buffer for next time.
            # The frame is only built once its whole body is here.
            if len(self.data) < length + 9:
                break

            frame = self.parse_frame(frame_type, flags, stream_id, self.data[9 : 9 + length])

            # At this point, as we know we'll use or discard the entire frame, we
            # can update the data.
            del self.data[: 9 + length]

            frame = self.through_header_block(frame, frame_type)

            # If we got a frame we didn't understand or shouldn't yield, rather
            # than return None it'd be better if we just tried to get the next
            # frame in the sequence instead. Recurse back into ourselves to do
            # that. This is safe because the amount of work we have to do here is
            # strictly bounded by the length of the buffer.
            if frame:
                yield frame

    def parse_frame(
        self,
        frame_type: int,
        flags: int,
        stream_id: int,
        body_data: bytearray,
    ) -> Frame:
        """
        The frame of ``frame_type`` with ``flags`` on ``stream_id`` whose
        body is ``body_data``.
        """
        frame = Frame(stream_id & 0x7FFFFFFF, frame_type, parsed_flag_byte=flags)

        # The most frequent frame types first.
        if frame_type == 0x0:
            # DATA
            padding_data_offset = 0
            frame.pad_length = 0

            if "PADDED" in frame.flags:  # type: ignore
                frame.pad_length = struct.unpack("!B", body_data[:1])[0]
                padding_data_offset = 1

            data_length = len(body_data)
            # Unpadded, the frame body is the data: no second copy.
            frame.data = (
                body_data[padding_data_offset : data_length - frame.pad_length]
                if padding_data_offset
                else body_data
            )
            frame.body_len = data_length

        elif frame_type == 0x01:
            # HEADERS
            padding_data_offset = 0

            if "PADDED" in frame.flags:  # type: ignore
                frame.pad_length = struct.unpack("!B", body_data[:1])[0]
                padding_data_offset = 1

            if padding_data_offset:
                body_data = body_data[padding_data_offset:]

            if "PRIORITY" in frame.flags:
                frame.depends_on, frame.stream_weight = _STRUCT_LB.unpack(
                    body_data[:5]
                )
                frame.exclusive = True if frame.depends_on >> 31 else False
                frame.depends_on &= 0x7FFFFFFF
                padding_data_offset = 5

            else:
                padding_data_offset = 0

            data_length = len(body_data)
            frame.body_len = data_length
            # Without padding or priority fields, the body is the header
            # block: no second copy.
            frame.data = (
                body_data[padding_data_offset : data_length - frame.pad_length]
                if padding_data_offset or frame.pad_length
                else body_data
            )

        elif frame_type == 0x06:
            # PING
            frame.opaque_data = body_data
            frame.body_len = 8

        elif frame_type == 0x02:
            # PRIORITY

            try:
                frame.depends_on, frame.stream_weight = _STRUCT_LB.unpack(
                    body_data[:5]
                )
            except struct.error:
                raise Exception("Invalid Priority data")

            frame.exclusive = True if frame.depends_on >> 31 else False
            frame.depends_on &= 0x7FFFFFFF

            frame.body_len = 5

        elif frame_type == 0x05:
            # PUSH PROMISE
            padding_data_offset = 0
            frame.pad_length = 0
            if "PADDED" in frame.flags:  # type: ignore
                try:
                    frame.pad_length = struct.unpack("!B", body_data[:1])[0]
                except struct.error:
                    raise Exception("Invalid Padding data")
                padding_data_offset = 1

            frame.promised_stream_id = _STRUCT_L.unpack(
                body_data[padding_data_offset : padding_data_offset + 4]
            )[0]

            data_len = len(body_data)
            frame.data = body_data[
                padding_data_offset + 4 : data_len - frame.pad_length
            ]
            frame.body_len = data_len

        elif frame_type == 0x03:
            # RESET
            frame.error_code = _STRUCT_L.unpack(body_data)[0]
            frame.body_len = 4

        elif frame_type == 0x04:
            # SETTINGS
            body_len = 0
            for i in range(0, len(body_data), 6):
                name, value = _STRUCT_HL.unpack(body_data[i : i + 6])

                frame.settings[name] = value
                body_len += 6

            frame.body_len = body_len

        elif frame_type == 0x08:
            # WINDOW UPDATE
            frame.window_increment = _STRUCT_L.unpack(body_data)[0]
            frame.body_len = 4

        elif frame_type == 0x07:
            # GOAWAY

            frame.last_stream_id, frame.error_code = _STRUCT_LL.unpack(
                body_data[:8]
            )
            frame.body_len = len(body_data)

            if len(body_data) > 8:
                frame.additional_data = body_data[8:]

        elif frame_type == 0x09:
            # CONTINUATION
            frame.data = body_data
            frame.body_len = len(body_data)

        elif frame_type == 0xA:
            # ALTSVC

            origin_len = _STRUCT_H.unpack(body_data[0:2])[0]
            frame.origin = body_data[2 : 2 + origin_len]

            if len(frame.origin) != origin_len:
                raise Exception("Invalid ALTSVC frame body.")

            frame.field = body_data[2 + origin_len :]
            frame.body_len = len(body_data)


        return frame

    def through_header_block(self, frame: Frame, frame_type: int) -> Frame | None:
        """
        Passes ``frame`` through the header block in progress: the frame to
        hand on -- itself, or the HEADERS or PUSH_PROMISE frame a
        CONTINUATION completes -- or None while a header block is still
        being collected.
        """
        # Pass the frame through the heaer buffer.
        # f = self._update_header_buffer(f)
        is_headers_or_push_promise = frame_type == 0x01 or frame_type == 0x05

        if self._headers_buffer:
            stream_id = self._headers_buffer[0].stream_id
            valid_frame = (
                frame is not None
                and frame_type == 0x09
                and frame.stream_id == stream_id
            )

            if valid_frame == False:
                raise Exception("Invalid frame during header block.")

            # Append the frame to the buffer.
            self._headers_buffer.append(frame)

            # If this is the end of the header block, then we want to build a
            # mutant HEADERS frame that's massive. Use the original one we got,
            # then set END_HEADERS and set its data appopriately. If it's not
            # the end of the block, lose the current frame: we can't yield it.
            if "END_HEADERS" in frame.flags:
                frame = self._headers_buffer[0]
                frame.flags.add("END_HEADERS")

                frame_data = bytearray()
                for header_frame in self._headers_buffer:
                    frame_data.extend(header_frame.data)

                frame.data = frame_data
                self._headers_buffer = []

            else:
                frame = None
        elif is_headers_or_push_promise and "END_HEADERS" not in frame.flags:
            # This is the start of a headers block! Save the frame off and then
            # act like we didn't receive one.
            self._headers_buffer.append(frame)
            frame = None

        return frame
