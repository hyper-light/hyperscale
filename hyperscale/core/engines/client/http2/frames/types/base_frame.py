# -*- coding: utf-8 -*-
"""
hyperframe/frame
~~~~~~~~~~~~~~~~

Defines framing logic for HTTP/2. Provides both classes to represent framed
data and logic for aiding the connection when it comes to reading from the
socket.
"""

import sys
from typing import Any, Iterable, Optional, Tuple

from .attributes import (
    _STRUCT_B,
    _STRUCT_H,
    _STRUCT_HBBBL,
    _STRUCT_HL,
    _STRUCT_L,
    _STRUCT_LB,
    _STRUCT_LL,
    Flag,
    Flags,
)
from .utils import raw_data_repr

# The flags each frame type defines (RFC 9113 6), shared by every frame of
# that type.
NO_FLAGS: Tuple[Flag, ...] = ()
CONTINUATION_FLAGS = (Flag("END_HEADERS", 0x04),)
DATA_FLAGS = (Flag("END_STREAM", 0x01), Flag("PADDED", 0x08))
HEADERS_FLAGS = (
    Flag("END_STREAM", 0x01),
    Flag("END_HEADERS", 0x04),
    Flag("PADDED", 0x08),
    Flag("PRIORITY", 0x20),
)
ACK_FLAGS = (Flag("ACK", 0x01),)
PUSH_PROMISE_FLAGS = (Flag("END_HEADERS", 0x04), Flag("PADDED", 0x08))


def flag_names_by_byte(defined_flags: Tuple[Flag, ...]) -> Tuple[Tuple[str, ...], ...]:
    """For each value of a frame's flag byte, the names of the defined flags it sets."""
    return tuple(
        tuple(name for name, bit in defined_flags if flag_byte & bit)
        for flag_byte in range(256)
    )


NO_FLAG_NAMES = flag_names_by_byte(NO_FLAGS)
CONTINUATION_FLAG_NAMES = flag_names_by_byte(CONTINUATION_FLAGS)
DATA_FLAG_NAMES = flag_names_by_byte(DATA_FLAGS)
HEADERS_FLAG_NAMES = flag_names_by_byte(HEADERS_FLAGS)
ACK_FLAG_NAMES = flag_names_by_byte(ACK_FLAGS)
PUSH_PROMISE_FLAG_NAMES = flag_names_by_byte(PUSH_PROMISE_FLAGS)

# What an attribute a frame's type never sets reads as -- the value every
# frame used to be given. Frames are built for every request, so each sets
# only the attributes its type uses.
ATTRIBUTE_DEFAULTS = {
    "field": b"",
    "data": b"",
    "origin": b"",
    "error_code": 0,
    "pad_length": 0,
    "last_stream_id": 0,
    "additional_data": 0,
    "depends_on": 0x0,
    "stream_weight": 0x0,
    "exclusive": False,
    "opaque_data": b"",
    "promised_stream_id": 0,
    "window_increment": 0,
    "flag_byte": 0x0,
    "body_len": 0,
}


class Frame:
    __slots__ = (
        "stream_id",
        "flags",
        "body_len",
        "flags",
        "type",
        "frame_type",
        "data",
        "settings",
        "origin",
        "field",
        "error_code",
        "pad_length",
        "last_stream_id",
        "additional_data",
        "depends_on",
        "stream_weight",
        "exclusive",
        "opaque_data",
        "promised_stream_id",
        "window_increment",
        "flag_byte",
        "defined_flags",
    )

    FRAMES = {}
    """
    The base class for all HTTP/2 frames.
    """
    #: The flags defined on this type of frame.

    # If 'has-stream', the frame's stream_id must be non-zero. If 'no-stream',
    # it must be zero. If 'either', it's not checked.
    stream_association: Optional[str] = None
    frame_types = {
        0xA: sys.intern("ALTSVC"),
        0x09: sys.intern("CONTINUATION"),
        0x0: sys.intern("DATA"),
        0x07: sys.intern("GOAWAY"),
        0x01: sys.intern("HEADERS"),
        0x06: sys.intern("PING"),
        0x02: sys.intern("PRIORITY"),
        0x05: sys.intern("PUSHPROMISE"),
        0x03: sys.intern("RESET"),
        0x04: sys.intern("SETTINGS"),
        0x08: sys.intern("WINDOWUPDATE"),
    }

    def __init__(
        self,
        stream_id: int,
        frame_type: int,
        flags: Iterable[str] = (),
        parsed_flag_byte: int = 0,
        **kwargs: Any,
    ) -> None:
        #: The stream identifier for the stream this frame was received on.
        #: Set to 0 for frames sent on the connection (stream-id 0).
        self.stream_id = stream_id
        self.type = frame_type
        self.frame_type = self.frame_types.get(frame_type)
        self.defined_flags: Tuple[Flag, ...] = NO_FLAGS
        flag_names = NO_FLAG_NAMES

        #: The flags set for this frame.
        self.flags = Flags()
        if flags:
            self.flags.update(flags)

        # The most frequent frame types first. Attributes a type does not set
        # read their ATTRIBUTE_DEFAULTS value (see __getattr__).
        if frame_type == 0x0:
            # DATA
            self.defined_flags = DATA_FLAGS
            flag_names = DATA_FLAG_NAMES

            self.pad_length = kwargs.get("pad_length", 0)
            self.data = kwargs.get("data", b"")

        elif frame_type == 0x01:
            # HEADERS
            self.defined_flags = HEADERS_FLAGS
            flag_names = HEADERS_FLAG_NAMES

            self.data = kwargs.get("data", b"")
            self.pad_length = kwargs.get("pad_length", 0)
            self.depends_on = kwargs.get("depends_on", 0x0)
            self.stream_weight = kwargs.get("stream_weight", 0x0)
            self.exclusive = kwargs.get("exclusive", False)

        elif frame_type == 0x04:
            # SETTINGS
            self.defined_flags = ACK_FLAGS
            flag_names = ACK_FLAG_NAMES

            self.settings = kwargs.get("settings", {})

        elif frame_type == 0x08:
            # WINDOW UPDATE
            self.window_increment = kwargs.get("window_increment", 0)

        elif frame_type == 0x03:
            # RESET
            self.error_code = kwargs.get("error_code", 0)

        elif frame_type == 0x06:
            # PING
            self.defined_flags = ACK_FLAGS
            flag_names = ACK_FLAG_NAMES

            self.opaque_data = kwargs.get("opaque_data", b"")

        elif frame_type == 0x07:
            # GOAWAY
            self.last_stream_id = kwargs.get("last_stream_id", 0)
            self.additional_data = kwargs.get("additional_data", b"")
            self.error_code = kwargs.get("error_code", 0)

        elif frame_type == 0x09:
            # CONTINUATION
            self.defined_flags = CONTINUATION_FLAGS
            flag_names = CONTINUATION_FLAG_NAMES

            self.data = kwargs.get("data")

        elif frame_type == 0x02:
            # PRIORITY
            self.depends_on = kwargs.get("depends_on", 0x0)
            self.stream_weight = kwargs.get("stream_weight", 0x0)
            self.exclusive = kwargs.get("exclusive", False)

        elif frame_type == 0x05:
            # PUSH PROMISE
            self.defined_flags = PUSH_PROMISE_FLAGS
            flag_names = PUSH_PROMISE_FLAG_NAMES

            self.promised_stream_id = kwargs.get("promised_stream_id", 0)
            self.pad_length = kwargs.get("pad_length", 0)
            self.data = kwargs.get("data", b"")

        elif frame_type == 0xA:
            # ALTSVC
            self.origin = kwargs.get("origin", b"")
            self.field = kwargs.get("fields", b"")

        else:
            # EXTENSION
            self.flag_byte = kwargs.get("flag_byte", 0x0)

        if parsed_flag_byte:
            self.flags.update(flag_names[parsed_flag_byte & 0xFF])

    def __getattr__(self, name: str) -> Any:
        # Reached only for an attribute this frame's type never set.
        if name == "settings":
            # Mutable, so each frame gets its own, kept once read.
            self.settings = {}
            return self.settings

        try:
            return ATTRIBUTE_DEFAULTS[name]

        except KeyError:
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}"
            ) from None

    def __repr__(self) -> str:
        body_repr = (self._body_repr(),)
        return f"{type(self).__name__}(stream_id={self.stream_id}, flags={repr(self.flags)}): {body_repr}"

    def _body_repr(self) -> str:
        # More specific implementation may be provided by subclasses of Frame.
        # This fallback shows the serialized (and truncated) body content.
        return raw_data_repr(self.serialize())

    @property
    def flow_controlled_length(self) -> int:
        """
        The length of the frame that needs to be accounted for when considering
        flow control.
        """
        padding_len = 0
        if "PADDED" in self.flags:
            # Account for extra 1-byte padding length field, which is still
            # present if possibly zero-valued.
            padding_len = self.pad_length + 1
        return len(self.data) + padding_len

    def parse_flags(self, flag_byte: int) -> Flags:
        for flag, flag_bit in self.defined_flags:
            if flag_byte & flag_bit:
                self.flags.add(flag)

        return self.flags

    def serialize(self) -> bytes:
        """
        Convert a frame into a bytestring, representing the serialized form of
        the frame.
        """

        body = b""
        flags = 0

        if self.type == 0xA:
            # ALTSVC
            origin_len = _STRUCT_H.pack(len(self.origin))
            body = origin_len + self.origin + self.field

        elif self.type == 0x09:
            # CONTINUATION

            body = self.data

        elif self.type == 0x0:
            # DATA

            padding_data = b""
            if "PADDED" in self.flags:  # type: ignore
                padding_data = _STRUCT_B.pack(self.pad_length)

            padding = b"\0" * self.pad_length
            body = padding_data + self.data + padding

        elif self.type == 0x07:
            # GOAWAY

            self.data = _STRUCT_LL.pack(
                self.last_stream_id & 0x7FFFFFFF, self.error_code
            )

            body = self.data + self.additional_data

        elif self.type == 0x01:
            # HEADERS

            padding_data = b""
            if "PADDED" in self.flags:  # type: ignore
                padding_data = _STRUCT_B.pack(self.pad_length)

            padding = b"\0" * self.pad_length

            if "PRIORITY" in self.flags:
                priority_data = _STRUCT_LB.pack(
                    self.depends_on + (0x80000000 if self.exclusive else 0),
                    self.stream_weight,
                )
            else:
                priority_data = b""

            body = padding_data + priority_data + self.data + padding

        elif self.type == 0x06:
            # PING

            body = self.opaque_data
            body += b"\x00" * (8 - len(body))

        elif self.type == 0x02:
            # PRIORITY
            body = _STRUCT_LB.pack(
                self.depends_on + (0x80000000 if self.exclusive else 0),
                self.stream_weight,
            )

        elif self.type == 0x05:
            # PUSH PROMISE

            padding_data = b""
            if "PADDED" in self.flags:  # type: ignore
                padding_data = _STRUCT_B.pack(self.pad_length)

            padding = b"\0" * self.pad_length
            promise_data = _STRUCT_L.pack(self.promised_stream_id)

            body = padding_data + promise_data + self.data + padding

        elif self.type == 0x03:
            # RESET
            body = _STRUCT_L.pack(self.error_code)

        elif self.type == 0x04:
            # SETTINGS
            body = b"".join([
                _STRUCT_HL.pack(
                    setting & 0xFF, 
                    value,
                )
                for setting, value in self.settings.items()
            ])

        elif self.type == 0x08:
            # WINDOW UPDATE
            body = _STRUCT_L.pack(self.window_increment & 0x7FFFFFFF)

        else:
            # EXTENSION
            flags = self.flag_byte

        self.body_len = len(body)

        for flag, flag_bit in self.defined_flags:
            if flag in self.flags:
                flags |= flag_bit

        header = _STRUCT_HBBBL.pack(
            (self.body_len >> 8) & 0xFFFF,  # Length spread over top 24 bits
            self.body_len & 0xFF,
            self.type,
            flags,
            self.stream_id & 0x7FFFFFFF,  # Stream ID is 32 bits.
        )

        return header + body
