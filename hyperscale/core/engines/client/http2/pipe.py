import struct
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)

from .config import H2Configuration
from .errors import (
    ErrorCodes,
    StreamError,
)
from .events import (
    ConnectionTerminated,
    StreamReset,
    WindowUpdated,
)
from .fast_hpack import ConnectionEncoder, Decoder
from .frames.types.attributes import _STRUCT_HBBBL
from .frames.types.base_frame import Frame
from .protocols import HTTP2Connection
from .settings import SettingCodes, Settings, StreamClosedBy
from .windows import WindowManager

# A whole frame whose payload is one 32-bit word, packed inline: WINDOW_UPDATE
# (the increment) or RST_STREAM (the error code). RFC 9113 4.1, 6.4, 6.9.
_FRAME_WITH_UINT32 = struct.Struct(">HBBBLL")

# GOAWAY for a frame larger than the SETTINGS_MAX_FRAME_SIZE we advertised:
# last stream id 0 (the server opens none), FRAME_SIZE_ERROR (RFC 9113 4.2,
# 6.8).
_GOAWAY_FRAME_SIZE_ERROR = _FRAME_WITH_UINT32.pack(0, 8, 0x07, 0, 0, 0) + struct.pack(
    ">L", ErrorCodes.FRAME_SIZE_ERROR
)

# Frame flags (RFC 9113 6.1, 6.2).
_END_STREAM = 0x01
_END_HEADERS = 0x04
_PADDED = 0x08
_PRIORITY = 0x20
# A HEADERS frame the response fast path takes: a whole header block,
# without padding or priority fields.
_HEADERS_BLOCK_FLAGS = _END_HEADERS | _PADDED | _PRIORITY

# Frame types (RFC 9113 6.2, 6.10).
_HEADERS_FRAME = 0x01

# Each :status value, three ASCII digits (RFC 9113 8.3.2; RFC 9110 15),
# with its code: one lookup both checks a value and reads it.
_STATUS_CODES = {f"{code:03d}": code for code in range(1000)}
_CONTINUATION_FRAME = 0x09

_unpack_frame_header = _STRUCT_HBBBL.unpack_from


class HTTP2Pipe:
    __slots__ = (
        "connected",
        "concurrency",
        "_encoder",
        "_decoder",
        "_init_sent",
        "local_settings",
        "remote_settings",
        "outbound_flow_control_window",
        "_inbound_flow_control_window_manager",
        "local_settings_dict",
        "remote_settings_dict",
        "closed_by",
        "_early_response",
        "_local_initial_window_size",
        "_local_max_frame_size",
        "_remote_initial_window_size",
        "_remote_max_frame_size",
    )

    CONFIG = H2Configuration(
        validate_inbound_headers=False,
    )

    def __init__(self, concurrency: int):
        self.connected = False
        self.concurrency = concurrency
        # One per connection: its dynamic table mirrors this peer's decoder.
        self._encoder = ConnectionEncoder()
        self._decoder = Decoder()
        self._decoder.max_allowed_table_size = self._decoder.header_table.maxsize
        self._init_sent = False
        self.closed_by: StreamClosedBy | ErrorCodes | None = None
        self._early_response: Tuple[int, Dict[bytes, bytes], bytes, Optional[Exception]] | None = None

        self.local_settings = Settings(
            client=True,
            initial_values={
                SettingCodes.MAX_CONCURRENT_STREAMS: concurrency,
                SettingCodes.MAX_HEADER_LIST_SIZE: 2**16,
                # The pipe reads only the request's own stream and has no
                # PUSH_PROMISE handling, so servers must not push.
                SettingCodes.ENABLE_PUSH: 0,
            },
        )
        self.remote_settings = Settings(client=False)

        self.outbound_flow_control_window = self.remote_settings.initial_window_size

        del self.local_settings[SettingCodes.ENABLE_CONNECT_PROTOCOL]

        self._refresh_settings()

        self._inbound_flow_control_window_manager = WindowManager(
            max_window_size=self.local_settings.initial_window_size
        )

        self.local_settings_dict = {
            setting_name: setting_value
            for setting_name, setting_value in self.local_settings.items()
        }
        self.remote_settings_dict = {
            setting_name: setting_value
            for setting_name, setting_value in self.remote_settings.items()
        }

    def _refresh_settings(self):
        # The settings every request reads, cached: they change only when a
        # SETTINGS frame is acknowledged.
        self._local_initial_window_size = self.local_settings.initial_window_size
        self._local_max_frame_size = self.local_settings.max_frame_size
        self._remote_initial_window_size = self.remote_settings.initial_window_size
        self._remote_max_frame_size = self.remote_settings.max_frame_size

    def _guard_increment_window(self, current, increment):
        # The largest value the flow control window may take.
        LARGEST_FLOW_CONTROL_WINDOW = 2**31 - 1

        new_size = current + increment

        if new_size > LARGEST_FLOW_CONTROL_WINDOW:
            # RFC 9113 6.9.1: a FLOW_CONTROL_ERROR, which fails the request.
            raise StreamError(
                f"Flow control window may not exceed {LARGEST_FLOW_CONTROL_WINDOW}"
            )

        return new_size

    def send_preamble(self, connection: HTTP2Connection):
        """
        Writes the connection preface (RFC 9113 3.4), if it has not been sent:
        the client magic, our SETTINGS, and a connection WINDOW_UPDATE granting
        65,536 bytes, recorded in the inbound window so the window the server
        sees matches ours. send_request_headers sends the same preface with a
        connection's first request, so its callers need not call this.
        """
        if self._init_sent is False:
            settings_frame = Frame(0, 0x04)
            for setting, value in self.local_settings.items():
                settings_frame.settings[setting] = value

            connection.write(
                b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                + settings_frame.serialize()
                + _FRAME_WITH_UINT32.pack(0, 4, 0x08, 0, 0, 65536)
            )

            self._inbound_flow_control_window_manager.window_opened(65536)
            self._init_sent = True
            self.outbound_flow_control_window = self.remote_settings.initial_window_size

        return connection

    def send_request_headers(
        self,
        headers: bytes,
        data: Optional[bytes],
        connection: HTTP2Connection,
    ):
        stream = connection.stream
        remote_initial_window_size = self._remote_initial_window_size

        # The stream's windows as new WindowManagers start them, reused rather
        # than reallocated for every request, and set inline: the inbound one
        # already opened by the 65,536 bytes the WINDOW_UPDATE below grants.
        inbound_window_size = self._local_initial_window_size + 65536

        if (inbound := stream.inbound) is None:
            stream.inbound = WindowManager(inbound_window_size)

        else:
            inbound.max_window_size = inbound_window_size
            inbound.current_window_size = inbound_window_size
            inbound.bytes_processed = 0

        if (outbound := stream.outbound) is None:
            stream.outbound = WindowManager(remote_initial_window_size)

        else:
            outbound.max_window_size = remote_initial_window_size
            outbound.current_window_size = remote_initial_window_size
            outbound.bytes_processed = 0

        stream.max_inbound_frame_size = self._local_max_frame_size
        stream.max_outbound_frame_size = self._remote_max_frame_size
        stream.current_outbound_window_size = remote_initial_window_size

        stream_id = stream.stream_id

        # The HEADERS frame is packed directly, byte for byte what
        # Frame.serialize() builds: END_HEADERS, plus END_STREAM when no body
        # follows. The WINDOW_UPDATE grants the 65,536 bytes just recorded,
        # so the stream window the server sees matches ours.
        header_block_length = len(headers)
        end_stream = 0 if data is not None else _END_STREAM

        if header_block_length <= self._remote_max_frame_size:
            frames = (
                _STRUCT_HBBBL.pack(
                    (header_block_length >> 8) & 0xFFFF,
                    header_block_length & 0xFF,
                    _HEADERS_FRAME,
                    _END_HEADERS | end_stream,
                    stream_id & 0x7FFFFFFF,
                )
                + headers
                + _FRAME_WITH_UINT32.pack(0, 4, 0x08, 0, stream_id, 65536)
            )

        else:
            # Past the peer's largest frame: a HEADERS frame and the
            # CONTINUATION frames that carry the rest of the block, back to
            # back (RFC 9113 4.3, 6.10) -- END_STREAM only on the HEADERS
            # frame, END_HEADERS only on the last.
            max_frame_size = self._remote_max_frame_size
            block = memoryview(headers)
            frames = bytearray()
            frame_type = _HEADERS_FRAME
            flags = end_stream

            for fragment_start in range(0, header_block_length, max_frame_size):
                fragment_end = min(fragment_start + max_frame_size, header_block_length)
                if fragment_end == header_block_length:
                    flags |= _END_HEADERS

                fragment_length = fragment_end - fragment_start
                frames += _STRUCT_HBBBL.pack(
                    (fragment_length >> 8) & 0xFFFF,
                    fragment_length & 0xFF,
                    frame_type,
                    flags,
                    stream_id & 0x7FFFFFFF,
                )
                frames += block[fragment_start:fragment_end]

                frame_type = _CONTINUATION_FRAME
                flags = 0

            frames += _FRAME_WITH_UINT32.pack(0, 4, 0x08, 0, stream_id, 65536)

        if self._init_sent is False:
            # The connection's first request: the preface (send_preamble's,
            # byte for byte) goes ahead of it in the same write.
            settings_frame = Frame(0, 0x04)
            for setting, value in self.local_settings.items():
                settings_frame.settings[setting] = value

            stream.writer._transport.write(
                b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                + settings_frame.serialize()
                + _FRAME_WITH_UINT32.pack(0, 4, 0x08, 0, 0, 65536)
                + frames
            )

            self._inbound_flow_control_window_manager.window_opened(65536)
            self._init_sent = True
            self.outbound_flow_control_window = self.remote_settings.initial_window_size

        else:
            stream.writer._transport.write(frames)

        return connection

    async def receive_response(
        self,
        connection: HTTP2Connection,
        until_window_open: bool = False,
        head_request: bool = False,
    ):
        if (early_response := self._early_response) is not None:
            # submit_request_body already read this response: the server sent
            # it before taking the whole request body.
            self._early_response = None
            return early_response

        # Each DATA payload, copied once: a body of one frame is returned as
        # its chunk, with no second copy.
        body_chunks: List[bytes] = []
        status_code: Optional[int] = 200
        # Set with the final response's header section: a caller reads it
        # only once that arrived.
        headers_dict: Optional[Dict[str, str]] = None
        # The trailer section's fields, apart from the header section's.
        trailers_dict: Optional[Dict[str, str]] = None
        # Whether the final response's header section has arrived: a
        # HEADERS frame after it is the trailer section.
        header_section_done = False
        error: Optional[Exception] = None

        stream = connection.stream
        stream_id = stream.stream_id
        frame_buffer = stream.frame_buffer
        reader = stream.reader
        connection_window = self._inbound_flow_control_window_manager
        stream_window = stream.inbound
        decoder = self._decoder
        decode = decoder.decode
        status_codes = _STATUS_CODES
        response_started = False

        # The frames are parsed in place, in the buffer the reader owns for
        # its transport's lifetime, from its _start up to its _end. Where the
        # last parse left unparsed: while that many bytes are unparsed there
        # is nothing new to parse. Every read adds to them; making room moves
        # them to the front without changing how many there are. (Where the
        # buffer ends cannot tell: a read that fills the room made ends it
        # where the last parse did.)
        buffer = reader._buffer
        view = reader._view
        parsed_unparsed = -1
        max_inbound_frame_size = stream.max_inbound_frame_size
        frames = None

        done = False
        while done is False:
            if (buffer_length := reader._end) - reader._start == parsed_unparsed:
                # Nothing new: the transport's error or the end of the
                # stream ends the read, else it waits -- on the reader's
                # future itself.
                if reader._exception is not None:
                    return (400, headers_dict, b"".join(body_chunks), reader._exception, trailers_dict)

                if reader._eof:
                    # HTTP/2 ends a response with END_STREAM, never by closing
                    # the connection, so this response is incomplete.
                    error = Exception(
                        f"Connection - {stream_id} err: connection closed before the stream ended"
                    )
                    break

                try:
                    await reader.data_waiter()

                except Exception as err:
                    return (400, headers_dict, b"".join(body_chunks), err, trailers_dict)

                continue

            offset = reader._start

            try:
                while buffer_length - offset >= 9:
                    (
                        length_high,
                        length_low,
                        frame_type,
                        flags,
                        frame_stream_id,
                    ) = _unpack_frame_header(buffer, offset)

                    frame_start = offset + 9
                    frame_end = frame_start + (length_high << 8) + length_low

                    if frame_end - frame_start > max_inbound_frame_size:
                        # Larger than the SETTINGS_MAX_FRAME_SIZE we advertised:
                        # a connection error of type FRAME_SIZE_ERROR (RFC 9113
                        # 4.2, 5.4.1). Parsing stops here; a response already
                        # complete stands, and the next read on this connection
                        # meets the frame again and fails.
                        if done is False:
                            if stream.writer is not None:
                                connection.write(_GOAWAY_FRAME_SIZE_ERROR)

                            status_code = 400
                            error = Exception(
                                f"Connection - {stream_id} err: a frame of {frame_end - frame_start} bytes "
                                f"exceeds SETTINGS_MAX_FRAME_SIZE {max_inbound_frame_size}"
                            )
                            done = True

                        break

                    if frame_end > buffer_length:
                        # The rest of this frame has not arrived.
                        break

                    frame_stream_id &= 0x7FFFFFFF

                    if (
                        frame_type == 0x0
                        and not flags & _PADDED
                        and not frame_buffer._headers_buffer
                    ):
                        # DATA without padding: the hot path, parsed and handled
                        # in place, flow control inlined. Its flow-controlled
                        # length is its payload's.
                        offset = frame_end
                        flow_controlled_length = frame_end - frame_start

                        connection_window.current_window_size -= flow_controlled_length
                        if connection_processed := (
                            connection_window.bytes_processed + flow_controlled_length
                        ):
                            connection_max_window = connection_window.max_window_size
                            connection_current_window = connection_window.current_window_size

                            if connection_processed >= connection_max_window // 2 or (
                                connection_current_window == 0
                                and connection_processed > min(1024, connection_max_window // 4)
                            ):
                                connection_increment = min(
                                    connection_processed,
                                    connection_max_window - connection_current_window,
                                )
                                connection_window.bytes_processed = 0
                                connection_window.current_window_size = (
                                    connection_current_window + connection_increment
                                )

                                if connection_increment:
                                    stream.write_window_update_frame(0, connection_increment)

                            else:
                                connection_window.bytes_processed = connection_processed

                        if frame_stream_id != stream_id:
                            # DATA for an earlier stream on this connection only
                            # consumed connection-level window.
                            continue

                        if header_section_done is False:
                            # DATA before the final response's header section (RFC 9113
                            # 8.1): malformed (8.1.1).
                            if done is False:
                                if stream.writer is not None:
                                    connection.write(
                                        _FRAME_WITH_UINT32.pack(0, 4, 0x03, 0, stream_id, ErrorCodes.PROTOCOL_ERROR)
                                    )

                                status_code = 400
                                error = Exception(
                                    f"Connection - {stream_id} err: malformed response: DATA before its HEADERS"
                                )
                                done = True

                            continue

                        response_started = True
                        stream_window.current_window_size -= flow_controlled_length
                        body_chunks.append(bytes(view[frame_start:frame_end]))

                        if flags & _END_STREAM:
                            done = True

                        # A closed stream receives no more DATA, and RFC 9113 5.1
                        # forbids sending WINDOW_UPDATE on it.
                        elif stream_processed := (
                            stream_window.bytes_processed + flow_controlled_length
                        ):
                            stream_max_window = stream_window.max_window_size
                            stream_current_window = stream_window.current_window_size

                            if stream_processed >= stream_max_window // 2 or (
                                stream_current_window == 0
                                and stream_processed > min(1024, stream_max_window // 4)
                            ):
                                stream_increment = min(
                                    stream_processed,
                                    stream_max_window - stream_current_window,
                                )
                                stream_window.bytes_processed = 0
                                stream_window.current_window_size = (
                                    stream_current_window + stream_increment
                                )

                                if stream_increment:
                                    stream.write_window_update_frame(None, stream_increment)

                            else:
                                stream_window.bytes_processed = stream_processed

                        continue

                    if (
                        frame_type == 0x01
                        and flags & _HEADERS_BLOCK_FLAGS == _END_HEADERS
                        and not frame_buffer._headers_buffer
                    ):
                        # A whole header block without padding or priority:
                        # handled in place, as the frame below would be.
                        offset = frame_end

                        if frame_stream_id and frame_stream_id != stream_id:
                            # A late frame for an earlier stream on this
                            # connection. Its header block still passes through
                            # the connection-wide HPACK decoder, or the dynamic
                            # table falls out of step with the server.
                            try:
                                decode(buffer[frame_start:frame_end], True)

                            except Exception as headers_read_err:
                                status_code = 400
                                error = Exception(
                                    f"Connection - {stream_id} err: HPACK state lost on stream {frame_stream_id}: {headers_read_err}"
                                )
                                done = True

                            continue

                        response_started = True
                        headers: List[Tuple[str, str]] = ()

                        try:
                            headers = decode(buffer[frame_start:frame_end], True)

                        except Exception as headers_read_err:
                            status_code = status_code or 400
                            error = headers_read_err
                            done = True

                        # A stream already ended or failed is decoded only to
                        # keep the HPACK table in step.
                        if done is False:
                            end_stream = flags & _END_STREAM

                            # A response is any number of interim (1xx) header sections, the
                            # final response's, its DATA, and an optional trailer section (RFC
                            # 9113 8.1); a client must not accept a malformed one (8.1.1). The
                            # decoder checked every field: its characters (8.2.1), and that it
                            # is neither a pseudo-header field but :status (8.3) nor a
                            # connection-specific field (8.2.2).
                            malformed: Optional[str] = decoder.malformed_field

                            if malformed is None:
                                if header_section_done:
                                    # The trailer section: it ends the stream, holds no :status
                                    # (RFC 9113 8.1, 8.3), and stays apart from the header
                                    # section (RFC 9110 6.5).
                                    if not end_stream:
                                        malformed = "a HEADERS frame without END_STREAM after the header section"

                                    else:
                                        trailers_dict = dict(headers)
                                        if ":status" in trailers_dict:
                                            malformed = "pseudo-header field :status in the trailer section"

                                        done = True

                                elif not headers or headers[0][0] != ":status":
                                    # Every response, interim ones too, leads with :status (RFC
                                    # 9113 8.3, 8.3.2).
                                    malformed = "no :status leading the header section"

                                elif (response_code := status_codes.get(headers[0][1])) is None:
                                    malformed = f"invalid :status {headers[0][1]}"

                                elif 99 < response_code < 200:
                                    # An interim response: passed over -- its fields are not the
                                    # response's (RFC 9110 15.2) -- it cannot end the stream, and
                                    # holds one :status (RFC 9113 8.1, 8.3).
                                    if end_stream:
                                        malformed = "an interim (1xx) response with END_STREAM"

                                    elif [field_name for field_name, _ in headers].count(":status") > 1:
                                        malformed = "pseudo-header field :status repeated"

                                else:
                                    # The fields by name, :status taken out. A name that repeats --
                                    # the map then holds fewer fields than the block -- may be a
                                    # second :status, or content-length values that disagree (RFC
                                    # 9113 8.3; RFC 9110 8.6).
                                    headers_dict = dict(headers)
                                    del headers_dict[":status"]
                                    if len(headers_dict) != len(headers) - 1:
                                        status_lines = 0
                                        content_lengths = set()
                                        for field_name, field_value in headers:
                                            if field_name == ":status":
                                                status_lines += 1

                                            elif field_name == "content-length":
                                                content_lengths.add(field_value)

                                        if status_lines > 1:
                                            malformed = "pseudo-header field :status repeated"

                                        elif len(content_lengths) > 1:
                                            malformed = "conflicting content-length values"

                                    status_code = response_code
                                    header_section_done = True
                                    if end_stream:
                                        done = True

                            if malformed is not None:
                                # A stream error of type PROTOCOL_ERROR (RFC 9113 8.1.1): the
                                # stream is reset and the request fails.
                                if stream.writer is not None:
                                    connection.write(
                                        _FRAME_WITH_UINT32.pack(0, 4, 0x03, 0, stream_id, ErrorCodes.PROTOCOL_ERROR)
                                    )

                                status_code = 400
                                error = Exception(f"Connection - {stream_id} err: malformed response: {malformed}")
                                done = True

                        continue

                    # Every other frame -- and DATA or HEADERS with padding,
                    # priority, or a header block in progress -- is built and
                    # handled as before.
                    frame = frame_buffer.parse_frame(
                        frame_type,
                        flags,
                        frame_stream_id,
                        buffer[frame_start:frame_end],
                    )

                    offset = frame_end

                    frame = frame_buffer.through_header_block(frame, frame_type)
                    if not frame:
                        continue

                    if frame.type == 0x0:
                        # DATA with padding; DATA without takes the path above.
                        flow_controlled_length = frame.flow_controlled_length

                        connection_window.window_consumed(flow_controlled_length)
                        if connection_increment := connection_window.process_bytes(
                            flow_controlled_length
                        ):
                            stream.write_window_update_frame(
                                stream_id=0, window_increment=connection_increment
                            )

                        if frame.stream_id != stream_id:
                            # DATA for an earlier stream on this connection only
                            # consumed connection-level window.
                            continue

                        if header_section_done is False:
                            # DATA before the final response's header section (RFC 9113
                            # 8.1): malformed (8.1.1).
                            if done is False:
                                if stream.writer is not None:
                                    connection.write(
                                        _FRAME_WITH_UINT32.pack(0, 4, 0x03, 0, stream_id, ErrorCodes.PROTOCOL_ERROR)
                                    )

                                status_code = 400
                                error = Exception(
                                    f"Connection - {stream_id} err: malformed response: DATA before its HEADERS"
                                )
                                done = True

                            continue

                        response_started = True
                        stream_window.window_consumed(flow_controlled_length)
                        body_chunks.append(frame.data)

                        if "END_STREAM" in frame.flags:
                            done = True

                        # A closed stream receives no more DATA, and RFC 9113 5.1
                        # forbids sending WINDOW_UPDATE on it.
                        elif stream_increment := stream_window.process_bytes(
                            flow_controlled_length
                        ):
                            stream.write_window_update_frame(
                                window_increment=stream_increment
                            )

                        continue

                    if frame.stream_id and frame.stream_id != stream_id:
                        # A late frame for an earlier stream on this connection,
                        # e.g. a cancelled stream's response. Its header block
                        # still passes through the connection-wide HPACK decoder,
                        # or the dynamic table falls out of step with the server.
                        if frame.type == 0x01:
                            try:
                                self._decoder.decode(frame.data, raw=True)

                            except Exception as headers_read_err:
                                status_code = 400
                                error = Exception(
                                    f"Connection - {stream_id} err: HPACK state lost on stream {frame.stream_id}: {headers_read_err}"
                                )
                                done = True

                        continue

                    try:
                        if frame.type == 0x07:
                            # GOAWAY

                            new_event = ConnectionTerminated()
                            new_event.error_code = ErrorCodes(frame.error_code)
                            new_event.last_stream_id = frame.last_stream_id

                            if frame.additional_data:
                                new_event.additional_data = frame.additional_data

                            frames = []

                            # The server still finishes streams up to last_stream_id;
                            # a later one was never processed and never will be.
                            if done is False and frame.last_stream_id < stream_id:
                                status_code = 400
                                error = Exception(
                                    f"Connection - {stream_id} err: {str(new_event)}"
                                )
                                done = True

                        elif frame.type == 0x01:
                            # HEADERS
                            response_started = True
                            headers: List[Tuple[bytes, bytes]] = {}

                            try:
                                headers = self._decoder.decode(frame.data, raw=True)

                            except Exception as headers_read_err:
                                status_code = status_code or 400
                                error = headers_read_err
                                done = True

                            # A stream already ended or failed is decoded only to
                            # keep the HPACK table in step. END_STREAM is on the
                            # HEADERS frame, not on its CONTINUATION frames.
                            if done is False:
                                end_stream = "END_STREAM" in frame.flags

                                # A response is any number of interim (1xx) header sections, the
                                # final response's, its DATA, and an optional trailer section (RFC
                                # 9113 8.1); a client must not accept a malformed one (8.1.1). The
                                # decoder checked every field: its characters (8.2.1), and that it
                                # is neither a pseudo-header field but :status (8.3) nor a
                                # connection-specific field (8.2.2).
                                malformed: Optional[str] = decoder.malformed_field

                                if malformed is None:
                                    if header_section_done:
                                        # The trailer section: it ends the stream, holds no :status
                                        # (RFC 9113 8.1, 8.3), and stays apart from the header
                                        # section (RFC 9110 6.5).
                                        if not end_stream:
                                            malformed = "a HEADERS frame without END_STREAM after the header section"

                                        else:
                                            trailers_dict = dict(headers)
                                            if ":status" in trailers_dict:
                                                malformed = "pseudo-header field :status in the trailer section"

                                            done = True

                                    elif not headers or headers[0][0] != ":status":
                                        # Every response, interim ones too, leads with :status (RFC
                                        # 9113 8.3, 8.3.2).
                                        malformed = "no :status leading the header section"

                                    elif (response_code := status_codes.get(headers[0][1])) is None:
                                        malformed = f"invalid :status {headers[0][1]}"

                                    elif 99 < response_code < 200:
                                        # An interim response: passed over -- its fields are not the
                                        # response's (RFC 9110 15.2) -- it cannot end the stream, and
                                        # holds one :status (RFC 9113 8.1, 8.3).
                                        if end_stream:
                                            malformed = "an interim (1xx) response with END_STREAM"

                                        elif [field_name for field_name, _ in headers].count(":status") > 1:
                                            malformed = "pseudo-header field :status repeated"

                                    else:
                                        # The fields by name, :status taken out. A name that repeats --
                                        # the map then holds fewer fields than the block -- may be a
                                        # second :status, or content-length values that disagree (RFC
                                        # 9113 8.3; RFC 9110 8.6).
                                        headers_dict = dict(headers)
                                        del headers_dict[":status"]
                                        if len(headers_dict) != len(headers) - 1:
                                            status_lines = 0
                                            content_lengths = set()
                                            for field_name, field_value in headers:
                                                if field_name == ":status":
                                                    status_lines += 1

                                                elif field_name == "content-length":
                                                    content_lengths.add(field_value)

                                            if status_lines > 1:
                                                malformed = "pseudo-header field :status repeated"

                                            elif len(content_lengths) > 1:
                                                malformed = "conflicting content-length values"

                                        status_code = response_code
                                        header_section_done = True
                                        if end_stream:
                                            done = True

                                if malformed is not None:
                                    # A stream error of type PROTOCOL_ERROR (RFC 9113 8.1.1): the
                                    # stream is reset and the request fails.
                                    if stream.writer is not None:
                                        connection.write(
                                            _FRAME_WITH_UINT32.pack(0, 4, 0x03, 0, stream_id, ErrorCodes.PROTOCOL_ERROR)
                                        )

                                    status_code = 400
                                    error = Exception(f"Connection - {stream_id} err: malformed response: {malformed}")
                                    done = True

                            frames = []

                        elif frame.type == 0x03 and done is False:
                            # RESET

                            self.closed_by = StreamClosedBy.RECV_RST_STREAM
                            reset_event = StreamReset()
                            reset_event.stream_id = connection.stream.stream_id

                            reset_event.error_code = ErrorCodes(frame.error_code)

                            status_code = 400
                            error = Exception(
                                f"Connection - {connection.stream.stream_id} - err: {str(reset_event)}"
                            )
                            done = True

                        elif frame.type == 0x04:
                            # SETTINGS

                            if "ACK" in frame.flags:
                                changes = self.local_settings.acknowledge()
                                self._refresh_settings()
                                if SettingCodes.INITIAL_WINDOW_SIZE in changes:
                                    setting = changes[SettingCodes.INITIAL_WINDOW_SIZE]
                                    delta = setting.new_value - (setting.original_value or 0)

                                    new_max_size = connection.stream.inbound.max_window_size + delta
                                    connection.stream.inbound.window_opened(delta)
                                    connection.stream.inbound.max_window_size = new_max_size


                                if SettingCodes.MAX_HEADER_LIST_SIZE in changes:
                                    setting = changes[SettingCodes.MAX_HEADER_LIST_SIZE]
                                    self._decoder.max_header_list_size = setting.new_value

                                if SettingCodes.MAX_FRAME_SIZE in changes:
                                    # _refresh_settings() gave later streams the
                                    # acknowledged limit; this one takes it now.
                                    setting = changes[SettingCodes.MAX_FRAME_SIZE]
                                    connection.stream.max_inbound_frame_size = setting.new_value
                                    max_inbound_frame_size = setting.new_value

                                if SettingCodes.HEADER_TABLE_SIZE in changes:
                                    setting = changes[SettingCodes.HEADER_TABLE_SIZE]
                                    # This is safe across all hpack versions: some versions just won't
                                    # respect it.
                                    self._decoder.max_allowed_table_size = setting.new_value



                            else:
                                self.remote_settings.update(frame.settings)

                                changes = self.remote_settings.acknowledge()
                                self._refresh_settings()

                                if SettingCodes.INITIAL_WINDOW_SIZE in changes:
                                    setting = changes[SettingCodes.INITIAL_WINDOW_SIZE]

                                    delta = setting.new_value - (setting.original_value or 0)

                                    connection.stream.current_outbound_window_size = self._guard_increment_window(
                                        connection.stream.current_outbound_window_size,
                                        delta,
                                    )

                                # HEADER_TABLE_SIZE changes by the remote part affect our encoder: cf.
                                # RFC 7540 Section 6.5.2.
                                if SettingCodes.HEADER_TABLE_SIZE in changes:
                                    setting = changes[SettingCodes.HEADER_TABLE_SIZE]
                                    self._encoder.header_table_size = setting.new_value

                                if SettingCodes.MAX_FRAME_SIZE in changes:
                                    setting = changes[SettingCodes.MAX_FRAME_SIZE]
                                    connection.stream.max_outbound_frame_size = setting.new_value

                                settings_frame = Frame(0, 0x04, flags=['ACK'])
                                settings_frame.flags.add("ACK")

                                frames = [settings_frame]

                        elif frame.type == 0x06:
                            # PING: answer it. Its opaque data is not response body.

                            if "ACK" not in frame.flags:
                                ping_frame = Frame(0, 0x06)
                                ping_frame.flags.add("ACK")
                                ping_frame.opaque_data = frame.opaque_data
                                frames = [ping_frame]

                        elif frame.type == 0x08:
                            # WINDOW UPDATE

                            frames = []
                            increment = frame.window_increment
                            if frame.stream_id:
                                try:
                                    event = WindowUpdated()
                                    event.stream_id = connection.stream.stream_id

                                    # If we encounter a problem with incrementing the flow control window,
                                    # this should be treated as a *stream* error, not a *connection* error.
                                    # That means we need to catch the error and forcibly close the stream.
                                    event.delta = increment

                                    try:
                                        connection.stream.current_outbound_window_size = (
                                            self._guard_increment_window(
                                                connection.stream.current_outbound_window_size,
                                                increment
                                            )
                                        )
                                    except StreamError:
                                        # Ok, this is bad. We're going to need to perform a local
                                        # reset.

                                        event = StreamReset()
                                        event.stream_id = connection.stream.stream_id
                                        event.error_code = ErrorCodes.FLOW_CONTROL_ERROR
                                        event.remote_reset = False

                                        self.closed_by = ErrorCodes.FLOW_CONTROL_ERROR

                                        status_code = 400
                                        error = Exception(
                                            f"Connection - {connection.stream.stream_id} err: {str(event)}"
                                        )
                                        done = True

                                except Exception:
                                    frames = []

                            else:
                                self.outbound_flow_control_window = (
                                    self._guard_increment_window(
                                        self.outbound_flow_control_window,
                                        increment,
                                    )
                                )
                                # FIXME: Should we split this into one event per active stream?
                                window_updated_event = WindowUpdated()
                                window_updated_event.stream_id = 0
                                window_updated_event.delta = increment

                                frames = []

                    except Exception as e:
                        status_code = status_code or 400
                        error = Exception(
                            f"Connection - {connection.stream.stream_id} err- {str(e)}"
                        )
                        done = True

                    if frames:
                        # Each reply is written once.
                        for f in frames:
                            connection.write(f.serialize())

                        frames = None

            finally:
                # The frames handled, or failed on, are consumed; once all of
                # them are, the transport writes from the buffer's start.
                if offset == buffer_length:
                    reader._start = reader._end = parsed_unparsed = 0

                else:
                    reader._start = offset
                    parsed_unparsed = buffer_length - offset

            if reader._reading_paused:
                # The buffer filled: parsed, it has room again.
                reader._reading_paused = False
                reader._transport.resume_reading()

            if done:
                break

            if (
                until_window_open
                and response_started is False
                and stream.current_outbound_window_size > 0
                and self.outbound_flow_control_window > 0
            ):
                # Nothing of the response yet, and the body may flow again.
                return None

        body = b"".join(body_chunks)

        if (
            error is None
            and header_section_done
            and head_request is False
            and status_code != 204
            and status_code != 304
            and (content_length := headers_dict.get("content-length")) is not None
        ):
            # A response with content: its content-length equals its DATA's
            # total (RFC 9113 8.1.1). HEAD's, 204's and 304's carry none, and
            # may give any length (RFC 9110 6.4.1). A list of one repeated
            # value stands for that value (RFC 9110 8.6).
            if not content_length.isdigit():
                members = {member.strip() for member in content_length.split(",")}
                content_length = members.pop() if len(members) == 1 else ""

            if not content_length.isdigit() or int(content_length) != len(body):
                if stream.writer is not None:
                    connection.write(
                        _FRAME_WITH_UINT32.pack(0, 4, 0x03, 0, stream_id, ErrorCodes.PROTOCOL_ERROR)
                    )

                status_code = 400
                error = Exception(
                    f"Connection - {stream_id} err: malformed response: content-length "
                    f"{headers_dict['content-length']} for {len(body)} bytes of DATA"
                )

        return (status_code, headers_dict, body, error, trailers_dict)

    def cancel_stream(self, connection: HTTP2Connection):
        """
        End the request's stream with RST_STREAM(CANCEL) (RFC 9113 6.4, 7) when
        its response is abandoned, so the connection can carry the next request.
        Frames that still arrive for it are discarded (see ``receive_response``).
        """
        if connection.stream.writer is not None:
            connection.write(
                _FRAME_WITH_UINT32.pack(
                    0, 4, 0x03, 0, connection.stream.stream_id, ErrorCodes.CANCEL
                )
            )

    async def submit_request_body(self, data: bytes, connection: HTTP2Connection):
        stream = connection.stream
        stream_id = stream.stream_id
        body = memoryview(data)
        remaining = len(body)
        offset = 0

        while remaining:
            # A DATA frame may exceed neither flow control window nor the
            # server's frame size limit (RFC 9113 6.9.1, 4.2).
            flow = stream.current_outbound_window_size
            if (connection_window := self.outbound_flow_control_window) < flow:
                flow = connection_window

            if (max_frame_size := stream.max_outbound_frame_size) < flow:
                flow = max_frame_size

            if flow <= 0:
                # Blocked: process what the server sends until it opens a
                # window, or until it answers without taking the whole body.
                if (
                    early_response := await self.receive_response(
                        connection, until_window_open=True
                    )
                ) is not None:
                    if early_response[3] is None:
                        # The response completed, so abandon the rest of the
                        # body. A server that already reset the stream ignores
                        # this (RFC 9113 5.1).
                        connection.write(
                            _FRAME_WITH_UINT32.pack(0, 4, 0x03, 0, stream_id, ErrorCodes.CANCEL)
                        )

                    self._early_response = early_response
                    return connection

                continue

            chunk_size = flow if flow < remaining else remaining
            remaining -= chunk_size
            stream.current_outbound_window_size -= chunk_size
            self.outbound_flow_control_window -= chunk_size

            # END_STREAM (0x1) rides on the last DATA frame.
            connection.write(
                _STRUCT_HBBBL.pack(
                    chunk_size >> 8,
                    chunk_size & 0xFF,
                    0x0,
                    0x0 if remaining else 0x1,
                    stream_id,
                )
                + body[offset : offset + chunk_size]
            )
            offset += chunk_size

        if offset == 0:
            # An empty body still has to end the stream.
            connection.write(_STRUCT_HBBBL.pack(0, 0, 0x0, 0x1, stream_id))

        return connection
