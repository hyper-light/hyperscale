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

# HEADERS frame flags (RFC 9113 6.2).
_END_STREAM = 0x01
_END_HEADERS = 0x04


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
        if self._init_sent is False:
            window_increment = 65536

            self._inbound_flow_control_window_manager.window_opened(window_increment)

            settings_frame = Frame(0, 0x04)
            for setting, value in self.local_settings.items():
                settings_frame.settings[setting] = value

            # The WINDOW_UPDATE grants the window_increment recorded above, so
            # the connection window the server sees matches ours.
            connection.write(
                b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                + settings_frame.serialize()
                + _FRAME_WITH_UINT32.pack(0, 4, 0x08, 0, 0, window_increment)
            )
            self._init_sent = True

            self.outbound_flow_control_window = self.remote_settings.initial_window_size

        return connection

    def send_request_headers(
        self,
        headers: List[Tuple[bytes, bytes]],
        data: Optional[bytes],
        connection: HTTP2Connection,
    ):
        connection.stream.inbound = WindowManager(
            self.local_settings.initial_window_size
        )
        connection.stream.outbound = WindowManager(
            self.remote_settings.initial_window_size
        )

        connection.stream.max_inbound_frame_size = self.local_settings.max_frame_size
        connection.stream.max_outbound_frame_size = self.remote_settings.max_frame_size
        connection.stream.current_outbound_window_size = (
            self.remote_settings.initial_window_size
        )

        stream_id = connection.stream.stream_id

        connection.stream.inbound.window_opened(65536)

        # The HEADERS frame is packed directly, byte for byte what
        # Frame.serialize() builds: END_HEADERS, plus END_STREAM when no body
        # follows. The WINDOW_UPDATE grants the 65,536 bytes just recorded,
        # so the stream window the server sees matches ours.
        header_block_length = len(headers)
        connection.write(
            _STRUCT_HBBBL.pack(
                (header_block_length >> 8) & 0xFFFF,
                header_block_length & 0xFF,
                0x01,
                _END_HEADERS if data is not None else _END_HEADERS | _END_STREAM,
                stream_id & 0x7FFFFFFF,
            )
            + headers
            + _FRAME_WITH_UINT32.pack(0, 4, 0x08, 0, stream_id, 65536)
        )

        return connection

    async def receive_response(
        self,
        connection: HTTP2Connection,
        until_window_open: bool = False,
    ):
        if (early_response := self._early_response) is not None:
            # submit_request_body already read this response: the server sent
            # it before taking the whole request body.
            self._early_response = None
            return early_response

        body_data = bytearray()
        status_code: Optional[int] = 200
        headers_dict: Dict[bytes, bytes] = {}
        error: Optional[Exception] = None

        stream = connection.stream
        stream_id = stream.stream_id
        frame_buffer = stream.frame_buffer
        connection_window = self._inbound_flow_control_window_manager
        stream_window = stream.inbound
        response_started = False

        done = False
        while done is False:
            data = b""

            try:
                data = await connection.read(limit=65536 * 1024)

            except Exception as err:
                return (400, headers_dict, body_data, err)

            if data == b"":
                # HTTP/2 ends a response with END_STREAM, never by closing the
                # connection, so this response is incomplete.
                error = Exception(
                    f"Connection - {stream_id} err: connection closed before the stream ended"
                )
                done = True

            frame_buffer.data.extend(data)
            frame_buffer.max_frame_size = stream.max_outbound_frame_size

            frames = None

            for frame in frame_buffer:
                if frame.type == 0x0:
                    # DATA, inlined: this is the hot path.
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

                    response_started = True
                    stream_window.window_consumed(flow_controlled_length)
                    body_data.extend(frame.data)

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

                        for k, v in headers:
                            if k == ":status":
                                status_code = int(v)
                            elif k.startswith(":"):
                                headers_dict[k.strip(":")] = v
                            else:
                                headers_dict[k] = v

                        if "END_STREAM" in frame.flags:
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
                            if SettingCodes.INITIAL_WINDOW_SIZE in changes:
                                setting = changes[SettingCodes.INITIAL_WINDOW_SIZE]
                                delta = setting.new_value - (setting.original_Value or 0)

                                new_max_size = connection.stream.inbound.max_window_size + delta
                                connection.stream.inbound.window_opened(delta)
                                connection.stream.inbound.max_window_size = new_max_size
                   

                            if SettingCodes.MAX_HEADER_LIST_SIZE in changes:
                                setting = changes[SettingCodes.MAX_HEADER_LIST_SIZE]
                                self._decoder.max_header_list_size = setting.new_value

                            if SettingCodes.MAX_FRAME_SIZE in changes:
                                setting = changes[SettingCodes.MAX_FRAME_SIZE]
                                self.max_inbound_frame_size = setting.new_value

                            if SettingCodes.HEADER_TABLE_SIZE in changes:
                                setting = changes[SettingCodes.HEADER_TABLE_SIZE]
                                # This is safe across all hpack versions: some versions just won't
                                # respect it.
                                self._decoder.max_allowed_table_size = setting.new_value



                        else:
                            self.remote_settings.update(frame.settings)

                            changes = self.remote_settings.acknowledge()

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

        return (status_code, headers_dict, bytes(body_data), error)

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
