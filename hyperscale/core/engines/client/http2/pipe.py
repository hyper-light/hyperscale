from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)

from .config import H2Configuration
from .errors import (
    ErrorCodes,
    StreamClosedError,
    StreamError,
)
from .events import (
    ConnectionTerminated,
    DataReceived,
    StreamReset,
    WindowUpdated,
    PingReceived,
    PingAckReceived,
    SettingsAcknowledged,
    RemoteSettingsChanged,
)
from .fast_hpack import Decoder, Encoder
from .frames.types.base_frame import Frame
from .protocols import HTTP2Connection
from .settings import SettingCodes, Settings, StreamClosedBy
from .windows import WindowManager


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
    )

    CONFIG = H2Configuration(
        validate_inbound_headers=False,
    )

    def __init__(self, concurrency: int):
        self.connected = False
        self.concurrency = concurrency
        self._encoder = Encoder()
        self._decoder = Decoder()
        self._decoder.max_allowed_table_size = self._decoder.header_table.maxsize
        self._init_sent = False

        self.local_settings = Settings(
            client=True,
            initial_values={
                SettingCodes.MAX_CONCURRENT_STREAMS: concurrency,
                SettingCodes.MAX_HEADER_LIST_SIZE: 2**16,
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
            self.outbound_flow_control_window = self.remote_settings.initial_window_size

            self._inbound_flow_control_window_manager = WindowManager(
                max_window_size=self.local_settings.initial_window_size
            )

        return LARGEST_FLOW_CONTROL_WINDOW - current

    def send_preamble(self, connection: HTTP2Connection):
        if self._init_sent is False:
            window_increment = 65536

            self._inbound_flow_control_window_manager.window_opened(window_increment)

            settings_frame = Frame(0, 0x04)
            for setting, value in self.local_settings.items():
                settings_frame.settings[setting] = value

            connection.write(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + settings_frame.serialize())
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

        headers_frame = Frame(connection.stream.stream_id, 0x01)
        headers_frame.flags.add("END_HEADERS")

        headers_frame.data = headers

        if data is None:
            headers_frame.flags.add("END_STREAM")

        connection.stream.inbound.window_opened(65536)

        connection.write(headers_frame.serialize())

        return connection

    async def receive_response(self, connection: HTTP2Connection):
        body_data = bytearray()
        status_code: Optional[int] = 200
        headers_dict: Dict[bytes, bytes] = {}
        error: Optional[Exception] = None

        done = False
        while done is False:
            data = b""

            try:
                data = await connection.read(limit=65536 * 1024)

            except Exception as err:
                return (400, headers_dict, body_data, err)

            if data == b"":
                done = True

            connection.stream.frame_buffer.data.extend(data)
            connection.stream.frame_buffer.max_frame_size = (
                connection.stream.max_outbound_frame_size
            )

            write_data = bytearray()
            frames = None
            stream_events: List[Frame] = []


            for frame in connection.stream.frame_buffer:
                try:
                    if frame.type == 0x0:
                        # DATA

                        end_stream = "END_STREAM" in frame.flags
                        flow_controlled_length = frame.flow_controlled_length
                        frame_data = frame.data

                        frames = []
                        self._inbound_flow_control_window_manager.window_consumed(
                            flow_controlled_length
                        )

                        try:
                            connection.stream.inbound.window_consumed(
                                flow_controlled_length
                            )

                            event = DataReceived()
                            event.stream_id = connection.stream.stream_id

                            stream_events.append(event)

                            if end_stream:
                                done = True

                            stream_events[0].data = frame_data
                            stream_events[
                                0
                            ].flow_controlled_length = flow_controlled_length

                        except StreamClosedError as e:
                            status_code = status_code or 400
                            error = Exception(
                                f"Connection - {connection.stream.stream_id} err: {str(e._events[0])}"
                            )

                    elif frame.type == 0x07:
                        # GOAWAY

                        new_event = ConnectionTerminated()
                        new_event.error_code = ErrorCodes(frame.error_code)
                        new_event.last_stream_id = frame.last_stream_id

                        if frame.additional_data:
                            new_event.additional_data = frame.additional_data

                        frames = []
                        done = True

                    elif frame.type == 0x01:
                        # HEADERS
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

                    elif frame.type == 0x03:
                        # RESET

                        self.closed_by = StreamClosedBy.RECV_RST_STREAM
                        reset_event = StreamReset()
                        reset_event.stream_id = connection.stream.stream_id

                        reset_event.error_code = ErrorCodes(frame.error_code)

                        status_code = 400
                        error = Exception(
                            f"Connection - {connection.stream.stream_id} - err: {str(reset_event)}"
                        )

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


                            ack_event = SettingsAcknowledged()
                            ack_event.changed_settings = changes
                            stream_events.append(ack_event)

                        else:
                            self.remote_settings.update(frame.settings)
                            stream_events.append(
                                RemoteSettingsChanged(
                                    self.remote_settings, 
                                    frame.settings,
                                ),
                            )

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
                        # PING
                        
                        if "ACK" in frame.flags:
                            event = PingAckReceived()

                        else:
                            event = PingReceived()
                            ping_frame = Frame(0, 0x06)
                            ping_frame.flags.add("ACK")
                            ping_frame.opaque_data = frame.opaque_data
                            frames.append(ping_frame)

                        event.data = frame.opaque_data
                        stream_events.append(event)


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

                if frames:
                    for f in frames:
                        write_data.extend(f.serialize())

                    connection.write(write_data)

            for event in stream_events:
                amount = event.flow_controlled_length

                conn_increment = (
                    self._inbound_flow_control_window_manager.process_bytes(amount)
                )

                if conn_increment:
                    connection.stream.write_window_update_frame(
                        stream_id=0, window_increment=conn_increment
                    )

                if event.data is None:
                    event.data = b""

                body_data.extend(event.data)

            if done:
                break

        return (status_code, headers_dict, bytes(body_data), error)

    async def submit_request_body(self, data: bytes, connection: HTTP2Connection):
        while data:
            local_flow = connection.stream.current_outbound_window_size
            max_frame_size = self.max_outbound_frame_size
            flow = min(local_flow, max_frame_size)
            while flow == 0:
                await self.receive_response(connection)

                local_flow = connection.stream.current_outbound_window_size
                max_frame_size = self.max_outbound_frame_size
                flow = min(local_flow, max_frame_size)

            max_flow = flow
            chunk_size = min(len(data), max_flow)
            chunk, data = data[:chunk_size], data[chunk_size:]

            df = Frame(connection.stream.stream_id, 0x0)
            df.data = chunk

            # Subtract flow_controlled_length to account for possible padding
            self.outbound_flow_control_window -= df.flow_controlled_length
            assert self.outbound_flow_control_window >= 0

            connection.write(df.serialize())

        df = Frame(connection.stream.stream_id, 0x0)
        df.flags.add("END_STREAM")

        connection.write(df.serialize())

        return connection
