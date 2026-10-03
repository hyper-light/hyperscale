from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Dict, Optional, Tuple, Literal

from hyperscale.core.engines.client.shared.protocols import (
    Reader,
    Writer,
)
from .tcp import MAXLINE

from .tcp import TCPConnection


class FTPConnection:
    __slots__ = (
        "address_info",
        "port",
        "ssl",
        "ip_addr",
        "lock",
        "reader",
        "writer",
        "connected",
        "reset_connections",
        "pending",
        "_connection_factory",
        "_reader_and_writer",
        "reset_connection",
        "server_name",
        "socket_family",
        "host",
        "_current_auth",
        "_secure_locations",
        "login_lock",
        "secure_lock",
        "session_open",
        "logged_in",
        "data_connection",
        "transfer_type",
    )

    def __init__(self, reset_connections: bool = False) -> None:
        self.address_info: tuple[
            int,
            int,
            int,
            str,
            tuple[str, int] | tuple[str, int, int , int]
        ] = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.reader: Reader = None
        self.writer: Writer = None

        self._reader_and_writer: Dict[str, Tuple[Reader, Writer]] = {}

        self.connected = False
        self.reset_connection = reset_connections
        self.pending = 0
        self._connection_factory = TCPConnection()
        self.server_name: str | None = None
        self.socket_family: int = None
        self.host: str | None = None
        self._current_auth: tuple[
            str, 
            str, 
            str,
        ] = ('', '', '')

        self._secure_locations: dict[str, bool] = {}
        self.login_lock = asyncio.Lock()
        self.secure_lock = asyncio.Lock()
        # The open control session: its greeting was read, and whether it
        # logged in (as ``_current_auth``).
        self.session_open = False
        self.logged_in = False
        # A control connection's paired data connection (as an HTTP2
        # connection is paired with its pipe), and its session's TYPE.
        self.data_connection: FTPConnection | None = None
        self.transfer_type: str | None = None

    def check_logged_in(
        self,
        auth: tuple[str, str, str] | None,
    ):
        return self.logged_in and self._current_auth == auth

    def mark_logged_in(
        self,
        auth: tuple[str, str, str] | None,
    ):
        self._current_auth = auth
        self.logged_in = True
    
    def check_is_secure(self, url: str):
        return self._secure_locations.get(url) is not None

    def mark_secure(self, url: str):
        self._secure_locations[url] = True
    
    async def make_connection(
        self,
        hostname: str,
        address_info: str,
        port: int,
        ssl: Optional[SSLContext] = None,
        timeout: int | None = None,
    ) -> None:
        if self._reader_and_writer.get(hostname) is None:
            (
                reader, 
                writer,
                port
            ) = await self._connection_factory.create(
                hostname,
                address_info, 
                ssl=ssl,
                timeout=timeout,
            )

            self.reader = reader
            self.writer = writer

            self._reader_and_writer[hostname] = (reader, writer)

            self.address_info = address_info
            self.port = port
            self.ssl = ssl
        else:
            reader, writer = self._reader_and_writer.get(hostname)

            self.reader = reader
            self.writer = writer

        return port

    @property
    def empty(self):
        return not self.reader._buffer

    def read(self, num_bytes: int = MAXLINE):
        return self.reader.read(n=num_bytes)

    def readexactly(self, n_bytes: int):
        return self.reader.readexactly(n=n_bytes)

    def readuntil(self, sep=b"\n"):
        return self.reader.readuntil(separator=sep)

    def readline(
        self, 
        num_bytes: int | None = None,
        exit_on_eof: bool = False,
    ):
        return self.reader.readline(
            num_bytes=num_bytes,
            exit_on_eof=exit_on_eof,
        )

    def write(self, data):
        self.writer.write(data)

    def reset_buffer(self):
        self.reader._buffer = bytearray()

    def read_headers(self):
        return self.reader.read_headers()

    def _clear_session(self):
        self._reader_and_writer.clear()
        self.reader = None
        self.writer = None
        self.session_open = False
        self.logged_in = False
        self._current_auth = ('', '', '')
        self._secure_locations.clear()
        self.transfer_type = None

    def close(self):
        self._clear_session()
        self._connection_factory.close()

        if self.data_connection is not None:
            self.data_connection.close()

    def reset(self):
        self._clear_session()
        self._connection_factory.reset()

        if self.data_connection is not None:
            self.data_connection.reset()
