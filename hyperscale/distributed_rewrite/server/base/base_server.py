


import asyncio
import socket
import ssl
from collections import defaultdict, deque
from typing import (
    Any,
    Coroutine,
    Deque,
    Dict,
    Optional,
    Tuple,
    Union,
    Callable,
)

import msgspec
import zstandard

from hyperscale.distributed_rewrite.env import Env, TimeParser
from mkfst.snowflake.snowflake_generator import SnowflakeGenerator
from hyperscale.distributed_rewrite.encryption import AESGCMFernet
from hyperscale.distributed_rewrite.server.protocol import MercurySyncTCPServerProtocol


class MercurySyncTCPConnection:
    def __init__(self, host: str, port: int, instance_id: int, env: Env) -> None:
        self.id_generator = SnowflakeGenerator(instance_id)
        self.env = env

        self.host = host
        self.port = port

        self.events: Dict[str, Coroutine] = {}

        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]]] = defaultdict(deque)
        self.connected = False
        self._running = False

        self._client_transports: Dict[str, asyncio.Transport] = {}
        self._server: asyncio.Server = None
        self._upgrade_server: asyncio.Server | None = None
        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses: Deque[asyncio.Task] = deque()
        self._last_call: Deque[str] = deque()

        self._sent_values = deque()
        self.server_socket: socket.socket | None = None
        self.upgrade_socket: socket.socket | None = None
        self._stream = False

        self._client_key_path: Union[str, None] = None
        self._client_cert_path: Union[str, None] = None

        self._server_key_path: Union[str, None] = None
        self._server_cert_path: Union[str, None] = None

        self._client_ssl_context: Union[ssl.SSLContext, None] = None
        self._server_ssl_context: Union[ssl.SSLContext, None] = None

        self._encryptor = AESGCMFernet(env)
        self._semaphore: Union[asyncio.Semaphore, None] = None
        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None
        self._cleanup_task: Union[asyncio.Task, None] = None
        self._sleep_task: Union[asyncio.Task, None] = None
        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT

    def from_env(self, env: Env):
        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT

    async def connect_async(
        self,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        worker_socket: Optional[socket.socket] = None,
        upgrade_socket: Optional[socket.socket] = None,
        worker_server: Optional[asyncio.Server] = None,
    ):
        try:
            self._loop = asyncio.get_event_loop()

        except Exception:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        self._running = True
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        if cert_path and key_path:
            self._server_ssl_context = self._create_server_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
            )

        if self.connected is False and worker_socket is None:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.server_socket.bind((self.host, self.port))

            except Exception:
                pass

            self.server_socket.setblocking(False)

        elif self.connected is False and worker_socket:
            self.server_socket = worker_socket
            host, port = worker_socket.getsockname()

            self.host = host
            self.port = port

        elif self.connected is False and worker_server:
            self._server = worker_server

            server_socket, _ = worker_server.sockets
            host, port = server_socket.getsockname()
            self.host = host
            self.port = port

            self.connected = True
            self._cleanup_task = self._loop.create_task(self._cleanup())

        if self.connected is False and upgrade_socket:
            self.upgrade_socket = upgrade_socket
            host, port = upgrade_socket.getsockname()

            self.host = host
            self.port = port

        if self.connected is False:
            server = await self._loop.create_server(
                lambda: MercurySyncTCPServerProtocol(self),
                sock=self.server_socket,
                ssl=self._server_ssl_context if self.upgrade_socket is None else None,
            )

            if self.upgrade_socket:
                upgrade_server = await self._loop.create_server(
                    lambda: MercurySyncTCPServerProtocol(self),
                    sock=self.upgrade_socket,
                    ssl=self._server_ssl_context,
                )

                self._upgrade_server = upgrade_server

            self._server = server
            self.connected = True

            self._cleanup_task = self._loop.create_task(self._cleanup())

    def _create_server_ssl_context(
        self,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
    ) -> ssl.SSLContext:
        if self._server_cert_path is None:
            self._server_cert_path = cert_path

        if self._server_key_path is None:
            self._server_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.options |= ssl.OP_SINGLE_DH_USE
        ssl_ctx.options |= ssl.OP_SINGLE_ECDH_USE
        ssl_ctx.load_cert_chain(cert_path, keyfile=key_path)
        ssl_ctx.load_verify_locations(cafile=cert_path)
        ssl_ctx.check_hostname = False

        match self._verify_cert:
            case "REQUIRED":
                ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED

            case "OPTIONAL":
                ssl_ctx.verify_mode = ssl.VerifyMode.CERT_OPTIONAL

            case _:
                ssl_ctx.verify_mode = ssl.VerifyMode.CERT_NONE

        ssl_ctx.set_ciphers("ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384")

        return ssl_ctx

    def read(self, data: bytes, transport: asyncio.Transport):
        pass

    async def _cleanup(self):
        while self._running:
            self._sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            try:
                await self._sleep_task

            except (Exception, asyncio.CancelledError, KeyboardInterrupt):
                pass

            for pending in list(self._pending_responses):
                if pending.done() or pending.cancelled():
                    try:
                        await pending

                    except (Exception, socket.error):
                        pass
                    self._pending_responses.pop()

    async def close(self) -> None:
        self._stream = False
        self._running = False

        for client in self._client_transports.values():
            client.abort()

        if self._cleanup_task:
            self._cleanup_task.cancel()
            if self._cleanup_task.cancelled() is False:
                try:
                    self._sleep_task.cancel()
                    if not self._sleep_task.cancelled():
                        await self._sleep_task

                except (Exception, socket.error):
                    pass

                try:
                    await self._cleanup_task

                except Exception:
                    pass

    def abort(self) -> None:
        self._stream = False
        self._running = False

        if self._cleanup_task:
            try:
                self._sleep_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

            try:
                self._cleanup_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass