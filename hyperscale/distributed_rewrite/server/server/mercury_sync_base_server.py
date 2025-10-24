


import asyncio
import socket
import ssl
from abc import abstractmethod
from collections import defaultdict, deque
from typing import (
    Any,
    Coroutine,
    Deque,
    Dict,
    Optional,
    Tuple,
    Union,
)

import zstandard

from hyperscale.core.engines.client.udp.protocols.dtls import do_patch
from hyperscale.distributed_rewrite.env import Env, TimeParser
from hyperscale.distributed_rewrite.encryption import AESGCMFernet
from hyperscale.distributed_rewrite.server.protocol import (
    MercurySyncTCPProtocol,
    MercurySyncUDPProtocol,
)
from hyperscale.distributed_rewrite.server.events import LamportClock


do_patch()


class MercurySyncBaseServer:
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
    ) -> None:
        self._clock = LamportClock()
        self.env = env

        self._tcp_host = host
        self._udp_host = host
        self._tcp_port = tcp_port
        self._udp_port = udp_port

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._running = False

        self._tcp_events: Dict[str, Coroutine] = {}
        self._udp_events: Dict[str, Coroutine] = {}

        self._tcp_queue: Dict[str, Deque[Tuple[str, int, float, Any]]] = defaultdict(deque)
        self._udp_queue: Dict[str, Deque[Tuple[str, int, float, Any]]] = defaultdict(deque)

        self._tcp_connected = False
        self._udp_connected = False

        self._tcp_client_transports: Dict[str, asyncio.Transport] = {}
        self._udp_client_transports: Dict[str, asyncio.Transport] = {}

        self._tcp_server: asyncio.Server = None
        self._udp_server: asyncio.Server = None

        self._udp_transport: asyncio.Transport = None
        self._tcp_transport: asyncio.Transport = None

        self._tcp_waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._udp_waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)

        self._pending_tcp_client_responses: Deque[asyncio.Task] = deque()
        self._pending_udp_client_responses: Deque[asyncio.Task] = deque()

        self._pending_tcp_server_responses: Deque[asyncio.Task] = deque()
        self._pending_udp_server_responses: Deque[asyncio.Task] = deque()

        self._tcp_server_socket: socket.socket | None = None
        self._udp_server_socket: socket.socket | None = None

        self._client_key_path: Union[str, None] = None
        self._client_cert_path: Union[str, None] = None

        self._server_key_path: Union[str, None] = None
        self._server_cert_path: Union[str, None] = None

        self._client_tcp_ssl_context: Union[ssl.SSLContext, None] = None
        self._server_tcp_ssl_context: Union[ssl.SSLContext, None] = None

        self._udp_ssl_context: Union[ssl.SSLContext, None] = None

        self._encryptor = AESGCMFernet(env)
        
        self._tcp_semaphore: Union[asyncio.Semaphore, None] = None
        self._udp_semaphore: Union[asyncio.Semaphore, None] = None

        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None

        self._tcp_client_cleanup_task: Union[asyncio.Task, None] = None
        self._tcp_server_cleanup_task: Union[asyncio.Task, None] = None
        self._tcp_client_sleep_task: Union[asyncio.Task, None] = None
        self._tcp_server_sleep_task: Union[asyncio.Task, None] = None

        self._udp_client_cleanup_task: Union[asyncio.Task, None] = None
        self._udp_server_cleanup_task: Union[asyncio.Task, None] = None
        self._udp_client_sleep_task: Union[asyncio.Task, None] = None
        self._udp_server_sleep_task: Union[asyncio.Task, None] = None

        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._udp_connect_retires = env.MERCURY_SYNC_UDP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT

    def from_env(self, env: Env):
        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT

    async def start_server(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        udp_server_worker_socket: socket.socket | None = None,
        udp_server_worker_transport: asyncio.DatagramTransport | None = None,
        tcp_server_worker_socket: socket.socket | None = None,
        tcp_server_worker_server: asyncio.Server | None = None,
    ):
        
        if self._server_cert_path is None:
            self._server_cert_path = cert_path

        if self._server_key_path is None:
            self._server_key_path = key_path

        try:
            self._loop = asyncio.get_event_loop()

        except Exception:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()


        await self._start_udp_server(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=udp_server_worker_socket,
            udp_server_worker_transport=udp_server_worker_transport,
        )
        await self._start_tcp_server(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=tcp_server_worker_socket,
            worker_server=tcp_server_worker_server,
        )

        if self._tcp_client_cleanup_task is None:
            self._tcp_client_cleanup_task = self._loop.create_task(self._cleanup_tcp_client_tasks())
                                                                   
        if self._udp_client_cleanup_task is None:
            self._udp_client_cleanup_task = self._loop.create_task(self._cleanup_udp_client_tasks())

        if self._tcp_server_cleanup_task is None:
            self._tcp_server_cleanup_task = self._loop.create_task(self._cleanup_tcp_server_tasks())
                                                                   
        if self._udp_server_cleanup_task is None:
            self._udp_server_cleanup_task = self._loop.create_task(self._clanup_udp_server_tasks())

    async def _start_udp_server(
        self,
        worker_socket: socket.socket | None = None,
        worker_transport: asyncio.DatagramTransport | None = None,
    ) -> None:

        if self._udp_connected is False and worker_socket is None:
            self._udp_server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            self._udp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._udp_server_socket.bind((self._udp_host, self._udp_port))

            self._udp_server_socket.setblocking(False)

        elif self._udp_connected is False and worker_socket:
            self._udp_server_socket = worker_socket
            host, port = worker_socket.getsockname()
            self._udp_host = host
            self._udp_port = port

        elif self._udp_connected is False:
            self._udp_transport = worker_transport

            address_info: Tuple[str, int] = self._udp_transport.get_extra_info("sockname")
            self._udp_server_socket: socket.socket = self._udp_transport.get_extra_info("socket")

            host, port = address_info
            self._udp_host = host
            self._udp_port = port

            self._udp_connected = True

        if self._udp_connected is False and self._server_cert_path and self._server_key_path:
            self._udp_ssl_context = self._create_udp_ssl_context()

            self._udp_server_socket = self._udp_ssl_context.wrap_socket(self._udp_server_socket)
            self._udp_connected = True

        if self._udp_connected is False:
            server = self._loop.create_datagram_endpoint(
                lambda: MercurySyncUDPProtocol(self),
                sock=self._udp_server_socket,
            )

            transport, _ = await server

            self._udp_transport = transport
            self._udp_connected = True

    async def _start_tcp_server(
        self,
        worker_socket: socket.socket | None = None,
        worker_server: asyncio.Server | None = None,
    ):

        if self._server_cert_path and self._server_key_path:
            self._server_tcp_ssl_context = self._create_tcp_server_ssl_context()

        if self._tcp_connected is False and worker_socket is None:
            self._tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._tcp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self._tcp_server_socket.bind((self._tcp_host, self._tcp_port))

            except Exception:
                pass

            self._tcp_server_socket.setblocking(False)

        elif self._tcp_connected is False and worker_socket:
            self._tcp_server_socket = worker_socket
            host, port = worker_socket.getsockname()

            self._tcp_host = host
            self._tcp_port = port
            
            self._tcp_connected = True

        elif self._tcp_connected is False and worker_server:
            self._tcp_server = worker_server

            server_socket, _ = worker_server.sockets
            host, port = server_socket.getsockname()
            self._tcp_host = host
            self._tcp_port = port

            self._tcp_connected = True

        if self._tcp_connected is False:
            server = await self._loop.create_server(
                lambda: MercurySyncTCPProtocol(self),
                sock=self._tcp_server_socket,
                ssl=self._server_tcp_ssl_context,
            )

            self._tcp_server = server
            self._tcp_connected = True


    def _create_udp_ssl_context(self) -> ssl.SSLContext:

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.options |= ssl.OP_SINGLE_DH_USE
        ssl_ctx.options |= ssl.OP_SINGLE_ECDH_USE
        ssl_ctx.load_cert_chain(self._server_cert_path, keyfile=self._server_key_path)
        ssl_ctx.load_verify_locations(cafile=self._server_cert_path)
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
    
    def _create_tcp_server_ssl_context(self) -> ssl.SSLContext:

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.options |= ssl.OP_SINGLE_DH_USE
        ssl_ctx.options |= ssl.OP_SINGLE_ECDH_USE
        ssl_ctx.load_cert_chain(self._server_cert_path, keyfile=self._server_key_path)
        ssl_ctx.load_verify_locations(cafile=self._server_cert_path)
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
    
    async def _connect_tcp_client(
        self,
        address: Tuple[str, int],
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        worker_socket: Optional[socket.socket] = None,
    ) -> None:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._loop = asyncio.get_event_loop()
        if cert_path and key_path:
            self._client_ssl_context = self._create_tcp_client_ssl_context(
                cert_path=cert_path, key_path=key_path
            )

        if worker_socket is None:
            tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            await self._loop.run_in_executor(None, tcp_socket.connect, address)

            tcp_socket.setblocking(False)

        else:
            tcp_socket = worker_socket

        last_error: Union[Exception, None] = None

        for _ in range(self._tcp_connect_retries):
            try:
                client_transport, _ = await self._loop.create_connection(
                    lambda: MercurySyncTCPProtocol(self),
                    sock=tcp_socket,
                    ssl=self._client_ssl_context,
                )

                self._tcp_client_transports[address] = client_transport

                return client_transport

            except ConnectionRefusedError as connection_error:
                last_error = connection_error

            await asyncio.sleep(1)

        if last_error:
            raise last_error
        
    def _create_tcp_client_ssl_context(
        self,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
    ) -> ssl.SSLContext:
        if self._client_cert_path is None:
            self._client_cert_path = cert_path

        if self._client_key_path is None:
            self._client_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.load_cert_chain(cert_path, keyfile=key_path)
        ssl_ctx.load_verify_locations(cafile=cert_path)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ssl_ctx.set_ciphers("ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384")

        return ssl_ctx

    @abstractmethod
    def read_client_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass

    @abstractmethod
    def read_server_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass
    
    @abstractmethod
    def read_client_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass

    @abstractmethod
    def read_server_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass

    async def _cleanup_tcp_client_tasks(self):
        while self._running:
            self._tcp_client_sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            try:
                await self._tcp_client_sleep_task

            except (Exception, asyncio.CancelledError, KeyboardInterrupt):
                pass

            for pending in list(self._pending_tcp_client_responses):
                if pending.done() or pending.cancelled():
                    try:
                        await pending

                    except (Exception, socket.error):
                        pass
                    self._pending_tcp_client_responses.pop()


    async def _cleanup_udp_client_tasks(self):
        while self._running:
            self._udp_client_sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            try:
                await self._udp_client_sleep_task

            except (Exception, asyncio.CancelledError, KeyboardInterrupt):
                pass

            for pending in list(self._pending_udp_client_responses):
                if pending.done() or pending.cancelled():
                    try:
                        await pending

                    except (Exception, socket.error):
                        pass
                    self._pending_udp_client_responses.pop()

    async def _cleanup_tcp_server_tasks(self):
        while self._running:
            self._tcp_server_sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            try:
                await self._tcp_server_sleep_task

            except (Exception, asyncio.CancelledError, KeyboardInterrupt):
                pass

            for pending in list(self._pending_tcp_server_responses):
                if pending.done() or pending.cancelled():
                    try:
                        await pending

                    except (Exception, socket.error):
                        pass
                    self._pending_tcp_server_responses.pop()


    async def _clanup_udp_server_tasks(self):
        while self._running:
            self._udp_server_sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            try:
                await self._udp_server_sleep_task

            except (Exception, asyncio.CancelledError, KeyboardInterrupt):
                pass

            for pending in list(self._pending_udp_server_responses):
                if pending.done() or pending.cancelled():
                    try:
                        await pending

                    except (Exception, socket.error):
                        pass
                    self._pending_udp_server_responses.pop()

    async def close(self) -> None:
        self._running = False

        for client in self._tcp_client_transports.values():
            client.abort()

        for client in self._udp_client_transports.values():
            client.abort()

        await asyncio.gather(*[
            self._cleanup_tcp_client_tasks(),
            self._cleanup_udp_client_tasks(),
            self._cleanup_tcp_server_tasks(),
            self._cleanup_udp_server_tasks(),
        ])

    async def _cleanup_tcp_client_tasks(self):

        if self._tcp_client_cleanup_task:
            self._tcp_client_cleanup_task.cancel()
            if self._tcp_client_cleanup_task.cancelled() is False:
                try:
                    self._tcp_client_sleep_task.cancel()
                    if not self._tcp_client_sleep_task.cancelled():
                        await self._tcp_client_sleep_task

                except (Exception, socket.error):
                    pass

                try:
                    await self._tcp_client_cleanup_task

                except Exception:
                    pass

    async def _cleanup_udp_client_tasks(self):

        if self._udp_client_cleanup_task:
            self._udp_client_cleanup_task.cancel()
            if self._udp_client_cleanup_task.cancelled() is False:
                try:
                    self._udp_client_sleep_task.cancel()
                    if not self._udp_client_sleep_task.cancelled():
                        await self._udp_client_sleep_task

                except (Exception, socket.error):
                    pass

                try:
                    await self._udp_client_cleanup_task

                except Exception:
                    pass

    async def _cleanup_tcp_server_tasks(self):

        if self._tcp_server_cleanup_task:
            self._tcp_server_cleanup_task.cancel()
            if self._tcp_server_cleanup_task.cancelled() is False:
                try:
                    self._tcp_server_sleep_task.cancel()
                    if not self._tcp_server_sleep_task.cancelled():
                        await self._tcp_server_sleep_task

                except (Exception, socket.error):
                    pass

                try:
                    await self._tcp_server_cleanup_task

                except Exception:
                    pass

    async def _cleanup_udp_server_tasks(self):

        if self._udp_server_cleanup_task:
            self._udp_server_cleanup_task.cancel()
            if self._udp_server_cleanup_task.cancelled() is False:
                try:
                    self._udp_server_sleep_task.cancel()
                    if not self._udp_server_sleep_task.cancelled():
                        await self._udp_server_sleep_task

                except (Exception, socket.error):
                    pass

                try:
                    await self._udp_server_cleanup_task

                except Exception:
                    pass

    def abort(self) -> None:
        self._running = False

        if self._tcp_client_cleanup_task:
            try:
                self._tcp_client_sleep_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

            try:
                self._tcp_client_cleanup_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

        if self._tcp_server_cleanup_task:
            try:
                self._tcp_server_sleep_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

            try:
                self._tcp_server_cleanup_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

        if self._udp_client_cleanup_task:
            try:
                self._udp_client_sleep_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

            try:
                self._udp_client_cleanup_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

        if self._udp_server_cleanup_task:
            try:
                self._udp_server_sleep_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass

            try:
                self._udp_server_cleanup_task.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
                Exception,
                socket.error,
            ):
                pass