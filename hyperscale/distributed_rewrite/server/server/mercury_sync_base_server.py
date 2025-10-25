


import asyncio
import inspect
import orjson
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
    Awaitable,
    TypeVar,
    get_type_hints,
)

import msgspec
import zstandard

from hyperscale.core.engines.client.udp.protocols.dtls import do_patch
from hyperscale.distributed_rewrite.env import Env, TimeParser
from hyperscale.distributed_rewrite.encryption import AESGCMFernet
from hyperscale.distributed_rewrite.server.protocol import (
    MercurySyncTCPProtocol,
    MercurySyncUDPProtocol,
)
from hyperscale.distributed_rewrite.server.events import LamportClock
from hyperscale.distributed_rewrite.server.hooks.tcp import (
    TCPClientCall,
    TCPServerCall,
)
from hyperscale.distributed_rewrite.server.hooks.udp import (
    UDPClientCall,
    UDPServerCall,
)


do_patch()

D = TypeVar("D", bound=msgspec.Struct)
R = TypeVar("R", bound=msgspec.Struct)


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

        self._host = host
        self._udp_host = host
        self._tcp_port = tcp_port
        self._udp_port = udp_port

        self._encoded_host = host.encode()
        self._encoded_tcp_port = str(tcp_port).encode()
        self._encoded_udp_port = str(udp_port).encode()

        self._tcp_addr_slug = self._encoded_host + b':' + self._encoded_tcp_port
        self._udp_addr_slug = self._encoded_host + b':' + self._encoded_udp_port

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

        self._udp_transport: asyncio.DatagramTransport = None
        self._tcp_transport: asyncio.Transport = None

        self._tcp_client_data: dict[
            bytes,
            dict[bytes, asyncio.Queue[bytes]]
        ] = defaultdict(lambda: defaultdict(asyncio.Queue))

        self._udp_client_data: dict[
            bytes,
            dict[bytes, asyncio.Queue[bytes]]
        ] = defaultdict(lambda: defaultdict(asyncio.Queue))

        self._pending_tcp_server_responses: Deque[asyncio.Task] = deque()
        self._pending_udp_server_responses: Deque[asyncio.Task] = deque()

        self._tcp_server_socket: socket.socket | None = None
        self._udp_server_socket: socket.socket | None = None

        self._client_key_path: str | None = None
        self._client_cert_path: str | None = None

        self._server_key_path: str | None = None
        self._server_cert_path: str | None = None

        self._client_tcp_ssl_context: Union[ssl.SSLContext, None] = None
        self._server_tcp_ssl_context: Union[ssl.SSLContext, None] = None

        self._udp_ssl_context: Union[ssl.SSLContext, None] = None

        self._encryptor = AESGCMFernet(env)
        
        self._tcp_semaphore: asyncio.Semaphore | None= None
        self._udp_semaphore: asyncio.Semaphore | None= None

        self._compressor: zstandard.ZstdCompressor | None = None
        self._decompressor: zstandard.ZstdDecompressor| None = None

        self._tcp_server_cleanup_task: asyncio.Task | None = None
        self._tcp_server_sleep_task: asyncio.Task | None = None

        self._udp_server_cleanup_task: asyncio.Task | None = None
        self._udp_server_sleep_task: asyncio.Task | None = None

        self.tcp_client_waiting_for_data: asyncio.Event = None
        self.tcp_server_waiting_for_data: asyncio.Event = None
        self.udp_client_waiting_for_data: asyncio.Event = None
        self.udp_server_waiting_for_data: asyncio.Event = None

        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._udp_connect_retires = env.MERCURY_SYNC_UDP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT

        self._model_handler_map: dict[bytes, bytes] = {}
        self.tcp_client_response_models: dict[bytes, type[msgspec.Struct]] = {}
        self.tcp_server_request_models: dict[bytes, type[msgspec.Struct]] = {}
        self.udp_client_response_models: dict[bytes, type[msgspec.Struct]] = {}
        self.udp_server_request_models: dict[bytes, type[msgspec.Struct]] = {}

        self.tcp_handlers: dict[
            bytes,
            Callable[
                [bytes],
                Awaitable[msgspec.Struct],
            ]
        ] = {}
        
        self.udp_handlers: dict[
            bytes,
            Callable[
                [bytes],
                Awaitable[msgspec.Struct],
            ]
        ] = {}

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
        
        if self._client_cert_path is None:
            self._client_cert_path = cert_path
        
        if self._client_key_path is None:
            self._client_key_path = key_path
        
        if self._server_cert_path is None:
            self._server_cert_path = cert_path

        if self._server_key_path is None:
            self._server_key_path = key_path

        try:
            self._loop = asyncio.get_event_loop()

        except Exception:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        self._tcp_semaphore = asyncio.Semaphore(self._max_concurrency)
        self._udp_semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        self.tcp_client_waiting_for_data = asyncio.Event()
        self.tcp_server_waiting_for_data = asyncio.Event()
        self.udp_client_waiting_for_data = asyncio.Event()
        self.udp_server_waiting_for_data = asyncio.Event()

        self._get_tcp_hooks()
        self._get_udp_hooks()

        await self._start_udp_server(
            worker_socket=udp_server_worker_socket,
            worker_transport=udp_server_worker_transport,
        )
        
        await self._start_tcp_server(
            worker_socket=tcp_server_worker_socket,
            worker_server=tcp_server_worker_server,
        )

        if self._tcp_server_cleanup_task is None:
            self._tcp_server_cleanup_task = self._loop.create_task(self._cleanup_tcp_server_tasks())
                                                                   
        if self._udp_server_cleanup_task is None:
            self._udp_server_cleanup_task = self._loop.create_task(self._cleanup_udp_server_tasks())

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
                self._tcp_server_socket.bind((self._host, self._tcp_port))

            except Exception:
                pass

            self._tcp_server_socket.setblocking(False)

        elif self._tcp_connected is False and worker_socket:
            self._tcp_server_socket = worker_socket
            host, port = worker_socket.getsockname()

            self._host = host
            self._tcp_port = port
            
            self._tcp_connected = True

        elif self._tcp_connected is False and worker_server:
            self._tcp_server = worker_server

            server_socket, _ = worker_server.sockets
            host, port = server_socket.getsockname()
            self._host = host
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
    
    def _get_tcp_hooks(self):
        hooks: Dict[str, TCPClientCall | TCPServerCall] = {
            name: hook()
            for name, hook in inspect.getmembers(
                self,
                predicate=lambda member: (
                    hasattr(member, 'is_hook')
                    and hasattr(member, 'type')
                    and getattr(member, 'type') == 'tcp'
                )
            )
        }

        for hook in hooks.values():
            hook.call = hook.call.__get__(self, self.__class__)
            setattr(self, hook.name, hook.call)

            signature = inspect.signature(hook.call)

            for param in signature.parameters.values():
                encoded_hook_name = hook.name.encode()

                if param.annotation in msgspec.Struct.__subclasses__():
                    self.tcp_server_request_models[encoded_hook_name] = param.annotation
                    request_model_name = param.annotation.__name__.encode()

                    self._model_handler_map[request_model_name] = encoded_hook_name

            return_type = get_type_hints(hook.call).get("return")
            self.tcp_client_response_models[encoded_hook_name] = return_type

            if isinstance(hook, TCPServerCall):
                self.tcp_handlers[encoded_hook_name] = hook.call
    
    def _get_udp_hooks(self):
        hooks: Dict[str, UDPClientCall | UDPServerCall] = {
            name: hook()
            for name, hook in inspect.getmembers(
                self,
                predicate=lambda member: (
                    hasattr(member, 'is_hook')
                    and hasattr(member, 'type')
                    and getattr(member, 'type') == 'udp'
                )
            )
        }

        for hook in hooks.values():
            hook.call = hook.call.__get__(self, self.__class__)
            setattr(self, hook.name, hook.call)

            signature = inspect.signature(hook.call)

            for param in signature.parameters.values():
                encoded_hook_name = hook.name.encode()

                if param.annotation in msgspec.Struct.__subclasses__():
                    self.udp_server_request_models[encoded_hook_name] = param.annotation
                    request_model_name = param.annotation.__name__.encode()

                    self._model_handler_map[request_model_name] = encoded_hook_name

            return_type = get_type_hints(hook.call).get("return")
            self.udp_client_response_models[encoded_hook_name] = return_type

            if isinstance(hook, UDPServerCall):
                self.udp_handlers[encoded_hook_name] = hook.call
    
    async def _connect_tcp_client(
        self,
        address: Tuple[str, int],
        worker_socket: Optional[socket.socket] = None,
    ) -> None:

        if self._client_cert_path and self._client_key_path:
            self._client_tcp_ssl_context = self._create_tcp_client_ssl_context()

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
                    ssl=self._client_tcp_ssl_context,
                )

                self._tcp_client_transports[address] = client_transport

                return client_transport

            except ConnectionRefusedError as connection_error:
                last_error = connection_error

            await asyncio.sleep(1)

        if last_error:
            raise last_error
        
    def _create_tcp_client_ssl_context(self) -> ssl.SSLContext:

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.load_cert_chain(self._client_cert_path, keyfile=self._client_key_path)
        ssl_ctx.load_verify_locations(cafile=self._client_cert_path)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED


        match self._verify_cert:
            case "REQUIRED":
                ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED

            case "OPTIONAL":
                ssl_ctx.verify_mode = ssl.VerifyMode.CERT_OPTIONAL

            case _:
                ssl_ctx.verify_mode = ssl.VerifyMode.CERT_NONE

        ssl_ctx.set_ciphers("ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384")

        return ssl_ctx
    
    async def send_tcp_client_data(
        self,
        address: tuple[str, int],
        data: D,
        timeout: int | float | None = None,
    ) -> R:
        try:

        
            if timeout is None:
                timeout = self._request_timeout
            
            async with self._tcp_semaphore:        
                transport = self._tcp_client_transports.get(address)
                if transport is None or transport.is_closing():
                    transport = await self._connect_tcp_client(address)

                payload = orjson.dumps({
                    key: value for key, value in msgspec.structs.asdict(data).items() if value is not None
                })

                handler_name = data.__class__.__name__.encode()

                transport.write(
                    self._encryptor.encrypt(
                        self._compressor.compress(handler_name + b'<|/|' + payload)
                    ),
                )

                response_bytes = await asyncio.wait_for(
                    self._tcp_client_data[self._tcp_addr_slug][handler_name].get(),
                    timeout=timeout,
                )

                response_data = orjson.loads(response_bytes)
                response_model = self.tcp_client_response_models[handler_name]

                return response_model(**response_data)
            
        except Exception:
            pass

    async def send_udp_client_data(
        self,
        address: tuple[str, int],
        data: D,
        timeout: int | float | None = None,
    ) -> bytes:
        try:

            if timeout is None:
                timeout = self._request_timeout
            
            async with self._udp_semaphore:

                payload = orjson.dumps({
                    key: value for key, value in msgspec.structs.asdict(data).items() if value is not None
                })

                model_name = data.__class__.__name__.encode()
                handler_name = self._model_handler_map[model_name]

                self._udp_transport.sendto(
                    self._encryptor.encrypt(
                        self._compressor.compress(b'c' + b'<|/|' + handler_name + b'<|/|' + self._udp_addr_slug + b'<|/|' + payload)
                    ),
                    address,
                )

                host, port = address

                target = f'{host}:{port}'.encode()

                response_bytes = await asyncio.wait_for(
                    self._udp_client_data[target][handler_name].get(),
                    timeout=timeout,
                )

                response_data = orjson.loads(response_bytes)
                response_model = self.udp_client_response_models[handler_name]
                
                return response_model(**response_data)
            
        except Exception:
            pass
    
    def read_client_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        decrypted = self._decompressor.decompress(
            self._encryptor.decrypt(
               data,
            )
        )

        handler_name, addr, payload = decrypted.split(b'<|/|', maxsplit=2)
        self._tcp_client_data[addr][handler_name].put_nowait(payload)

    def read_server_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        self._pending_tcp_server_responses.append(
            asyncio.ensure_future(
                self.process_tcp_server_call(
                    data,
                    transport,
                ),
            ),
        )

    def read_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        decrypted = self._decompressor.decompress(
            self._encryptor.decrypt(data),
        )

        request_type, handler_name, addr, payload = decrypted.split(b'<|/|', maxsplit=3)

        match request_type:
            case b'c':
                self._pending_udp_server_responses.append(
                    asyncio.ensure_future(
                        self.process_udp_call(
                            handler_name,
                            addr,
                            payload,
                            transport,
                        ),
                    ),
                )

            case b's':
                self._udp_client_data[addr][handler_name].put_nowait(payload)


    async def process_tcp_server_call(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        try:
            decrypted = self._decompressor.decompress(
                self._encryptor.decrypt(
                    data,
                )
            )

            handler_name, _, payload = decrypted.split(b'<|/|', maxsplit=2)

            handler = self.tcp_handlers[handler_name]
            request_model = self.tcp_server_request_models[handler_name]

            response = await handler(request_model(**orjson.loads(payload)))

            response_payload = self._encryptor.encrypt(
                self._compressor.compress(handler_name + b'<|/|' + self._tcp_addr_slug + b'<|/|' + orjson.dumps({
                    key: value for key, value in msgspec.structs.asdict(response).items() if value is not None
                }),)
            )

            transport.write(response_payload)

        except Exception:
            pass

    async def process_udp_call(
        self,
        handler_name: bytes,
        addr: bytes,
        payload: bytes,
        transport: asyncio.DatagramTransport,
    ):
        
        handler = self.udp_handlers[handler_name]
        request_model = self.udp_server_request_models[handler_name]

        response = await handler(request_model(**orjson.loads(payload)))

        response_payload = self._encryptor.encrypt(
            self._compressor.compress(b's' + b'<|/|' + handler_name + b'<|/|' + self._udp_addr_slug + b'<|/|' + orjson.dumps({
                key: value for key, value in msgspec.structs.asdict(response).items() if value is not None
            }),)
        )

        host, port = addr.decode().split(':', maxsplit=1)

        transport.sendto(response_payload, (host, int(port)))

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


    async def _cleanup_udp_server_tasks(self):
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

    async def shutdown(self) -> None:
        self._running = False

        for client in self._tcp_client_transports.values():
            client.abort()

        for client in self._udp_client_transports.values():
            client.abort()

        await asyncio.gather(*[
            self._cleanup_tcp_server_tasks(),
            self._cleanup_udp_server_tasks(),
        ])

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