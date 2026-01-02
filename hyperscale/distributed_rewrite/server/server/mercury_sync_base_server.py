


import asyncio
import inspect
import secrets
import socket
import ssl
import traceback
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
    Literal,
    get_args,
    Generic,
)

import msgspec
import zstandard

from hyperscale.core.engines.client.udp.protocols.dtls import do_patch
from hyperscale.distributed_rewrite.server.context import Context, T
from hyperscale.distributed_rewrite.env import Env, TimeParser
from hyperscale.distributed_rewrite.encryption import AESGCMFernet
from hyperscale.distributed_rewrite.models import (
    Error,
    Message,
)

from hyperscale.distributed_rewrite.server.protocol import (
    MercurySyncTCPProtocol,
    MercurySyncUDPProtocol,
    ReplayGuard,
    RateLimiter,
    validate_message_size,
    parse_address,
    AddressValidationError,
    MAX_MESSAGE_SIZE,
    MAX_DECOMPRESSED_SIZE,
)
from hyperscale.distributed_rewrite.server.events import LamportClock
from hyperscale.distributed_rewrite.server.hooks.task import (
    TaskCall,
)

from hyperscale.distributed_rewrite.taskex import TaskRunner
from hyperscale.distributed_rewrite.taskex.run import Run
from hyperscale.logging import Logger

do_patch()

D = TypeVar("D", bound=msgspec.Struct)
R = TypeVar("R", bound=msgspec.Struct)

Handler = Callable[
    [
        tuple[str, int],
        bytes | msgspec.Struct,
        int,
    ],
    Awaitable[
        tuple[bytes, msgspec.Struct | bytes],
    ]
]


class MercurySyncBaseServer(Generic[T]):
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
    ) -> None:
        self._tcp_clock = LamportClock()
        self._udp_clock = LamportClock()

        self._tcp_logger: Logger | None = None
        self._udp_logger: Logger | None = None

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

        self._tcp_client_transports: Dict[tuple[str, int], asyncio.Transport] = {}
        self._udp_client_addrs: set[tuple[str, int]] = set()

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
            dict[bytes, asyncio.Queue[bytes | Message | Exception]]
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
        
        # Security utilities
        self._replay_guard = ReplayGuard()
        self._rate_limiter = RateLimiter()
        self._secure_random = secrets.SystemRandom()  # Cryptographically secure RNG
        
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

        self._context: Context[T] = None

        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._udp_connect_retires = env.MERCURY_SYNC_UDP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT

        self._model_handler_map: dict[bytes, bytes] = {}
        self.tcp_client_response_models: dict[bytes, type[Message]] = {}
        self.tcp_server_request_models: dict[bytes, type[Message]] = {}
        self.udp_client_response_models: dict[bytes, type[Message]] = {}
        self.udp_server_request_models: dict[bytes, type[Message]] = {}

        self.tcp_handlers: dict[
            bytes,
            Handler,
        ] = {}

        self.tcp_client_handler: dict[
            bytes,
            Handler,
        ] = {}

        self.udp_handlers: dict[
            bytes,
            Handler,
        ] = {}

        self.udp_client_handlers: dict[
            bytes,
            Handler,
        ] = {}

        self.task_handlers: dict[str, TaskCall] = {}

        self._tasks: dict[
            str,
            TaskCall,
        ] = {}

        self._task_runner: TaskRunner | None = None
        self._task_runs: dict[str, list[Run]] = defaultdict(list)

    @property
    def tcp_address(self):
        return self._host, self._tcp_port
    
    @property
    def udp_address(self):
        return self._host, self._udp_port
    
    @property
    def tcp_time(self):
        return self._tcp_clock.time
    
    @property
    def udp_time(self):
        return self._udp_clock.time
    
    def tcp_target_is_self(self, addr: tuple[str, int]):
        host, port = addr

        return host == self._host and port == self._tcp_port

    def udp_target_is_self(self, addr: tuple[str, int]):
        host, port = addr

        return host == self._host and port == self._udp_port

    def from_env(self, env: Env):
        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES
        self._verify_cert = env.MERCURY_SYNC_VERIFY_SSL_CERT


    async def start_server(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        init_context: T | None = None,
        udp_server_worker_socket: socket.socket | None = None,
        udp_server_worker_transport: asyncio.DatagramTransport | None = None,
        tcp_server_worker_socket: socket.socket | None = None,
        tcp_server_worker_server: asyncio.Server | None = None,
    ):
        
        if self._tcp_logger is None:
            self._tcp_logger = Logger()

        if self._udp_logger is None:
            self._udp_logger = Logger()
        
        if init_context is None:
            init_context = {}
        
        self.node_lock = asyncio.Lock()
        self._context = Context[T](init_context=init_context)
        
        if self._task_runner is None:
            self._task_runner = TaskRunner(0, self.env)

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
        self._get_task_hooks()

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

        
        for task_name, task in self._tasks.items():
            if task.trigger == 'ON_START':
                run = self._task_runner.run(
                    task.call,
                    *task.args,
                    alias=task.alias,
                    run_id=task.run_id,
                    timeout=task.timeout,
                    schedule=task.schedule,
                    trigger=task.trigger,
                    repeat=task.repeat,
                    keep=task.keep,
                    max_age=task.max_age,
                    keep_policy=task.keep_policy,
                    **task.kwargs,
                )

                self._task_runs[task_name].append(run)

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
        # Hostname verification: disabled by default for local testing,
        # set MERCURY_SYNC_TLS_VERIFY_HOSTNAME=true in production
        ssl_ctx.check_hostname = self.env.MERCURY_SYNC_TLS_VERIFY_HOSTNAME.lower() == "true"


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
        # Hostname verification: disabled by default for local testing,
        # set MERCURY_SYNC_TLS_VERIFY_HOSTNAME=true in production
        ssl_ctx.check_hostname = self.env.MERCURY_SYNC_TLS_VERIFY_HOSTNAME.lower() == "true"

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
        hooks: Dict[str, Handler] = {
            name: hook
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
            hook = hook.__get__(self, self.__class__)
            setattr(self, hook.name, hook)

            signature = inspect.signature(hook)
            encoded_hook_name = hook.name.encode()

            for param in signature.parameters.values():

                if param.annotation in msgspec.Struct.__subclasses__():
                    self.tcp_server_request_models[encoded_hook_name] = param.annotation
                    request_model_name = param.annotation.__name__.encode()

                    self._model_handler_map[request_model_name] = encoded_hook_name

            return_type = get_type_hints(hook).get("return")
            self.tcp_client_response_models[encoded_hook_name] = return_type

            if hook.action == 'receive':
                self.tcp_handlers[encoded_hook_name] = hook

            elif hook.action == 'handle':
                self.tcp_client_handler[hook.target] = hook
    
    def _get_udp_hooks(self):
        hooks: Dict[str, Handler] = {
            name: hook
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
            hook = hook.__get__(self, self.__class__)
            setattr(self, hook.name, hook)

            signature = inspect.signature(hook)
            
            encoded_hook_name = hook.name.encode()

            for param in signature.parameters.values():

                subtypes = get_args(param.annotation)
                annotation = param.annotation

                if len(subtypes) > 0:
                    for annotated in subtypes:
                        if annotated in msgspec.Struct.__subclasses__():
                            annotation = annotated
                            break

                if annotation in msgspec.Struct.__subclasses__():
                    self.udp_server_request_models[encoded_hook_name] = annotation
                    request_model_name = annotation.__name__.encode()

                    self._model_handler_map[request_model_name] = encoded_hook_name

            return_type = get_type_hints(hook).get("return")

            if return_type in msgspec.Struct.__subclasses__():
                self.udp_client_response_models[encoded_hook_name] = return_type

            if hook.action == 'receive':
                self.udp_handlers[encoded_hook_name] = hook

            elif hook.action == 'handle':
                self.udp_client_handlers[hook.target] = hook

    def _get_task_hooks(self):
        hooks: Dict[str, Handler] = {
            name: hook
            for name, hook in inspect.getmembers(
                self,
                predicate=lambda member: (
                    hasattr(member, 'is_hook')
                    and hasattr(member, 'type')
                    and getattr(member, 'type') == 'task'
                )
            )
        }

        for hook in hooks.values():
            hook = hook.__get__(self, self.__class__)
            setattr(self, hook.__name__, hook)

            if isinstance(hook, TaskCall):
                self.task_handlers[hook.__name__] = hook
    
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
        # Hostname verification: disabled by default for local testing,
        # set MERCURY_SYNC_TLS_VERIFY_HOSTNAME=true in production
        ssl_ctx.check_hostname = self.env.MERCURY_SYNC_TLS_VERIFY_HOSTNAME.lower() == "true"
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
    
    async def send_tcp(
        self,
        address: tuple[str, int],
        action: str,
        data: D,
        timeout: int | float | None = None,
    ) -> R | Error:
        try:

            if timeout is None:
                timeout = self._request_timeout
            
            async with self._tcp_semaphore:        
                transport = self._tcp_client_transports.get(address)
                if transport is None or transport.is_closing():
                    transport = await self._connect_tcp_client(address)

                clock = await self._udp_clock.increment()

                encoded_action = action.encode()
                
                if isinstance(data, Message):
                    data = data.dump()

                transport.write(
                    self._encryptor.encrypt(
                        self._compressor.compress(self._tcp_addr_slug + b'<' + encoded_action + b'<' + data  + b'<' + clock.to_bytes(64) )
                    ),
                )

                return await asyncio.wait_for(
                    self._tcp_client_data[self._tcp_addr_slug][encoded_action].get(),
                    timeout=timeout,
                )
            
        except Exception as error:
            return (
                error,
                self._tcp_clock.time,
            )

    async def broadcast_tcp(
        self,
        data: D,
        max_nodes: int | None = None,
        selection_method: Literal["all", "random", "subset"] = "subset",
        timeout: int | float | None = None,
    ):
        nodes = list(self._tcp_client_transports.keys())
        node_max = len(nodes)

        if max_nodes is None:
            max_nodes = max(1, node_max/2)
        
        match selection_method:
            case "random":
                selection = [nodes[self._secure_random.randrange(0, node_max)]]

            case "subset":
                selection = self._secure_random.choices(nodes, k=max_nodes)

            case "all":
                selection = nodes

        errors: list[Exception] = []
        results: list[R] = []
        
        async for complete in asyncio.as_completed([
            self.send_tcp(
                addr,
                data,
                timeout=timeout,
            ) for addr in selection
        ]):
            
            result = await complete
            
            if isinstance(result, Error):
                errors.append(result)

            else:
                results.append(result)

        return (
            results,
            errors,
        ) 
        
    async def send_udp(
        self,
        address: tuple[str, int],
        action: str,
        data: D,
        timeout: int | float | None = None
    ) -> R | Exception:
        try:

            if timeout is None:
                timeout = self._request_timeout
            
            async with self._udp_semaphore:

                clock = await self._udp_clock.increment()

                encoded_action = action.encode()

                if isinstance(data, Message):
                    data = data.dump()

            
                self._udp_transport.sendto(
                    self._encryptor.encrypt(
                        self._compressor.compress(
                            b'c<' + self._udp_addr_slug + b'<' + encoded_action + b'<' + data + b'<' + clock.to_bytes(64),
                        )
                    ),
                    address,
                )

                host, port = address

                target = f'{host}:{port}'.encode()

                return await asyncio.wait_for(
                    self._udp_client_data[target][encoded_action].get(),
                    timeout=timeout,
                )
            
        except Exception as error:
            return (
                error,
                self._udp_clock.time,
            )
        
    async def broadcast_udp(
        self,
        data: D,
        max_nodes: int | None = None,
        selection_method: Literal["all", "random", "subset"] = "subset",
        timeout: int | float | None = None,
    ):
        nodes = list(self._tcp_client_transports.keys())
        node_max = len(nodes)

        if max_nodes is None:
            max_nodes = max(1, node_max/2)
        
        match selection_method:
            case "random":
                selection = [nodes[self._secure_random.randrange(0, node_max)]]

            case "subset":
                selection = self._secure_random.choices(nodes, k=max_nodes)

            case "all":
                selection = nodes

        errors: list[Exception] = []
        results: list[R] = []
        
        async for complete in asyncio.as_completed([
            self.send_udp(
                addr,
                data,
                timeout=timeout,
            ) for addr in selection
        ]):
            
            result = await complete
            
            if isinstance(result, Error):
                errors.append(result)

            else:
                results.append(result)

        return (
            results,
            errors,
        )
    
    async def connect_tcp_client(
        self,
        host: str,
        port: int,
        timeout: int | float | None = None,
    ) -> Error | None:
        
        if timeout is None:
            timeout = self._request_timeout

        error: Exception | None = None
        trace: str | None = None

        try:

            self._tcp_client_transports[(host, port)] = await asyncio.wait_for(
                self._connect_tcp_client(
                    (host, port),
                ),
                timeout=timeout,
            )

        except Exception as err:
            error = err
            trace = traceback.format_exc()

            return Error(
                message=str(error),
                traceback=trace,
                node=(host, port)
            )

    def read_client_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        self._pending_tcp_server_responses.append(
            asyncio.ensure_future(
                self.process_tcp_client_resopnse(
                    data,
                    transport,
                ),
            ),
        )

    def read_server_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        self._pending_tcp_server_responses.append(
            asyncio.ensure_future(
                self.process_tcp_server_request(
                    data,
                    transport,
                ),
            ),
        )

    def read_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        sender_addr: tuple[str, int] | None = None,
    ):
        try:
            # Rate limiting (if sender address available)
            if sender_addr is not None:
                if not self._rate_limiter.check(sender_addr):
                    return  # Rate limited - silently drop
            
            # Message size validation (before decompression)
            if len(data) > MAX_MESSAGE_SIZE:
                return  # Message too large - silently drop

            decrypted_data = self._encryptor.decrypt(data)
            
            decrypted = self._decompressor.decompress(decrypted_data)
            
            # Validate decompressed size
            if len(decrypted) > MAX_DECOMPRESSED_SIZE:
                return  # Decompressed message too large - silently drop

            request_type, addr, handler_name, payload = decrypted.split(b'<', maxsplit=3)

            match request_type:

                case b'c':
                    payload, clock = payload.split(b'<', maxsplit=1)


                    self._pending_udp_server_responses.append(
                        asyncio.ensure_future(
                            self.process_udp_server_request(
                                handler_name,
                                addr,
                                payload,
                                int.from_bytes(clock),
                                transport,
                            ),
                        ),
                    )

                case b's':

                    self._pending_udp_server_responses.append(
                        asyncio.ensure_future(
                            self.process_udp_client_response(
                                handler_name,
                                addr,
                                payload,
                            )
                        )
                    )

        except Exception:
            pass

    async def process_tcp_client_resopnse(
        self,
        data: bytes,
        _: asyncio.Transport,
    ):
        
        decrypted = self._decompressor.decompress(
            self._encryptor.decrypt(
                data,
            )
        )

        handler_name, address_bytes, payload, clock = decrypted.split(b'<', maxsplit=3)
        clock_time = int.from_bytes(clock)

        await self._udp_clock.ack(clock_time)

        try:
            addr = parse_address(address_bytes)
        except AddressValidationError:
            return  # Silently drop malformed addresses

        try:

            if request_model := self.tcp_server_request_models.get(handler_name):
                payload = request_model.load(payload)

            handler = self.tcp_client_handler.get(handler_name)
            if handler:
                payload = await handler(
                    addr,
                    payload,
                    clock_time,
                )

            self._tcp_client_data[addr][handler_name].put_nowait((payload, clock_time))

        except Exception as err:
            self._tcp_client_data[addr][handler_name].put_nowait((err, clock_time))
        

    async def process_tcp_server_request(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        # Get client address for rate limiting
        peername = transport.get_extra_info('peername')
        
        try:
            # Rate limiting
            if peername is not None:
                if not self._rate_limiter.check(peername):
                    return  # Rate limited - silently drop
            
            # Message size validation
            if len(data) > MAX_MESSAGE_SIZE:
                return  # Message too large - silently drop
            
            decrypted_data = self._encryptor.decrypt(data)

            decrypted = self._decompressor.decompress(decrypted_data)
            
            # Validate decompressed size
            if len(decrypted) > MAX_DECOMPRESSED_SIZE:
                return  # Decompressed message too large - silently drop

            handler_name, address_bytes, payload, clock = decrypted.split(b'<', maxsplit=3)
            clock_time = int.from_bytes(clock)

            next_time = await self._tcp_clock.update(clock_time)

            try:
                addr = parse_address(address_bytes)
            except AddressValidationError:
                return  # Silently drop malformed addresses
            
            if request_model := self.tcp_server_request_models.get(handler_name):
                payload = request_model.load(payload)

            handler = self.tcp_handlers[handler_name]

            (
                response_handler,
                response,
            ) = await handler(
                addr,
                payload,
                clock_time,
            )

            if isinstance(response, Message):
                response = response.dump()

            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    self._tcp_addr_slug + b'<' + response_handler + b'<' + response + b'<' + next_time.to_bytes(64),
                )
            )

            transport.write(response_payload)

        except Exception:
            # Sanitized error response - don't leak internal details
            try:
                error_time = await self._tcp_clock.tick()
                error_response = self._encryptor.encrypt(
                    self._compressor.compress(
                        self._tcp_addr_slug + b'<error<Request processing failed<' + error_time.to_bytes(64),
                    )
                )
                transport.write(error_response)
            except Exception:
                pass  # Best effort error response

    async def process_udp_server_request(
        self,
        handler_name: bytes,
        addr: bytes,
        payload: bytes,
        clock_time: int,
        transport: asyncio.DatagramTransport,
    ):
        
        next_time = await self._udp_clock.update(clock_time)
        
        try:
            parsed_addr = parse_address(addr)
        except AddressValidationError:
            return  # Silently drop malformed addresses
        
        try:
            if request_models := self.udp_server_request_models.get(handler_name):
                    payload = request_models.load(payload)

            handler = self.udp_handlers[handler_name]
            response = await handler(
                parsed_addr,
                payload,
                clock_time,
            )

            if isinstance(response, Message):
                response = response.dump()

            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    b's<' + self._udp_addr_slug + b'<' + handler_name + b'<' + response + b'<' + next_time.to_bytes(64),
                )
            )

            transport.sendto(response_payload, parsed_addr)

        except Exception:
            # Sanitized error response - don't leak internal details
            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    b's<' + self._udp_addr_slug + b'<' + handler_name + b'<Request processing failed<' + next_time.to_bytes(64),
                )
            )

            transport.sendto(response_payload, parsed_addr)

    async def process_udp_client_response(
        self,
        handler_name: bytes,
        addr: bytes,
        payload: bytes,
    ):
        try:

            payload, clock = payload.split(b'<', maxsplit=1)
            clock_time = int.from_bytes(clock)

            await self._udp_clock.ack(clock_time)

            if response_model := self.udp_client_response_models.get(handler_name):
                    payload = response_model.load(payload)


            handler = self.udp_client_handlers.get(handler_name)
            if handler:
                payload = await handler(
                    addr,
                    payload,
                    clock_time,
                )

            self._udp_client_data[addr][handler_name].put_nowait((payload, clock_time))

        except Exception as err:
            self._udp_client_data[addr][handler_name].put_nowait((err, clock_time))

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

        await self._task_runner.shutdown()

        for client in self._tcp_client_transports.values():
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

        self._task_runner.abort()

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