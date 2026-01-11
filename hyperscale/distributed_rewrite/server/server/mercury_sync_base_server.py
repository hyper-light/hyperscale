


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
    ReplayError,
    validate_message_size,
    parse_address,
    AddressValidationError,
    frame_message,
    DropCounter,
    InFlightTracker,
    MessagePriority,
    PriorityLimits,
)
from hyperscale.distributed_rewrite.server.protocol.security import MessageSizeError
from hyperscale.distributed_rewrite.reliability import ServerRateLimiter
from hyperscale.distributed_rewrite.server.events import LamportClock
from hyperscale.distributed_rewrite.server.hooks.task import (
    TaskCall,
)

from hyperscale.distributed_rewrite.taskex import TaskRunner
from hyperscale.distributed_rewrite.taskex.run import Run
from hyperscale.core.jobs.protocols.constants import MAX_DECOMPRESSED_SIZE, MAX_MESSAGE_SIZE
from hyperscale.core.utils.cancel_and_release_task import cancel_and_release_task
from hyperscale.logging import Logger
from hyperscale.logging.config import LoggingConfig
from hyperscale.logging.hyperscale_logging_models import ServerWarning, SilentDropStats

do_patch()

D = TypeVar("D", bound=msgspec.Struct)
R = TypeVar("R", bound=msgspec.Struct)

Handler = Callable[
    [
        tuple[str, int],
        bytes | msgspec.Struct,
        int,
        asyncio.Transport,  # AD-28: Transport for certificate extraction
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

        # Message queue size limits for backpressure
        self._message_queue_max_size = env.MESSAGE_QUEUE_MAX_SIZE

        # Use bounded queues to prevent memory exhaustion under load
        # When queue is full, put_nowait() will raise QueueFull and message will be dropped
        self._tcp_client_data: dict[
            bytes,
            dict[bytes, asyncio.Queue[bytes]]
        ] = defaultdict(lambda: defaultdict(lambda: asyncio.Queue(maxsize=self._message_queue_max_size)))

        self._udp_client_data: dict[
            bytes,
            dict[bytes, asyncio.Queue[bytes | Message | Exception]]
        ] = defaultdict(lambda: defaultdict(lambda: asyncio.Queue(maxsize=self._message_queue_max_size)))

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
        self._client_replay_guard = ReplayGuard()
        self._rate_limiter = ServerRateLimiter()
        self._secure_random = secrets.SystemRandom()  # Cryptographically secure RNG

        # Drop counters for silent drop monitoring
        self._tcp_drop_counter = DropCounter()
        self._udp_drop_counter = DropCounter()
        self._drop_stats_task: asyncio.Task | None = None
        self._drop_stats_interval = 60.0  # Log drop stats every 60 seconds

        # AD-32: Priority-aware bounded execution trackers
        pending_config = env.get_pending_response_config()
        priority_limits = PriorityLimits(
            critical=0,  # CRITICAL (SWIM) unlimited
            high=pending_config['high_limit'],
            normal=pending_config['normal_limit'],
            low=pending_config['low_limit'],
            global_limit=pending_config['global_limit'],
        )
        self._tcp_in_flight_tracker = InFlightTracker(limits=priority_limits)
        self._udp_in_flight_tracker = InFlightTracker(limits=priority_limits)
        self._pending_response_warn_threshold = pending_config['warn_threshold']
        
        self._tcp_semaphore: asyncio.Semaphore | None= None
        self._udp_semaphore: asyncio.Semaphore | None= None

        self._compressor: zstandard.ZstdCompressor | None = None
        self._decompressor: zstandard.ZstdDecompressor| None = None

        self._tcp_server_cleanup_task: asyncio.Task | None = None
        self._tcp_server_sleep_task: asyncio.Task | None = None

        self._udp_server_cleanup_task: asyncio.Future | None = None
        self._udp_server_sleep_task: asyncio.Future | None = None

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

    async def _log_security_warning(
        self,
        message: str,
        protocol: str = "udp",
    ) -> None:
        """
        Log a security-related warning event.
        
        Used for logging security events like rate limiting, malformed requests,
        decryption failures, etc. without leaking details to clients.
        
        Args:
            message: Description of the security event
            protocol: "tcp" or "udp" to select the appropriate logger
        """
        logger = self._udp_logger if protocol == "udp" else self._tcp_logger
        if logger is not None:
            try:
                await logger.log(
                    ServerWarning(
                        message=message,
                        node_id=0,  # Base server doesn't have node_id
                        node_host=self._host,
                        node_port=self._udp_port if protocol == "udp" else self._tcp_port,
                    )
                )
            except Exception:
                pass  # Best effort logging - don't fail on logging errors

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
        
        # Configure global log level from environment before creating loggers
        LoggingConfig().update(log_level=self.env.MERCURY_SYNC_LOG_LEVEL)

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

        # Mark server as running before starting network listeners
        self._running = True
        
        await self._start_udp_server(
            worker_socket=udp_server_worker_socket,
            worker_transport=udp_server_worker_transport,
        )
        
        await self._start_tcp_server(
            worker_socket=tcp_server_worker_socket,
            worker_server=tcp_server_worker_server,
        )

        if self._tcp_server_cleanup_task is None:
            self._tcp_server_cleanup_task = asyncio.create_task(self._cleanup_tcp_server_tasks())

        if self._udp_server_cleanup_task is None:
            self._udp_server_cleanup_task = asyncio.create_task(self._cleanup_udp_server_tasks())

        if self._drop_stats_task is None:
            self._drop_stats_task = asyncio.create_task(self._log_drop_stats_periodically())

        
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
                lambda: MercurySyncTCPProtocol(self, mode='server'),
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
    ) -> tuple[R | Error, int]:
        try:

            if timeout is None:
                timeout = self._request_timeout
            
            async with self._tcp_semaphore:        
                transport: asyncio.Transport = self._tcp_client_transports.get(address)
                if transport is None or transport.is_closing():
                    transport = await self._connect_tcp_client(address)
                    self._tcp_client_transports[address] = transport


                clock = await self._udp_clock.increment()

                encoded_action = action.encode()
                
                if isinstance(data, Message):
                    data = data.dump()

                # Build the message payload with length-prefixed data to avoid delimiter issues
                # Format: address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
                # Clock comes before data so all fixed-size fields are parsed first
                data_len = len(data).to_bytes(4, 'big')
                payload = self._tcp_addr_slug + b'<' + encoded_action + b'<' + clock.to_bytes(64) + data_len + data
                
                # Compress and encrypt
                encrypted = self._encryptor.encrypt(
                    self._compressor.compress(payload)
                )
                
                # Frame with length prefix for proper TCP stream handling
                transport.write(frame_message(encrypted))


                host, port = address

                target = f'{host}:{port}'.encode()

                return await asyncio.wait_for(
                    self._tcp_client_data[target][encoded_action].get(),
                    timeout=timeout,
                )
            
        except Exception as error:
            transport = self._tcp_client_transports.get(address)
            if transport and not transport.is_closing():
                transport.close()

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
    ) -> tuple[R | Exception, int]:
        try:

            if timeout is None:
                timeout = self._request_timeout
            
            async with self._udp_semaphore:

                clock = await self._udp_clock.increment()

                encoded_action = action.encode()

                if isinstance(data, Message):
                    data = data.dump()

                # UDP message with length-prefixed data to avoid delimiter issues
                # Format: type<address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
                data_len = len(data).to_bytes(4, 'big')
                self._udp_transport.sendto(
                    self._encryptor.encrypt(
                        self._compressor.compress(
                            b'c<' + self._udp_addr_slug + b'<' + encoded_action + b'<' + clock.to_bytes(64) + data_len + data,
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

    def _spawn_tcp_response(
        self,
        coro: Coroutine,
        priority: MessagePriority = MessagePriority.NORMAL,
    ) -> bool:
        """
        Spawn a TCP response task with priority-aware bounded execution (AD-32).

        Returns True if task spawned, False if shed due to load.
        Called from sync protocol callbacks.

        Args:
            coro: The coroutine to execute.
            priority: Message priority for load shedding decisions.

        Returns:
            True if task was spawned, False if request was shed.
        """
        if not self._tcp_in_flight_tracker.try_acquire(priority):
            # Load shedding - increment drop counter
            self._tcp_drop_counter.increment_load_shed()
            return False

        task = asyncio.ensure_future(coro)
        task.add_done_callback(
            lambda t: self._on_tcp_task_done(t, priority)
        )
        self._pending_tcp_server_responses.append(task)
        return True

    def _on_tcp_task_done(
        self,
        task: asyncio.Task,
        priority: MessagePriority,
    ) -> None:
        """Done callback for TCP response tasks - release slot and cleanup."""
        # Retrieve exception to prevent memory leak
        try:
            task.exception()
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass
        except Exception:
            pass  # Logged elsewhere

        # Release the priority slot
        self._tcp_in_flight_tracker.release(priority)

    def _spawn_udp_response(
        self,
        coro: Coroutine,
        priority: MessagePriority = MessagePriority.NORMAL,
    ) -> bool:
        """
        Spawn a UDP response task with priority-aware bounded execution (AD-32).

        Returns True if task spawned, False if shed due to load.
        Called from sync protocol callbacks.

        Args:
            coro: The coroutine to execute.
            priority: Message priority for load shedding decisions.

        Returns:
            True if task was spawned, False if request was shed.
        """
        if not self._udp_in_flight_tracker.try_acquire(priority):
            # Load shedding - increment drop counter
            self._udp_drop_counter.increment_load_shed()
            return False

        task = asyncio.ensure_future(coro)
        task.add_done_callback(
            lambda t: self._on_udp_task_done(t, priority)
        )
        self._pending_udp_server_responses.append(task)
        return True

    def _on_udp_task_done(
        self,
        task: asyncio.Task,
        priority: MessagePriority,
    ) -> None:
        """Done callback for UDP response tasks - release slot and cleanup."""
        # Retrieve exception to prevent memory leak
        try:
            task.exception()
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass
        except Exception:
            pass  # Logged elsewhere

        # Release the priority slot
        self._udp_in_flight_tracker.release(priority)

    def read_client_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        # AD-32: Use priority-aware spawn instead of direct append
        # TCP client responses are typically status updates (NORMAL priority)
        self._spawn_tcp_response(
            self.process_tcp_client_response(
                data,
                transport,
            ),
            priority=MessagePriority.NORMAL,
        )

    def read_server_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        # AD-32: Use priority-aware spawn instead of direct append
        # TCP server requests are typically job commands (HIGH priority)
        self._spawn_tcp_response(
            self.process_tcp_server_request(
                data,
                transport,
            ),
            priority=MessagePriority.HIGH,
        )

    def read_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        sender_addr: tuple[str, int] | None = None,
    ):
        # Early exit if server is not running (defense in depth)
        if not self._running:
            return

        try:
            # Rate limiting (if sender address available)
            if sender_addr is not None:
                if not self._rate_limiter.check(sender_addr):
                    self._udp_drop_counter.increment_rate_limited()
                    return

            # Message size validation (before decompression)
            if len(data) > MAX_MESSAGE_SIZE:
                self._udp_drop_counter.increment_message_too_large()
                return

            try:
                decrypted_data = self._encryptor.decrypt(data)
            except Exception:
                self._udp_drop_counter.increment_decryption_failed()
                return

            decrypted = self._decompressor.decompress(
                decrypted_data,
                max_output_size=MAX_DECOMPRESSED_SIZE,
            )

            # Validate compression ratio to detect compression bombs
            try:
                validate_message_size(len(decrypted_data), len(decrypted))
            except MessageSizeError:
                self._udp_drop_counter.increment_decompression_too_large()
                return

            # Parse length-prefixed UDP message format:
            # type<address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
            request_type, addr, handler_name, rest = decrypted.split(b'<', maxsplit=3)
            # Extract clock (first 64 bytes)
            clock_time = int.from_bytes(rest[:64])
            # Extract data length (next 4 bytes)
            data_len = int.from_bytes(rest[64:68], 'big')
            # Extract payload (remaining bytes)
            payload = rest[68:68 + data_len]

            match request_type:

                case b'c':
                    # AD-32: Use priority-aware spawn instead of direct append
                    # UDP client requests: priority determined by handler (subclass can override)
                    # Default to NORMAL; SWIM handlers override to CRITICAL in subclasses
                    self._spawn_udp_response(
                        self.process_udp_server_request(
                            handler_name,
                            addr,
                            payload,
                            clock_time,
                            transport,
                        ),
                        priority=MessagePriority.NORMAL,
                    )

                case b's':
                    # AD-32: Use priority-aware spawn for server responses
                    # These are typically status updates (NORMAL priority)
                    self._spawn_udp_response(
                        self.process_udp_client_response(
                            handler_name,
                            addr,
                            payload,
                            clock_time,
                            transport,
                        ),
                        priority=MessagePriority.NORMAL,
                    )


        except Exception as err:
            self._udp_drop_counter.increment_malformed_message()

    async def process_tcp_client_response(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        try:
            decrypted_data = self._encryptor.decrypt(data)
            decrypted = self._decompressor.decompress(
                decrypted_data,
                max_output_size=MAX_DECOMPRESSED_SIZE,
            )
            # Validate compression ratio to detect compression bombs
            validate_message_size(len(decrypted_data), len(decrypted))
        except (MessageSizeError, Exception) as decompression_error:
            await self._log_security_warning(
                f"TCP client response decompression failed: {type(decompression_error).__name__}",
                protocol="tcp",
            )
            self._tcp_drop_counter.increment_decompression_too_large()
            return

        # Parse length-prefixed message format:
        # address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
        address_bytes, handler_name, rest = decrypted.split(b'<', maxsplit=2)
        
        # Extract clock (first 64 bytes)
        clock_time = int.from_bytes(rest[:64])
        # Extract data length (next 4 bytes)
        data_len = int.from_bytes(rest[64:68], 'big')
        # Extract payload (remaining bytes)
        payload = rest[68:68 + data_len]

        await self._udp_clock.ack(clock_time)

        try:
            addr = parse_address(address_bytes)
        except AddressValidationError as e:
            await self._log_security_warning(
                f"TCP client response malformed address: {e}",
                protocol="tcp",
            )
            return

        try:

            if request_model := self.tcp_server_request_models.get(handler_name):
                payload = request_model.load(payload)

                # Validate response for replay attacks if it's a Message instance
                if isinstance(payload, Message):
                    try:
                        self._replay_guard.validate_with_incarnation(
                            payload.message_id,
                            payload.sender_incarnation,
                        )
                    except ReplayError:
                        self._tcp_drop_counter.increment_replay_detected()
                        return

            handler = self.tcp_client_handler.get(handler_name)
            if handler:
                payload = await handler(
                    addr,
                    payload,
                    clock_time,
                )

            self._tcp_client_data[address_bytes][handler_name].put_nowait((payload, clock_time))

        except asyncio.QueueFull:
            self._tcp_drop_counter.increment_load_shed()

        except Exception as err:
            self._tcp_client_data[address_bytes][handler_name].put_nowait((err, clock_time))
        

    async def process_tcp_server_request(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        # Get client address for rate limiting
        peername = transport.get_extra_info('peername')
        handler_name = b''

        try:
            # Rate limiting
            if peername is not None:
                if not self._rate_limiter.check(peername):
                    self._tcp_drop_counter.increment_rate_limited()
                    return

            # Message size validation
            if len(data) > MAX_MESSAGE_SIZE:
                self._tcp_drop_counter.increment_message_too_large()
                return

            try:
                decrypted_data = self._encryptor.decrypt(data)
            except Exception:
                self._tcp_drop_counter.increment_decryption_failed()
                return

            decrypted = self._decompressor.decompress(
                decrypted_data,
                max_output_size=MAX_DECOMPRESSED_SIZE,
            )

            # Validate compression ratio to detect compression bombs
            try:
                validate_message_size(len(decrypted_data), len(decrypted))
            except MessageSizeError:
                self._tcp_drop_counter.increment_decompression_too_large()
                return

            # Parse length-prefixed message format:
            # address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
            address_bytes, handler_name, rest = decrypted.split(b'<', maxsplit=2)
            
            # Extract clock (first 64 bytes)
            clock_time = int.from_bytes(rest[:64])
            # Extract data length (next 4 bytes)
            data_len = int.from_bytes(rest[64:68], 'big')
            # Extract payload (remaining bytes)
            payload = rest[68:68 + data_len]

            next_time = await self._tcp_clock.update(clock_time)

            try:
                addr = parse_address(address_bytes)
            except AddressValidationError as e:
                await self._log_security_warning(
                    f"TCP server request malformed address: {e}",
                    protocol="tcp",
                )
                return
            
            
            if request_model := self.tcp_server_request_models.get(handler_name):
                payload = request_model.load(payload)

                # Validate message for replay attacks if it's a Message instance
                if isinstance(payload, Message):
                    try:
                        self._replay_guard.validate_with_incarnation(
                            payload.message_id,
                            payload.sender_incarnation,
                        )
                    except ReplayError:
                        self._tcp_drop_counter.increment_replay_detected()
                        return

            handler = self.tcp_handlers.get(handler_name)
            if handler is None:
                return

            response = await handler(
                addr,
                payload,
                clock_time,
                transport,  # AD-28: Pass transport for certificate extraction
            )

            if isinstance(response, Message):
                response = response.dump()

            if handler_name == b'':
                handler_name = b'error'

            # Build response with clock before length-prefixed data
            # Format: address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
            response_len = len(response).to_bytes(4, 'big')
            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    self._tcp_addr_slug + b'<' + handler_name + b'<' + next_time.to_bytes(64) + response_len + response,
                )
            )

            # Frame with length prefix for proper TCP stream handling
            transport.write(frame_message(response_payload))

        except Exception as e:
            self._tcp_drop_counter.increment_malformed_message()
            # Log security event - could be decryption failure, malformed message, etc.
            await self._log_security_warning(
                f"TCP server request failed: {type(e).__name__}",
                protocol="tcp",
            )
            # Sanitized error response - don't leak internal details
            try:
                error_time = await self._tcp_clock.tick()
                error_msg = b'Request processing failed'
                error_len = len(error_msg).to_bytes(4, 'big')
                error_response = self._encryptor.encrypt(
                    self._compressor.compress(
                        self._tcp_addr_slug + b'<' + handler_name + b'<' + error_time.to_bytes(64) + error_len + error_msg,
                    )
                )
                # Frame with length prefix for proper TCP stream handling
                transport.write(frame_message(error_response))
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
        except AddressValidationError as e:
            await self._log_security_warning(
                f"UDP server request malformed address: {e}",
                protocol="udp",
            )
            return
        
        try:
            if request_models := self.udp_server_request_models.get(handler_name):
                    payload = request_models.load(payload)

                    # Validate message for replay attacks if it's a Message instance
                    if isinstance(payload, Message):
                        try:
                            self._replay_guard.validate_with_incarnation(
                                payload.message_id,
                                payload.sender_incarnation,
                            )
                        except ReplayError:
                            self._udp_drop_counter.increment_replay_detected()
                            return

            handler = self.udp_handlers[handler_name]
            response = await handler(
                parsed_addr,
                payload,
                clock_time,
            )

            if isinstance(response, Message):
                response = response.dump()

            # UDP response with clock before length-prefixed data
            # Format: type<address<handler<clock(64 bytes)data_len(4 bytes)data(N bytes)
            response_len = len(response).to_bytes(4, 'big')
            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    b's<' + self._udp_addr_slug + b'<' + handler_name + b'<' + next_time.to_bytes(64) + response_len + response,
                )
            )

            transport.sendto(response_payload, parsed_addr)

        except Exception as e:
            # Log security event - don't leak internal details
            await self._log_security_warning(
                f"UDP server request failed: {type(e).__name__}",
                protocol="udp",
            )

            # Sanitized error response
            error_msg = b'Request processing failed'
            error_len = len(error_msg).to_bytes(4, 'big')
            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    b's<' + self._udp_addr_slug + b'<' + handler_name + b'<' + next_time.to_bytes(64) + error_len + error_msg,
                )
            )

            transport.sendto(response_payload, parsed_addr)

    async def process_udp_client_response(
        self,
        handler_name: bytes,
        addr: bytes,
        payload: bytes,
        clock_time: int,
        _: asyncio.DatagramTransport,
    ):
        try:
            await self._udp_clock.ack(clock_time)

            if response_model := self.udp_client_response_models.get(handler_name):
                    payload = response_model.load(payload)

                    # Validate message for replay attacks if it's a Message instance
                    if isinstance(payload, Message):
                        try:
                            self._client_replay_guard.validate_with_incarnation(
                                payload.message_id,
                                payload.sender_incarnation,
                            )
                        except ReplayError:
                            self._udp_drop_counter.increment_replay_detected()
                            return


            handler = self.udp_client_handlers.get(handler_name)
            if handler:
                payload = await handler(
                    addr,
                    payload,
                    clock_time,
                )

            self._udp_client_data[addr][handler_name].put_nowait((payload, clock_time))

        except asyncio.QueueFull:
            self._udp_drop_counter.increment_load_shed()

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

    async def _log_drop_stats_periodically(self) -> None:
        """Periodically log silent drop statistics for security monitoring."""
        while self._running:
            try:
                await asyncio.sleep(self._drop_stats_interval)
            except (asyncio.CancelledError, Exception):
                break

            # Get and reset TCP drop stats
            tcp_snapshot = self._tcp_drop_counter.reset()
            if tcp_snapshot.has_drops:
                try:
                    await self._tcp_logger.log(
                        SilentDropStats(
                            message="TCP silent drop statistics",
                            node_id=0,
                            node_host=self._host,
                            node_port=self._tcp_port,
                            protocol="tcp",
                            rate_limited_count=tcp_snapshot.rate_limited,
                            message_too_large_count=tcp_snapshot.message_too_large,
                            decompression_too_large_count=tcp_snapshot.decompression_too_large,
                            decryption_failed_count=tcp_snapshot.decryption_failed,
                            malformed_message_count=tcp_snapshot.malformed_message,
                            load_shed_count=tcp_snapshot.load_shed,
                            total_dropped=tcp_snapshot.total,
                            interval_seconds=tcp_snapshot.interval_seconds,
                        )
                    )
                except Exception:
                    pass  # Best effort logging

            # Get and reset UDP drop stats
            udp_snapshot = self._udp_drop_counter.reset()
            if udp_snapshot.has_drops:
                try:
                    await self._udp_logger.log(
                        SilentDropStats(
                            message="UDP silent drop statistics",
                            node_id=0,
                            node_host=self._host,
                            node_port=self._udp_port,
                            protocol="udp",
                            rate_limited_count=udp_snapshot.rate_limited,
                            message_too_large_count=udp_snapshot.message_too_large,
                            decompression_too_large_count=udp_snapshot.decompression_too_large,
                            decryption_failed_count=udp_snapshot.decryption_failed,
                            malformed_message_count=udp_snapshot.malformed_message,
                            load_shed_count=udp_snapshot.load_shed,
                            total_dropped=udp_snapshot.total,
                            interval_seconds=udp_snapshot.interval_seconds,
                        )
                    )
                except Exception:
                    pass  # Best effort logging

    async def shutdown(self) -> None:
        self._running = False

        await self._task_runner.shutdown()

        for client in self._tcp_client_transports.values():
            client.abort()

        # Close UDP transport to stop receiving datagrams
        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None
            self._udp_connected = False
            
        # Close TCP server to stop accepting connections
        if self._tcp_server is not None:
            self._tcp_server.abort_clients()
            self._tcp_server.close()
            try:
                await self._tcp_server.wait_closed()
            except Exception:
                pass
            self._tcp_server = None
            self._tcp_connected = False

        cancel_and_release_task(self._drop_stats_task)
        cancel_and_release_task(self._tcp_server_sleep_task)
        cancel_and_release_task(self._tcp_server_cleanup_task)
        cancel_and_release_task(self._udp_server_sleep_task)
        cancel_and_release_task(self._udp_server_cleanup_task)

    def abort(self) -> None:
        self._running = False

        self._task_runner.abort()

        # Close UDP transport to stop receiving datagrams
        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None
            self._udp_connected = False

        # Close TCP server
        if self._tcp_server is not None:
            self._tcp_server.close()
            self._tcp_server = None
            self._tcp_connected = False

        # Close all TCP client transports
        for client in self._tcp_client_transports.values():
            try:
                client.abort()
            except Exception:
                pass
        self._tcp_client_transports.clear()

        cancel_and_release_task(self._drop_stats_task)
        cancel_and_release_task(self._tcp_server_sleep_task)
        cancel_and_release_task(self._tcp_server_cleanup_task)
        cancel_and_release_task(self._udp_server_sleep_task)
        cancel_and_release_task(self._udp_server_cleanup_task)