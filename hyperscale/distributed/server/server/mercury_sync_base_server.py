import asyncio
import inspect
import itertools
import secrets
import socket
import ssl
import traceback

try:
    import resource
except ImportError:
    # Windows reports no per-process descriptor limit: no cap.
    resource = None
from collections import defaultdict, deque
from typing import (
    Coroutine,
    Deque,
    Dict,
    Optional,
    Tuple,
    Union,
    Callable,
    Awaitable,
    get_type_hints,
    Literal,
    get_args,
    Generic,
)

import msgspec
import zstandard

from hyperscale.core.engines.client.udp.protocols.dtls import do_patch
from hyperscale.distributed.server.context import Context, T
from hyperscale.distributed.discovery.dns.negative_cache import NegativeCache
from hyperscale.distributed.discovery.dns.resolver import AsyncDNSResolver, DNSError
from hyperscale.distributed.env import Env, TimeParser
from hyperscale.distributed.encryption import AESGCMFernet
from hyperscale.distributed.models import (
    Error,
    Message,
    RateLimitResponse,
)
from hyperscale.distributed.runtime import (
    Clock,
    Random,
    RealClock,
    RealRandom,
    TransportFactory,
)

from hyperscale.distributed.server.server.host_address_resolver import (
    HostAddressResolver,
)
from hyperscale.distributed.server.protocol import (
    MercurySyncTCPProtocol,
    MercurySyncUDPProtocol,
    ReplayGuard,
    ReplayError,
    validate_message_size,
    parse_address,
    AddressValidationError,
    frame_message,
    DropCounter,
    ProtocolInFlightTracker,
    MessagePriority,
    PriorityLimits,
    _classify_handler_to_priority,
)
from hyperscale.distributed.server.protocol.security import MessageSizeError
from hyperscale.distributed.server.protocol.server_state import ServerState
from hyperscale.distributed.reliability import ServerRateLimiter
from hyperscale.distributed.reliability.load_shedding import (
    classify_handler_to_priority,
)
from hyperscale.distributed.server.events import LamportClock
from hyperscale.distributed.server.hooks.task import (
    TaskCall,
)

from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.taskex.run import Run
from hyperscale.core.jobs.protocols.constants import (
    MAX_DECOMPRESSED_SIZE,
    MAX_MESSAGE_SIZE,
)
from hyperscale.core.utils.cancel_and_release_task import cancel_and_release_task
from hyperscale.logging import Logger
from hyperscale.logging.config import LoggingConfig
from hyperscale.logging.hyperscale_logging_models import (
    ServerError,
    ServerWarning,
    SilentDropStats,
)
from hyperscale.core.jobs.tasks.cancel import cancel
import traceback as _traceback


do_patch()


# A TCP/UDP hook as the server invokes it: the sender's address, the
# request payload and the request's logical clock in; the reply (a
# ``Message`` is serialized before it is framed) out.
Handler = Callable[
    [tuple[str, int], bytes | Message, int],
    Awaitable[bytes | Message],
]


class MercurySyncBaseServer(Generic[T]):
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        *,
        clock: Clock | None = None,
        random_source: Random | None = None,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        # Phase 5 dependency-injection seams. ``clock`` covers every
        # wall/monotonic read, ``asyncio.sleep``, and
        # ``asyncio.wait_for`` in this class. ``random_source`` covers
        # the non-crypto peer-selection sites (load balancing); the
        # ``_secure_random`` field below remains for any future crypto
        # use but no longer drives load-balancing under Phase 5.
        # Defaults are stdlib-backed; Phase 6 SIM mode injects
        # ``VirtualClock`` / ``SeededRandom`` here.
        self._clock: Clock = clock if clock is not None else RealClock()
        self._random: Random = (
            random_source if random_source is not None else RealRandom()
        )
        # Phase 6 SIM-mode socket seam. ``None`` in REAL mode — every
        # socket-creation site (``_start_udp_server`` /
        # ``_start_tcp_server`` / ``_connect_tcp_client``) then runs its
        # existing OS-socket code unchanged. In SIM a ``SimTransportFactory``
        # routes those sites through the in-process transport registry
        # with no real sockets. See ``runtime/transport_factory.py``.
        self._transport_factory: TransportFactory | None = transport_factory
        # An OS datagram socket sends to an IP, so in REAL mode a peer
        # addressed by DNS name (a Kubernetes pod's stable name) is
        # resolved first; SIM's transport routes by the address itself.
        self._host_address_resolver: HostAddressResolver | None = (
            None
            if transport_factory is not None
            else HostAddressResolver(
                AsyncDNSResolver(
                    default_ttl_seconds=env.MERCURY_SYNC_HOST_RESOLUTION_TTL,
                    resolution_timeout_seconds=env.MERCURY_SYNC_HOST_RESOLUTION_TIMEOUT,
                    # A name that does not resolve yet (a peer pod still
                    # starting, or restarting) is retried after the same
                    # TTL DNS gives its NXDOMAIN answer, never backed off:
                    # the peer is reachable the moment its record exists.
                    negative_cache=NegativeCache(
                        base_ttl_seconds=env.MERCURY_SYNC_HOST_RESOLUTION_TTL,
                        max_ttl_seconds=env.MERCURY_SYNC_HOST_RESOLUTION_TTL,
                    ),
                )
            )
        )
        self._tcp_clock = LamportClock()
        self._udp_clock = LamportClock()

        # Configure global log level before constructing the loggers so the
        # eager Logger() instances pick up the right level. We initialize
        # loggers eagerly (rather than in start_server) because submodules
        # constructed during __init__ capture these references and would
        # otherwise see None — leading to AttributeError on first .log().
        LoggingConfig().update(log_level=env.MERCURY_SYNC_LOG_LEVEL)
        self._tcp_logger: Logger = Logger()
        self._udp_logger: Logger = Logger()

        self.env = env

        self._host = host
        self._udp_host = host
        self._tcp_port = tcp_port
        self._udp_port = udp_port

        self._encoded_host = host.encode()
        self._encoded_tcp_port = str(tcp_port).encode()
        self._encoded_udp_port = str(udp_port).encode()

        self._tcp_addr_slug = self._encoded_host + b":" + self._encoded_tcp_port
        self._udp_addr_slug = self._encoded_host + b":" + self._encoded_udp_port

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._running = False

        # Set at the end of both terminal paths (``shutdown`` / ``abort``)
        # so ``wait`` returns only once the server is down. An Event
        # rather than a bare future: setting it twice is harmless, it
        # carries no result or exception that could go unretrieved, any
        # number of waiters share it, and it binds to the running loop
        # lazily (safe to construct before any loop exists).
        self._stopped: asyncio.Event = asyncio.Event()

        self._tcp_events: Dict[str, Coroutine] = {}
        self._udp_events: Dict[str, Coroutine] = {}

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

        # In-flight TCP requests by request id. A reply echoes its
        # request's id and resolves exactly that request -- whatever address
        # the request was dialed at (a Service, a forwarded port), and never
        # a later request after this one gave up.
        self._tcp_request_waiters: dict[
            int, asyncio.Future[tuple[bytes | Exception, int]]
        ] = {}
        self._tcp_request_ids = itertools.count(1)
        # Requests waiting on each client transport, and the transports
        # retired from new requests (an invalidated address, a failed
        # request) that close once no request waits on them: a retired
        # transport still carries the replies to the requests sent on it.
        self._tcp_transport_requests: dict[asyncio.Transport, set[int]] = {}
        self._retired_tcp_client_transports: set[asyncio.Transport] = set()
        # Every connection this node's TCP server accepted, and their cap:
        # abort and shutdown close them -- a listener's close leaves them
        # open, still answering for this node (asyncio's own
        # ``Server.abort_clients`` exists from Python 3.13 only).
        maximum_accepted_connections = env.MERCURY_SYNC_MAX_ACCEPTED_TCP_CONNECTIONS
        if maximum_accepted_connections <= 0 and resource is not None:
            descriptor_soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            if descriptor_soft_limit != resource.RLIM_INFINITY:
                maximum_accepted_connections = descriptor_soft_limit // 2
        self._tcp_server_state = ServerState[MercurySyncTCPProtocol](
            max_connections=(
                maximum_accepted_connections if maximum_accepted_connections > 0 else None
            )
        )

        # In-flight UDP requests by request id, as for TCP: a reply echoes
        # its request's id and resolves exactly that request. A reply that
        # arrives after its request gave up finds no waiter and is dropped
        # -- it is never handed to the next request to the same peer -- and
        # nothing is kept per peer once its requests settle.
        self._udp_request_waiters: dict[
            int, asyncio.Future[tuple[bytes | Message | Exception, int]]
        ] = {}
        self._udp_request_ids = itertools.count(1)
        self._udp_datagram_processors = {
            b"c": self.process_udp_server_request,
            b"s": self.process_udp_client_response,
        }

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
            critical=0,  # Ungrouped CRITICAL remains unlimited.
            swim=pending_config["swim_limit"],
            high=pending_config["high_limit"],
            normal=pending_config["normal_limit"],
            low=pending_config["low_limit"],
            global_limit=pending_config["global_limit"],
        )
        self._tcp_in_flight_tracker = ProtocolInFlightTracker(limits=priority_limits)
        self._udp_in_flight_tracker = ProtocolInFlightTracker(limits=priority_limits)
        self._pending_response_warn_threshold = pending_config["warn_threshold"]

        self._tcp_semaphore: asyncio.Semaphore | None = None
        self._udp_semaphore: asyncio.Semaphore | None = None
        # AD-32: per destination, the TCP requests it may hold at once, and
        # how many requests are using its bound -- dropped when the last
        # one settles, so only destinations with requests outstanding are
        # tracked. A destination that stops answering fills its own bound,
        # never the node's: requests queued behind it hold no node-wide
        # slot.
        self._tcp_destination_slots: dict[tuple[str, int], asyncio.Semaphore] = {}
        self._tcp_destination_requests: dict[tuple[str, int], int] = {}
        self._max_requests_per_destination = env.OUTGOING_QUEUE_SIZE

        self._compressor: zstandard.ZstdCompressor | None = None
        self._decompressor: zstandard.ZstdDecompressor | None = None

        self._tcp_server_cleanup_task: asyncio.Task | None = None
        # Cooperative wakeup for the TCP/UDP cleanup loops. ``stop`` /
        # ``shutdown`` resolves the future via ``set_result(None)`` so
        # the loop wakes immediately, observes ``_running == False``,
        # and exits without ever raising ``CancelledError``. Previously
        # we created a sleep *task* and cancelled it; the cleanup loop
        # then swallowed ``CancelledError`` and looped forever — three
        # of these tasks routinely survived ``_force_cancel_survivors``
        # and tripped the simulation supervisor's leak detector.
        self._tcp_server_sleep_task: asyncio.Future | None = None

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
        self._tcp_server_request_transports: dict[tuple[str, int], asyncio.Transport] = {}
        self._tcp_client_response_transports: dict[tuple[str, int], asyncio.Transport] = {}
        self.udp_client_response_models: dict[bytes, type[Message]] = {}
        self.udp_server_request_models: dict[bytes, type[Message]] = {}
        self._udp_recv_arrived_count = 0

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
        self._udp_handler_priorities: dict[bytes, MessagePriority] = {}
        self._udp_handler_admission_groups: dict[bytes, str] = {}

        self.udp_client_handlers: dict[
            bytes,
            Handler,
        ] = {}

        self.task_handlers: dict[str, TaskCall] = {}

        self._tasks: dict[
            str,
            TaskCall,
        ] = {}

        # Initialize TaskRunner eagerly so subclasses' `__init__` (and any
        # submodule they construct that captures `self._task_runner` by
        # reference) see a usable value. The previous pattern of leaving
        # this None and populating in `start_server` meant any submodule
        # captured at __init__ time held a None reference and failed at
        # first run() with `AttributeError: 'NoneType' object has no
        # attribute 'run'`. ``TaskRunner.__init__`` does not require a
        # running event loop.
        self._task_runner: TaskRunner = TaskRunner(0, env)
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
                        node_port=self._udp_port
                        if protocol == "udp"
                        else self._tcp_port,
                    )
                )
            except Exception:
                # The logger itself failed: count the lost record where the
                # next drop report shows it.
                (self._udp_drop_counter if protocol == "udp" else self._tcp_drop_counter).log_write_failed += 1

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

        try:
            await self._start_udp_server(
                worker_socket=udp_server_worker_socket,
                worker_transport=udp_server_worker_transport,
            )

            await self._start_tcp_server(
                worker_socket=tcp_server_worker_socket,
                worker_server=tcp_server_worker_server,
            )
        except Exception:
            self._running = False
            self._close_startup_transports()
            raise

        # Phase 6b: explicit ``self._loop.create_task`` so each task
        # binds to the loop this server was started on rather than
        # implicitly going through ``get_running_loop`` at task-creation
        # time. ``self._loop`` is captured in ``start_server`` before
        # these background tasks are spawned.
        if self._tcp_server_cleanup_task is None:
            self._tcp_server_cleanup_task = self._loop.create_task(
                self._cleanup_tcp_server_tasks()
            )

        if self._udp_server_cleanup_task is None:
            self._udp_server_cleanup_task = self._loop.create_task(
                self._cleanup_udp_server_tasks()
            )

        if self._drop_stats_task is None:
            self._drop_stats_task = self._loop.create_task(
                self._log_drop_stats_periodically()
            )

        for task_name, task in self._tasks.items():
            if task.trigger == "ON_START":
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
        if self._transport_factory is not None:
            # SIM mode: register the UDP protocol with the in-process
            # transport registry instead of binding a real datagram
            # socket. ``register_datagram_endpoint`` fires
            # ``connection_made`` before returning (matching
            # ``loop.create_datagram_endpoint``), so the protocol is
            # wired to its transport on return.
            udp_protocol = MercurySyncUDPProtocol(self)
            self._udp_transport = (
                self._transport_factory.register_datagram_endpoint(
                    (self._udp_host, self._udp_port), udp_protocol
                )
            )
            self._udp_connected = True
            return

        if self._udp_connected is False and worker_socket is None:
            self._udp_server_socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
            )
            self._udp_server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            self._udp_server_socket.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_RCVBUF,
                self.env.MERCURY_SYNC_UDP_SERVER_RCVBUF,
            )
            actual_rcvbuf = self._udp_server_socket.getsockopt(
                socket.SOL_SOCKET, socket.SO_RCVBUF
            )
            self._udp_actual_rcvbuf = actual_rcvbuf
            await self._udp_logger.log(
                ServerError(
                    message=(
                        f"[UDP-RCVBUF] requested="
                        f"{self.env.MERCURY_SYNC_UDP_SERVER_RCVBUF} "
                        f"actual={actual_rcvbuf} host={self._udp_host} "
                        f"port={self._udp_port}"
                    ),
                    node_host=self._udp_host,
                    node_port=self._udp_port,
                    node_id=str(self._udp_port),
                ),
            )
            self._udp_server_socket.bind((self._udp_host, self._udp_port))

            self._udp_server_socket.setblocking(False)

        elif self._udp_connected is False and worker_socket:
            self._udp_server_socket = worker_socket
            host, port = worker_socket.getsockname()
            self._udp_host = host
            self._udp_port = port

        elif self._udp_connected is False:
            self._udp_transport = worker_transport

            address_info: Tuple[str, int] = self._udp_transport.get_extra_info(
                "sockname"
            )
            self._udp_server_socket: socket.socket = self._udp_transport.get_extra_info(
                "socket"
            )

            host, port = address_info
            self._udp_host = host
            self._udp_port = port

            self._udp_connected = True

        # No TLS on the datagram socket: Python's ssl has no DTLS, and
        # wrap_socket refuses anything but a stream socket -- the attempt,
        # made whenever certificates were configured, raised
        # NotImplementedError and the UDP server never started. Datagrams
        # are authenticated at the message layer instead.
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
        if self._transport_factory is not None:
            # SIM mode: register the server-side TCP protocol factory
            # with the in-process transport registry instead of binding
            # a real listening socket. A fresh ``MercurySyncTCPProtocol``
            # is built per accepted connection (matching
            # ``loop.create_server``); the server side receives its
            # transport via ``connection_made`` when a peer dials in.
            self._transport_factory.register_stream_server(
                (self._host, self._tcp_port),
                lambda: MercurySyncTCPProtocol(
                    self, mode="server", server_state=self._tcp_server_state
                ),
            )
            self._tcp_connected = True
            return

        if self._server_cert_path and self._server_key_path:
            self._server_tcp_ssl_context = self._create_tcp_server_ssl_context()

        if self._tcp_connected is False and worker_socket is None:
            self._tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._tcp_server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )

            try:
                self._tcp_server_socket.bind((self._host, self._tcp_port))
            except OSError as bind_error:
                self._tcp_server_socket.close()
                self._tcp_server_socket = None
                raise OSError(
                    "Unable to bind TCP server to "
                    f"{self._host}:{self._tcp_port}"
                ) from bind_error

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
                lambda: MercurySyncTCPProtocol(
                    self, mode="server", server_state=self._tcp_server_state
                ),
                sock=self._tcp_server_socket,
                ssl=self._server_tcp_ssl_context,
                backlog=self.env.MERCURY_SYNC_TCP_SERVER_BACKLOG,
            )

            self._tcp_server = server
            self._tcp_connected = True
            if self._tcp_server.sockets:
                # The host stays the one this node was started with: it
                # is the identity every frame, SWIM entry and NodeId
                # carries, and peers address this node by it. A DNS name
                # bound here would otherwise be replaced by its IP while
                # the frames still declare the name.
                self._tcp_port = self._tcp_server.sockets[0].getsockname()[1]

    def _close_startup_transports(self) -> None:
        """Close partially-started listeners after startup failure."""
        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None
            self._udp_connected = False
        if self._udp_server_socket is not None:
            try:
                self._udp_server_socket.close()
            except OSError:
                pass
            self._udp_server_socket = None
        if self._tcp_server is not None:
            self._tcp_server.close()
            self._tcp_server = None
            self._tcp_connected = False
        if self._tcp_server_socket is not None:
            try:
                self._tcp_server_socket.close()
            except OSError:
                pass
            self._tcp_server_socket = None

    def _create_tcp_server_ssl_context(self) -> ssl.SSLContext:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_ctx.options |= ssl.OP_SINGLE_DH_USE
        ssl_ctx.options |= ssl.OP_SINGLE_ECDH_USE
        ssl_ctx.load_cert_chain(self._server_cert_path, keyfile=self._server_key_path)
        ssl_ctx.load_verify_locations(cafile=self._server_cert_path)
        # Hostname verification: disabled by default for local testing,
        # set MERCURY_SYNC_TLS_VERIFY_HOSTNAME=true in production
        ssl_ctx.check_hostname = (
            self.env.MERCURY_SYNC_TLS_VERIFY_HOSTNAME.lower() == "true"
        )

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
                    hasattr(member, "is_hook")
                    and hasattr(member, "type")
                    and getattr(member, "type") == "tcp"
                ),
            )
        }

        for hook in hooks.values():
            # hook_metadata = hook.__func__
            hook = hook.__get__(self, self.__class__)
            setattr(self, hook.name, hook)
            # hook_metadata = getattr(hook, "__func__", hook)

            signature = inspect.signature(hook)
            encoded_hook_name = hook.name.encode()

            for param in signature.parameters.values():
                if param.annotation in msgspec.Struct.__subclasses__():
                    self.tcp_server_request_models[encoded_hook_name] = param.annotation
                    request_model_name = param.annotation.__name__.encode()

                    self._model_handler_map[request_model_name] = encoded_hook_name

            return_type = get_type_hints(hook).get("return")
            self.tcp_client_response_models[encoded_hook_name] = return_type

            if hook.action == "receive":
                self.tcp_handlers[encoded_hook_name] = hook

            elif hook.action == "handle":
                self.tcp_client_handler[hook.target] = hook

    def _get_udp_hooks(self):
        hooks: Dict[str, Handler] = {
            name: hook
            for name, hook in inspect.getmembers(
                self,
                predicate=lambda member: (
                    hasattr(member, "is_hook")
                    and hasattr(member, "type")
                    and getattr(member, "type") == "udp"
                ),
            )
        }

        for hook in hooks.values():
            hook_metadata = hook.__func__
            hook = hook.__get__(self, self.__class__)
            setattr(self, hook.name, hook)
            hook_metadata = getattr(hook, "__func__", hook)

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

            if hook.action == "receive":
                self.udp_handlers[encoded_hook_name] = hook
                hook_priority = hook_metadata.priority
                if hook_priority is not None:
                    self._udp_handler_priorities[encoded_hook_name] = hook_priority
                hook_admission_group = hook_metadata.admission_group
                if hook_admission_group is not None:
                    self._udp_handler_admission_groups[encoded_hook_name] = (
                        hook_admission_group
                    )

            elif hook.action == "handle":
                self.udp_client_handlers[hook.target] = hook

    def _get_task_hooks(self):
        hooks: Dict[str, Handler] = {
            name: hook
            for name, hook in inspect.getmembers(
                self,
                predicate=lambda member: (
                    hasattr(member, "is_hook")
                    and hasattr(member, "type")
                    and getattr(member, "type") == "task"
                ),
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
        if self._transport_factory is not None:
            # SIM mode: dial the peer through the in-process transport
            # registry instead of creating a real socket + connecting
            # via ``run_in_executor`` (which the SimulationLoop bans) +
            # ``loop.create_connection``. Raises ``ConnectionRefusedError``
            # when no server listens at ``address`` — same as REAL.
            client_transport, _ = await self._transport_factory.connect_stream(
                (self._host, self._tcp_port),
                address,
                lambda: MercurySyncTCPProtocol(self),
            )
            self._tcp_client_transports[address] = client_transport
            return client_transport

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
                    # asyncio requires server_hostname whenever ssl is
                    # used with a pre-connected socket -- without it
                    # every TLS connect died with ValueError before the
                    # handshake, hostname verification on or off. With
                    # verification on, this is also the name the peer
                    # certificate is checked against.
                    server_hostname=(
                        address[0]
                        if self._client_tcp_ssl_context is not None
                        else None
                    ),
                )

                self._tcp_client_transports[address] = client_transport

                return client_transport

            except ConnectionRefusedError as connection_error:
                last_error = connection_error

            await self._clock.sleep(1)

        if last_error:
            raise last_error

    def _create_tcp_client_ssl_context(self) -> ssl.SSLContext:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_ctx.load_cert_chain(self._client_cert_path, keyfile=self._client_key_path)
        ssl_ctx.load_verify_locations(cafile=self._client_cert_path)
        # Hostname verification: disabled by default for local testing,
        # set MERCURY_SYNC_TLS_VERIFY_HOSTNAME=true in production
        ssl_ctx.check_hostname = (
            self.env.MERCURY_SYNC_TLS_VERIFY_HOSTNAME.lower() == "true"
        )
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

    def _invalidate_tcp_client_transport(self, address: tuple[str, int]) -> None:
        """Drop the cached TCP client transport to ``address``.

        ``send_tcp`` reuses one cached transport per remote address while
        it is not ``is_closing()``, which only turns True once the loop
        has observed ``connection_lost``. When the process at an address
        dies and a new one starts there, nothing observes the old
        listener's death -- the cached transport just stops being
        answered, and the next send waits out its full timeout on it.
        Callers that learn the process at an address was replaced drop
        the transport here so the next send dials whoever listens now.
        Requests already sent on it keep waiting for their replies -- the
        process may never have been replaced -- and it closes once they
        settle.
        """
        if (cached := self._tcp_client_transports.pop(address, None)) is None:
            return

        # Replies are matched by request id, so the requests already sent
        # on it still get theirs from a live peer; closing it under them
        # only lost those replies.
        if self._tcp_transport_requests.get(cached):
            self._retired_tcp_client_transports.add(cached)
        else:
            cached.abort()

    async def send_tcp(
        self,
        address: tuple[str, int],
        action: str,
        data: bytes | Message,
        timeout: int | float | None = None,
    ) -> tuple[bytes | Exception, int]:
        transport: asyncio.Transport | None = None
        if timeout is None:
            timeout = self._request_timeout
        # One deadline for the whole request: its waits for slots, its dial
        # and its reply (the dial and the reply each used to get all of it).
        deadline = self._clock.monotonic() + timeout
        if (destination_slots := self._tcp_destination_slots.get(address)) is None:
            destination_slots = self._tcp_destination_slots[address] = asyncio.Semaphore(
                self._max_requests_per_destination
            )
        self._tcp_destination_requests[address] = self._tcp_destination_requests.get(address, 0) + 1
        holds_destination_slot = False
        try:
            # The destination's own bound first: a request waiting on a
            # destination that stopped answering holds no node-wide slot.
            # A free slot is taken at once; only a request that must queue
            # waits -- within its deadline.
            if destination_slots.locked():
                await self._clock.wait_for(destination_slots.acquire(), timeout=timeout)
            else:
                await destination_slots.acquire()
            holds_destination_slot = True
            if self._tcp_semaphore.locked():
                await self._clock.wait_for(
                    self._tcp_semaphore.acquire(), timeout=max(0.0, deadline - self._clock.monotonic())
                )
            else:
                await self._tcp_semaphore.acquire()
            try:
                transport = self._tcp_client_transports.get(address)
                if transport is None or transport.is_closing():
                    # The dial must sit INSIDE the request timeout: a
                    # down/unroutable peer otherwise hangs the connect
                    # forever — and it hangs holding _tcp_semaphore,
                    # starving every other outbound TCP send. (Under
                    # SIM an unroutable dial never completes at all;
                    # in REAL mode it waits on the OS connect timeout.)
                    transport = await self._clock.wait_for(
                        self._connect_tcp_client(address),
                        timeout=max(0.0, deadline - self._clock.monotonic()),
                    )
                    self._tcp_client_transports[address] = transport

                clock = await self._udp_clock.increment()

                encoded_action = action.encode()

                if isinstance(data, Message):
                    data = data.dump()

                # An id unique among this node's requests (wrapping at 8
                # bytes); the reply echoes it, and it is all the reply is
                # matched on.
                request_id = next(self._tcp_request_ids) & 0xFFFFFFFFFFFFFFFF

                # Message payload with length-prefixed data to avoid delimiter issues
                # Format: address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes)
                # Clock comes before data so all fixed-size fields are parsed first
                payload = (
                    self._tcp_addr_slug
                    + b"<"
                    + encoded_action
                    + b"<"
                    + clock.to_bytes(64)
                    + request_id.to_bytes(8, "big")
                    + len(data).to_bytes(4, "big")
                    + data
                )

                # Compress and encrypt
                encrypted = self._encryptor.encrypt(self._compressor.compress(payload))

                reply = asyncio.get_running_loop().create_future()
                self._tcp_request_waiters[request_id] = reply
                if (pending_requests := self._tcp_transport_requests.get(transport)) is None:
                    pending_requests = self._tcp_transport_requests[transport] = set()
                pending_requests.add(request_id)
                try:
                    # Frame with length prefix for proper TCP stream handling
                    transport.write(frame_message(encrypted))
                    return await self._clock.wait_for(
                        reply, timeout=max(0.0, deadline - self._clock.monotonic())
                    )

                finally:
                    self._tcp_request_waiters.pop(request_id, None)
                    # This request has settled (answered, timed out, or
                    # cancelled); a retired transport closes with its last.
                    pending_requests.discard(request_id)
                    if not pending_requests:
                        self._tcp_transport_requests.pop(transport, None)
                        if transport in self._retired_tcp_client_transports:
                            self._retired_tcp_client_transports.discard(transport)
                            transport.abort()
            finally:
                self._tcp_semaphore.release()

        except Exception as error:
            # New requests stop using the connection this one failed on,
            # while the requests still waiting on it keep their replies: it
            # closes once they settle. A newer connection to the address
            # is not this request's to close.
            if transport is not None and self._tcp_client_transports.get(address) is transport:
                del self._tcp_client_transports[address]
                if self._tcp_transport_requests.get(transport):
                    self._retired_tcp_client_transports.add(transport)
                else:
                    transport.abort()

            return (
                error,
                self._tcp_clock.time,
            )

        finally:
            if holds_destination_slot:
                destination_slots.release()
            if (remaining_requests := self._tcp_destination_requests[address] - 1) == 0:
                del self._tcp_destination_requests[address]
                del self._tcp_destination_slots[address]
            else:
                self._tcp_destination_requests[address] = remaining_requests

    async def send_udp(
        self,
        address: tuple[str, int],
        action: str,
        data: bytes | Message,
        timeout: int | float | None = None,
    ) -> tuple[bytes | Exception, int]:
        try:
            if timeout is None:
                timeout = self._request_timeout

            async with self._udp_semaphore:
                clock = await self._udp_clock.increment()

                encoded_action = action.encode()

                if isinstance(data, Message):
                    data = data.dump()

                # UDP message with length-prefixed data to avoid delimiter issues
                # Format: type<address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes)
                data_len = len(data).to_bytes(4, "big")
                request_id = next(self._udp_request_ids) & 0xFFFFFFFFFFFFFFFF
                if not isinstance(address, tuple):
                    # asyncio's ``sendto`` raises a TypeError that's
                    # swallowed by the transport's internal
                    # ``_fatal_error`` handler and logged as
                    # ``Fatal write error on datagram transport`` —
                    # invisible to application code, which keeps thinking
                    # the send succeeded while messages are silently
                    # dropped. Surface the type-mismatch via the async
                    # logger (with a captured stack so the offending
                    # caller is identifiable) and raise so the failure
                    # propagates through the existing ``except Exception``
                    # path rather than going down the silent asyncio
                    # fatal-error path. ``traceback.format_stack`` is
                    # the sync-but-non-blocking variant — it returns
                    # strings instead of writing to stderr, so we can
                    # include the trace inside the log payload without
                    # touching stderr from the event loop.
                    stack_str = "".join(_traceback.format_stack())
                    await self._udp_logger.log(
                        ServerError(
                            message=(
                                f"send_udp called with non-tuple address "
                                f"(type={type(address).__name__}, value={address!r}, "
                                f"action={action!r}); message dropped. Caller "
                                f"stack:\n{stack_str}"
                            ),
                            node_host=self._host,
                            node_port=self._udp_port,
                            node_id=0,
                            protocol="udp",
                        )
                    )
                    raise TypeError(
                        f"send_udp address must be tuple[str, int], "
                        f"got {type(address).__name__}: {address!r}"
                    )
                destination = (
                    address
                    if self._host_address_resolver is None
                    else await self._host_address_resolver.resolve(address)
                )
                reply = asyncio.get_running_loop().create_future()
                self._udp_request_waiters[request_id] = reply
                try:
                    self._udp_transport.sendto(
                        self._encryptor.encrypt(
                            self._compressor.compress(
                                b"c<"
                                + self._udp_addr_slug
                                + b"<"
                                + encoded_action
                                + b"<"
                                + clock.to_bytes(64)
                                + request_id.to_bytes(8, "big")
                                + data_len
                                + data,
                            )
                        ),
                        destination,
                    )
                    return await self._clock.wait_for(reply, timeout=timeout)
                finally:
                    # Answered, timed out or cancelled: a later reply to it
                    # is dropped.
                    self._udp_request_waiters.pop(request_id, None)

        except Exception as error:
            return (
                error,
                self._udp_clock.time,
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
            self._tcp_client_transports[(host, port)] = await self._clock.wait_for(
                self._connect_tcp_client(
                    (host, port),
                ),
                timeout=timeout,
            )

        except Exception as err:
            error = err
            trace = traceback.format_exc()

            return Error(message=str(error), traceback=trace, node=(host, port))

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

        # Phase 6b: explicit ``self._loop.create_task`` so the task
        # binds to the loop this server was started on rather than
        # implicitly going through ``get_running_loop`` at task-creation
        # time.
        task = self._loop.create_task(coro)
        task.add_done_callback(lambda t: self._on_tcp_task_done(t, priority))
        self._pending_tcp_server_responses.append(task)
        return True

    def _on_tcp_task_done(
        self,
        task: asyncio.Task,
        priority: MessagePriority,
    ) -> None:
        """Done callback for TCP response tasks - release slot and cleanup."""
        # A handler's own errors become error replies inside it; one that
        # escaped it is a bug, reported here rather than dropped.
        if not task.cancelled() and (escaped_error := task.exception()) is not None:
            self._task_runner.run(
                self._tcp_logger.log,
                ServerError(
                    message=f"TCP response task raised: {escaped_error!r}",
                    node_id=str(self._tcp_port),
                    node_host=self._host,
                    node_port=self._tcp_port,
                ),
            )

        # Release the priority slot
        self._tcp_in_flight_tracker.release(priority)

    def _spawn_udp_response(
        self,
        coro: Coroutine,
        priority: MessagePriority = MessagePriority.NORMAL,
        admission_group: str | None = None,
    ) -> bool:
        """
        Spawn a UDP response task with priority-aware bounded execution (AD-32).

        Returns True if task spawned, False if shed due to load.
        Called from sync protocol callbacks.

        For CRITICAL messages (SWIM probes/acks/suspects/alives,
        leadership traffic) the task creation is scheduled via
        ``loop.call_soon`` rather than executed inline. This places
        the eventual ``Task.__step`` call in the ready queue for the
        *next* loop iteration, ahead of coroutines that have already
        yielded earlier — giving SWIM control traffic preferential
        scheduling over any non-control coroutines that have
        accumulated awaitables. The receive callback also returns
        faster (skips the ``ensure_future`` cost), so subsequent
        UDP packets can be drained from the OS buffer sooner. Lower
        priorities use the direct synchronous spawn path because
        deferral would only add unnecessary loop iterations.

        Args:
            coro: The coroutine to execute.
            priority: Message priority for load shedding decisions.

        Returns:
            True if task was spawned, False if request was shed.
        """
        if not self._udp_in_flight_tracker.try_acquire(
            priority,
            admission_group=admission_group,
        ):
            # Load shedding - increment drop counter. Explicitly
            # close the rejected coroutine so it does not surface as
            # a "coroutine was never awaited" warning per CLAUDE.md's
            # no-orphan rule.
            coro.close()
            self._udp_drop_counter.increment_load_shed()
            if self._udp_drop_counter.load_shed % 10 == 0:
                tracker = self._udp_in_flight_tracker
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=(
                            f"[UDP-LOAD-SHED] priority={priority.name} "
                            f"group={admission_group} "
                            f"total_shed={self._udp_drop_counter.load_shed} "
                            f"in_flight={tracker._counts} "
                            f"group_in_flight={tracker._group_counts} "
                            f"acquired_total={tracker._acquired_total} "
                            f"group_acquired_total="
                            f"{tracker._group_acquired_total}"
                        ),
                        node_host=self._udp_host,
                        node_port=self._udp_port,
                        node_id=str(self._udp_port),
                    ),
                )
            return False

        if priority == MessagePriority.CRITICAL:
            # Defer the task creation to the next loop tick so SWIM
            # responses don't get stuck behind a deep synchronous
            # ``read_udp`` chain processing a backlog.
            asyncio.get_event_loop().call_soon(
                self._spawn_udp_response_deferred,
                coro,
                priority,
                admission_group,
            )
            return True

        # Phase 6b: explicit ``self._loop.create_task`` so the task
        # binds to the loop this server was started on rather than
        # implicitly going through ``get_running_loop`` at task-creation
        # time.
        task = self._loop.create_task(coro)
        task.add_done_callback(
            lambda task: self._on_udp_task_done(
                task,
                priority,
                admission_group,
            )
        )
        self._pending_udp_server_responses.append(task)
        return True

    def _spawn_udp_response_deferred(
        self,
        coro: Coroutine,
        priority: MessagePriority,
        admission_group: str | None,
    ) -> None:
        """``call_soon`` callback that completes a deferred CRITICAL spawn.

        Splitting the spawn so the deferred path can still install the
        done-callback and track the task in
        ``_pending_udp_server_responses`` for shutdown drain.
        """
        if not self._running:
            # Server stopped between schedule and execution; release
            # the priority slot we acquired in ``_spawn_udp_response``
            # so the in-flight tracker doesn't leak it. Close the
            # deferred coroutine too so it does not surface as an
            # orphan-coroutine warning per CLAUDE.md's no-orphan rule.
            coro.close()
            self._udp_in_flight_tracker.release(
                priority,
                admission_group=admission_group,
            )
            return
        # Phase 6b: explicit ``self._loop.create_task`` so the deferred
        # task binds to the loop this server was started on rather than
        # implicitly going through ``get_running_loop`` at task-creation
        # time.
        task = self._loop.create_task(coro)
        task.add_done_callback(
            lambda completed_task: self._on_udp_task_done(
                completed_task,
                priority,
                admission_group,
            )
        )
        self._pending_udp_server_responses.append(task)

    def _on_udp_task_done(
        self,
        task: asyncio.Task,
        priority: MessagePriority,
        admission_group: str | None = None,
    ) -> None:
        """Done callback for UDP response tasks - release slot and cleanup."""
        # A handler's own errors become error replies inside it; one that
        # escaped it is a bug, reported here rather than dropped.
        if not task.cancelled() and (escaped_error := task.exception()) is not None:
            self._task_runner.run(
                self._udp_logger.log,
                ServerError(
                    message=f"UDP response task raised: {escaped_error!r}",
                    node_id=str(self._udp_port),
                    node_host=self._host,
                    node_port=self._udp_port,
                ),
            )

        # Release the priority slot
        self._udp_in_flight_tracker.release(
            priority,
            admission_group=admission_group,
        )

    def lose_client_tcp(self, transport: asyncio.Transport) -> None:
        """A connection this node dialed closed: the requests waiting on
        it can get no reply, so each fails now rather than at its timeout
        (a peer that crashed mid-request held its caller the whole
        timeout)."""
        for request_id in self._tcp_transport_requests.get(transport, ()):
            if (waiter := self._tcp_request_waiters.get(request_id)) is not None and not waiter.done():
                waiter.set_exception(
                    ConnectionResetError(
                        f"connection to {transport.get_extra_info('peername')} "
                        "closed before the reply"
                    )
                )

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
            # Rate limiting (if sender address available). ``read_udp``
            # is invoked from ``MercurySyncUDPProtocol.datagram_received``
            # which is sync, so we need a synchronous rate-limit
            # primitive — use ``check_sync`` rather than the async
            # ``check`` (the latter created a discarded coroutine and
            # silently disabled rate-limiting entirely).
            if sender_addr is not None:
                if not self._rate_limiter.check_sync(sender_addr):
                    self._udp_drop_counter.increment_rate_limited()
                    if self._udp_drop_counter.rate_limited % 10 == 0:
                        self._task_runner.run(
                            self._udp_logger.log,
                            ServerError(
                                message=(
                                    f"[FRAMEWORK-RL-DROP] from={sender_addr} "
                                    f"total_rate_limited="
                                    f"{self._udp_drop_counter.rate_limited} "
                                    f"data_len={len(data)}"
                                ),
                                node_host=self._udp_host,
                                node_port=self._udp_port,
                                node_id=str(self._udp_port),
                            ),
                        )
                    return

            # Message size validation (before decompression)
            if len(data) > MAX_MESSAGE_SIZE:
                self._udp_drop_counter.increment_message_too_large()
                if self._udp_drop_counter.message_too_large % 10 == 0:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerError(
                            message=(
                                f"[UDP-SIZE-DROP] from={sender_addr} "
                                f"len={len(data)} "
                                f"total={self._udp_drop_counter.message_too_large}"
                            ),
                            node_host=self._udp_host,
                            node_port=self._udp_port,
                            node_id=str(self._udp_port),
                        ),
                    )
                return

            try:
                decrypted_data = self._encryptor.decrypt(data)
            except Exception as decrypt_error:
                self._udp_drop_counter.increment_decryption_failed()
                if self._udp_drop_counter.decryption_failed % 10 == 0:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerError(
                            message=(
                                f"[UDP-DECRYPT-DROP] from={sender_addr} "
                                f"err={type(decrypt_error).__name__} "
                                f"total={self._udp_drop_counter.decryption_failed}"
                            ),
                            node_host=self._udp_host,
                            node_port=self._udp_port,
                            node_id=str(self._udp_port),
                        ),
                    )
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
                if self._udp_drop_counter.decompression_too_large % 10 == 0:
                    self._task_runner.run(
                        self._udp_logger.log,
                        ServerError(
                            message=(
                                f"[UDP-DECOMPRESS-DROP] from={sender_addr} "
                                f"total="
                                f"{self._udp_drop_counter.decompression_too_large}"
                            ),
                            node_host=self._udp_host,
                            node_port=self._udp_port,
                            node_id=str(self._udp_port),
                        ),
                    )
                return

            # Parse length-prefixed UDP message format:
            # type<address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes)
            request_type, addr, handler_name, rest = decrypted.split(b"<", maxsplit=3)
            clock_time = int.from_bytes(rest[:64])
            request_id = int.from_bytes(rest[64:72], "big")
            data_len = int.from_bytes(rest[72:76], "big")
            payload = rest[76 : 76 + data_len]

            # Classify priority from explicit hook metadata first, then
            # fall back to AD-37 handler-name classification. SWIM uses
            # an umbrella hook name (`receive`) and stores its admission
            # policy on the hook so framework admission does not need to
            # parse SWIM's embedded message type.
            handler_priority = self._udp_handler_priorities.get(handler_name)
            admission_group = self._udp_handler_admission_groups.get(handler_name)

            if handler_priority is None:
                try:
                    handler_priority = _classify_handler_to_priority(
                        handler_name.decode("utf-8")
                    )
                except UnicodeDecodeError:
                    handler_priority = MessagePriority.NORMAL

            # A request (``c``) or a reply (``s``); anything else, though
            # authenticated, is counted malformed (the except below).
            process_datagram = self._udp_datagram_processors[request_type]
            self._spawn_udp_response(
                process_datagram(
                    handler_name,
                    addr,
                    payload,
                    clock_time,
                    request_id,
                    transport,
                ),
                priority=handler_priority,
                admission_group=admission_group,
            )

        except Exception as err:
            self._udp_drop_counter.increment_malformed_message()
            if self._udp_drop_counter.malformed_message % 10 == 0:
                self._task_runner.run(
                    self._udp_logger.log,
                    ServerError(
                        message=(
                            f"[UDP-MALFORMED] err={type(err).__name__}:{err} "
                            f"total={self._udp_drop_counter.malformed_message} "
                            f"data_len={len(data) if data else 0}"
                        ),
                        node_host=self._udp_host,
                        node_port=self._udp_port,
                        node_id=str(self._udp_port),
                    ),
                )

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
        # address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes)
        # A reply missing its separators names no request: it is dropped
        # here rather than escaping this task as a ValueError.
        try:
            address_bytes, handler_name, rest = decrypted.split(b"<", maxsplit=2)
        except ValueError:
            self._tcp_drop_counter.increment_malformed_message()
            await self._log_security_warning(
                "TCP client response malformed: missing address or handler separator",
                protocol="tcp",
            )
            return

        # Extract clock (first 64 bytes)
        clock_time = int.from_bytes(rest[:64])
        # Extract the id of the request this replies to (next 8 bytes)
        request_id = int.from_bytes(rest[64:72], "big")
        # Extract data length (next 4 bytes)
        data_len = int.from_bytes(rest[72:76], "big")
        # Extract payload (remaining bytes)
        payload = rest[76 : 76 + data_len]

        await self._udp_clock.ack(clock_time)

        try:
            addr = parse_address(address_bytes)
        except AddressValidationError as e:
            await self._log_security_warning(
                f"TCP client response malformed address: {e}",
                protocol="tcp",
            )
            return

        # The reply goes to the request waiting on its id. A reply whose
        # request already gave up (timed out or was cancelled) has no
        # waiter and is dropped: it is never handed to a later request.
        try:

            self._tcp_client_response_transports[addr] = transport
            payload = self._load_tcp_reply_payload(handler_name, payload)

            handler = self.tcp_client_handler.get(handler_name)
            if handler:
                payload = await handler(
                    addr,
                    payload,
                    clock_time,
                )

            waiter = self._tcp_request_waiters.pop(request_id, None)
            if waiter is not None and not waiter.done():
                waiter.set_result((payload, clock_time))

        except ReplayError:
            self._tcp_drop_counter.increment_replay_detected()

        except Exception as err:
            waiter = self._tcp_request_waiters.pop(request_id, None)
            if waiter is not None and not waiter.done():
                waiter.set_result((err, clock_time))

    def _load_tcp_reply_payload(self, handler_name: bytes, payload: bytes) -> bytes | Message:
        """A TCP reply's payload as its handler's model, when it has one; a
        ``Message`` is checked against replay first (``ReplayError``)."""
        if request_model := self.tcp_server_request_models.get(handler_name):
            payload = request_model.load(payload)
            if isinstance(payload, Message):
                self._replay_guard.validate_with_incarnation(
                    payload.message_id,
                    payload.sender_incarnation,
                )
        return payload

    async def _admit_tcp_request(
        self,
        peername: tuple[str, int],
        handler_name: bytes,
    ) -> RateLimitResponse | None:
        """AD-24 admission for one TCP request; the 429 to send if refused."""
        decoded_handler_name = handler_name.decode(errors="replace")
        admission = await self._rate_limiter.check_handler(
            peername,
            decoded_handler_name,
            classify_handler_to_priority(decoded_handler_name),
        )
        if admission.allowed:
            return None

        self._tcp_drop_counter.increment_rate_limited()
        return RateLimitResponse(
            operation=decoded_handler_name,
            retry_after_seconds=admission.retry_after_seconds,
            tokens_remaining=admission.tokens_remaining,
        )

    def _write_tcp_response(
        self,
        transport: asyncio.Transport,
        handler_name: bytes,
        clock_time: int,
        request_id: int,
        response: bytes,
    ) -> None:
        """Frame and write the response to request ``request_id``.

        Format: address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes),
        compressed, encrypted, and length-prefixed for the TCP stream.
        """
        response_payload = self._encryptor.encrypt(
            self._compressor.compress(
                self._tcp_addr_slug
                + b"<"
                + handler_name
                + b"<"
                + clock_time.to_bytes(64)
                + request_id.to_bytes(8, "big")
                + len(response).to_bytes(4, "big")
                + response,
            )
        )
        transport.write(frame_message(response_payload))

    async def process_tcp_server_request(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        # Get client address for rate limiting
        peername = transport.get_extra_info("peername")
        handler_name = b""
        # No request matches id 0: an error reply sent before the request
        # could be parsed reaches no waiter.
        request_id = 0

        try:
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
            # address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes)
            address_bytes, handler_name, rest = decrypted.split(b"<", maxsplit=2)

            # Extract clock (first 64 bytes)
            clock_time = int.from_bytes(rest[:64])
            # Extract the request's id, which its reply echoes (next 8 bytes)
            request_id = int.from_bytes(rest[64:72], "big")
            # Extract data length (next 4 bytes)
            data_len = int.from_bytes(rest[72:76], "big")
            # Extract payload (remaining bytes)
            payload = rest[76 : 76 + data_len]

            next_time = await self._tcp_clock.update(clock_time)

            if peername is not None and (
                rate_limited := await self._admit_tcp_request(peername, handler_name)
            ) is not None:
                # Answer instead of dropping: a silent drop left the sender
                # to time out, and its timeout closes the connection it
                # shares with every other in-flight request to this node.
                self._write_tcp_response(
                    transport,
                    handler_name,
                    next_time,
                    request_id,
                    rate_limited.dump(),
                )
                return

            try:
                addr = parse_address(address_bytes)
            except AddressValidationError as e:
                await self._log_security_warning(
                    f"TCP server request malformed address: {e}",
                    protocol="tcp",
                )
                return

            self._tcp_server_request_transports[addr] = transport

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
                # Answer through the sanitized error path below instead of
                # dropping: a silent drop left the sender waiting out its
                # whole timeout (and retry budget) for a request this node
                # can never serve — e.g. registering with the wrong role.
                raise LookupError(f"no TCP handler named {handler_name!r}")

            # The @tcp.receive() wrapper signature is (server, addr, data,
            # clock_time) — passing `transport` as a 4th positional arg
            # raises TypeError, which the generic `except Exception` below
            # silently catches and turns into an opaque error response.
            # No handler currently reads transport; if certificate
            # extraction (AD-28) is needed in future, extend the wrapper
            # to thread transport through explicitly.
            response = await handler(
                addr,
                payload,
                clock_time,
            )

            if isinstance(response, Message):
                response = response.dump()

            if handler_name == b"":
                handler_name = b"error"

            self._write_tcp_response(transport, handler_name, next_time, request_id, response)

        except Exception as e:
            self._tcp_drop_counter.increment_malformed_message()
            # Log security event - could be decryption failure, malformed message, etc.
            await self._log_security_warning(
                f"TCP server request failed: {type(e).__name__}: {e}",
                protocol="tcp",
            )
            # Sanitized error response - don't leak internal details
            try:
                error_time = await self._tcp_clock.tick()
                error_msg = b"Request processing failed"
                error_len = len(error_msg).to_bytes(4, "big")
                error_response = self._encryptor.encrypt(
                    self._compressor.compress(
                        self._tcp_addr_slug
                        + b"<"
                        + handler_name
                        + b"<"
                        + error_time.to_bytes(64)
                        + request_id.to_bytes(8, "big")
                        + error_len
                        + error_msg,
                    )
                )
                # Frame with length prefix for proper TCP stream handling
                transport.write(frame_message(error_response))
            except Exception as reply_error:
                # The sender waits out its timeout: say why it got no answer.
                await self._log_security_warning(
                    f"TCP error reply for {handler_name!r} failed: "
                    f"{type(reply_error).__name__}: {reply_error}",
                    protocol="tcp",
                )

    async def process_udp_server_request(
        self,
        handler_name: bytes,
        addr: bytes,
        payload: bytes,
        clock_time: int,
        request_id: int,
        transport: asyncio.DatagramTransport,
    ):
        if handler_name == b"receive":

            self._udp_recv_arrived_count += 1
            if self._udp_recv_arrived_count % 100 == 0:
                tracker = self._udp_in_flight_tracker
                await self._udp_logger.log(
                    ServerError(
                        message=(
                            f"[UDP-PROCESS-ARRIVED] handler=receive "
                            f"total={self._udp_recv_arrived_count} "
                            f"in_flight={dict(tracker._counts)} "
                            f"group_in_flight={dict(tracker._group_counts)} "
                            f"acquired_total={dict(tracker._acquired_total)} "
                            f"group_acquired_total="
                            f"{dict(tracker._group_acquired_total)} "
                            f"group_shed_total={dict(tracker._group_shed_total)}"
                        ),
                        node_host=self._udp_host,
                        node_port=self._udp_port,
                        node_id=str(self._udp_port),
                    )
                )

        # Terminal-abort barrier: a UDP response task that was spawned
        # before ``abort()`` fired must not continue to run handlers or
        # emit ``sendto`` after abort. Otherwise a hard-killed instance
        # keeps responding to in-flight probes/suspicions long enough to
        # refute its own death via the captured transport reference (see
        # ``abort()``'s docstring on the kill→restart race).
        if not self._running:
            return

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
                        if self._udp_drop_counter.replay_detected % 10 == 0:
                            await self._udp_logger.log(
                                ServerError(
                                    message=(
                                        f"[UDP-REPLAY-DROP] handler={handler_name!r} "
                                        f"total={self._udp_drop_counter.replay_detected}"
                                    ),
                                    node_host=self._udp_host,
                                    node_port=self._udp_port,
                                    node_id=str(self._udp_port),
                                )
                            )
                        return

            handler = self.udp_handlers[handler_name]
            response = await handler(
                parsed_addr,
                payload,
                clock_time,
            )

            if isinstance(response, Message):
                response = response.dump()

            # Final terminal-abort barrier: even if the handler completed
            # right before ``abort()`` flipped ``_running``, do not emit
            # any wire bytes from a stopping instance. The captured
            # ``transport`` argument is still alive (closed but the
            # ``sendto`` call doesn't error synchronously), so without
            # this check a half-killed worker can still send ALIVE /
            # refutation responses that the manager treats as authoritative.
            if not self._running:
                return

            # UDP response with clock and the request's id before length-prefixed data
            # Format: type<address<handler<clock(64 bytes)request_id(8 bytes)data_len(4 bytes)data(N bytes)
            response_len = len(response).to_bytes(4, "big")
            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    b"s<"
                    + self._udp_addr_slug
                    + b"<"
                    + handler_name
                    + b"<"
                    + next_time.to_bytes(64)
                    + request_id.to_bytes(8, "big")
                    + response_len
                    + response,
                )
            )

            # Reply to the address the requester declared, a DNS name
            # resolved to its IP in REAL mode.
            transport.sendto(
                response_payload,
                parsed_addr
                if self._host_address_resolver is None
                else await self._host_address_resolver.resolve(parsed_addr),
            )

        except Exception as e:
            # Log security event - don't leak internal details
            await self._log_security_warning(
                f"UDP server request failed: {type(e).__name__}",
                protocol="udp",
            )

            if not self._running:
                return

            # Sanitized error response
            error_msg = b"Request processing failed"
            error_len = len(error_msg).to_bytes(4, "big")
            response_payload = self._encryptor.encrypt(
                self._compressor.compress(
                    b"s<"
                    + self._udp_addr_slug
                    + b"<"
                    + handler_name
                    + b"<"
                    + next_time.to_bytes(64)
                    + request_id.to_bytes(8, "big")
                    + error_len
                    + error_msg,
                )
            )

            try:
                transport.sendto(
                    response_payload,
                    parsed_addr
                    if self._host_address_resolver is None
                    else await self._host_address_resolver.resolve(parsed_addr),
                )

            except DNSError as resolution_error:
                await self._log_security_warning(
                    f"UDP error reply dropped: {resolution_error}",
                    protocol="udp",
                )

    async def process_udp_client_response(
        self,
        handler_name: bytes,
        addr: bytes,
        payload: bytes,
        clock_time: int,
        request_id: int,
        _: asyncio.DatagramTransport,
    ):
        if not self._running:
            return
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

        except Exception as err:
            payload = err

        # Only the request this reply names takes it: one that gave up,
        # or never was, finds no waiter.
        if (waiter := self._udp_request_waiters.pop(request_id, None)) is not None and not waiter.done():
            waiter.set_result((payload, clock_time))

    async def _cleanup_tcp_server_tasks(self):
        loop = asyncio.get_running_loop()
        while self._running:
            self._tcp_server_sleep_task = loop.create_future()

            try:
                await self._clock.wait_for(
                    self._tcp_server_sleep_task,
                    timeout=self._cleanup_interval,
                )

            except asyncio.TimeoutError:
                # Normal cycle — sleep elapsed, fall through to cleanup.
                pass

            # A finished task's outcome was taken by its done callback;
            # only unfinished tasks stay tracked for the shutdown drain.
            self._pending_tcp_server_responses = deque(
                pending
                for pending in self._pending_tcp_server_responses
                if not pending.done()
            )

    async def _cleanup_udp_server_tasks(self):
        loop = asyncio.get_running_loop()
        while self._running:
            self._udp_server_sleep_task = loop.create_future()

            try:
                await self._clock.wait_for(
                    self._udp_server_sleep_task,
                    timeout=self._cleanup_interval,
                )

            except asyncio.TimeoutError:
                # Normal cycle — sleep elapsed, no per-iteration cleanup
                # for the UDP variant.
                pass

            # A finished task's outcome was taken by its done callback;
            # only unfinished tasks stay tracked for the shutdown drain.
            self._pending_udp_server_responses = deque(
                pending
                for pending in self._pending_udp_server_responses
                if not pending.done()
            )

    async def _log_drop_stats_periodically(self) -> None:
        """Periodically log silent drop statistics for security monitoring."""
        while self._running:
            try:
                await self._clock.sleep(self._drop_stats_interval)
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
                            replay_detected_count=tcp_snapshot.replay_detected,
                            load_shed_count=tcp_snapshot.load_shed,
                            log_write_failed_count=tcp_snapshot.log_write_failed,
                            total_dropped=tcp_snapshot.total,
                            interval_seconds=tcp_snapshot.interval_seconds,
                        )
                    )
                except Exception:
                    # The report did not reach the log: its drops go back to
                    # the counter for the next report, with the failed write
                    # (itself a lost record) counted beside them.
                    self._tcp_drop_counter.rate_limited += tcp_snapshot.rate_limited
                    self._tcp_drop_counter.message_too_large += tcp_snapshot.message_too_large
                    self._tcp_drop_counter.decompression_too_large += tcp_snapshot.decompression_too_large
                    self._tcp_drop_counter.decryption_failed += tcp_snapshot.decryption_failed
                    self._tcp_drop_counter.malformed_message += tcp_snapshot.malformed_message
                    self._tcp_drop_counter.replay_detected += tcp_snapshot.replay_detected
                    self._tcp_drop_counter.load_shed += tcp_snapshot.load_shed
                    self._tcp_drop_counter.log_write_failed += tcp_snapshot.log_write_failed + 1

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
                            replay_detected_count=udp_snapshot.replay_detected,
                            load_shed_count=udp_snapshot.load_shed,
                            log_write_failed_count=udp_snapshot.log_write_failed,
                            total_dropped=udp_snapshot.total,
                            interval_seconds=udp_snapshot.interval_seconds,
                        )
                    )
                except Exception:
                    # As for TCP: the drops go back for the next report.
                    self._udp_drop_counter.rate_limited += udp_snapshot.rate_limited
                    self._udp_drop_counter.message_too_large += udp_snapshot.message_too_large
                    self._udp_drop_counter.decompression_too_large += udp_snapshot.decompression_too_large
                    self._udp_drop_counter.decryption_failed += udp_snapshot.decryption_failed
                    self._udp_drop_counter.malformed_message += udp_snapshot.malformed_message
                    self._udp_drop_counter.replay_detected += udp_snapshot.replay_detected
                    self._udp_drop_counter.load_shed += udp_snapshot.load_shed
                    self._udp_drop_counter.log_write_failed += udp_snapshot.log_write_failed + 1

    def _wake_cleanup_loops(self) -> None:
        """Wake cleanup loops so they observe ``_running == False`` and exit."""
        for sleep_future in (
            self._tcp_server_sleep_task,
            self._udp_server_sleep_task,
        ):
            if sleep_future is not None and not sleep_future.done():
                sleep_future.set_result(None)

        # The drop-stats loop sleeps a full stats interval on the clock
        # and has no wake future, so the quiescence barrier used to wait
        # out its entire drain budget (measured: every shutdown after any
        # traffic took exactly drain_timeout). Cancellation IS its exit
        # path — it breaks on CancelledError mid-sleep and logs nothing
        # on the way out — so cancelling here equals waking it.
        if self._drop_stats_task is not None and not self._drop_stats_task.done():
            self._drop_stats_task.cancel()

    def _close_tcp_transports(self) -> None:
        """Close every TCP transport owned by this server."""
        transport_maps = (
            self._tcp_client_transports,
            self._tcp_client_response_transports,
            self._tcp_server_request_transports,
        )
        for transport_map in transport_maps:
            for transport in list(transport_map.values()):
                if not transport.is_closing():
                    transport.close()
            transport_map.clear()

        for transport in self._retired_tcp_client_transports:
            if not transport.is_closing():
                transport.close()
        self._retired_tcp_client_transports.clear()

        if self._tcp_transport is not None and not self._tcp_transport.is_closing():
            self._tcp_transport.close()
        self._tcp_transport = None

    def _close_udp_transport(self) -> None:
        """Close the UDP transport and underlying socket."""
        if self._udp_transport is not None:
            if not self._udp_transport.is_closing():
                self._udp_transport.close()
            self._udp_transport = None
            self._udp_connected = False

        if self._udp_server_socket is not None:
            try:
                self._udp_server_socket.close()
            except OSError:
                pass
            self._udp_server_socket = None

    async def _close_tcp_server(self) -> None:
        """Close the TCP listener, every connection it accepted, and the
        underlying socket."""
        if self._transport_factory is not None:
            self._transport_factory.close_stream_server((self._host, self._tcp_port))
        for accepted_connection in list(self._tcp_server_state.connections):
            accepted_connection.transport.abort()
        if self._tcp_server is not None:
            self._tcp_server.close()
            try:
                await self._tcp_server.wait_closed()
            except OSError:
                pass
            self._tcp_server = None
            self._tcp_connected = False

        if self._tcp_server_socket is not None:
            try:
                self._tcp_server_socket.close()
            except OSError:
                pass
            self._tcp_server_socket = None

    def _owned_shutdown_tasks(self) -> list[asyncio.Task | asyncio.Future]:
        """Return server-owned futures/tasks that must be terminal at teardown."""
        return [
            task
            for task in (
                [
                    self._drop_stats_task,
                    self._tcp_server_sleep_task,
                    self._tcp_server_cleanup_task,
                    self._udp_server_sleep_task,
                    self._udp_server_cleanup_task,
                ]
                + list(self._pending_tcp_server_responses)
                + list(self._pending_udp_server_responses)
            )
            if task is not None
        ]

    async def _await_owned_shutdown_tasks(
        self,
        tasks: list[asyncio.Task | asyncio.Future],
        drain_timeout: float,
    ) -> int:
        """Wait for server-owned tasks, then cancel any survivors."""
        pending = [task for task in tasks if not task.done()]
        if pending and drain_timeout > 0:
            _done, pending_set = await asyncio.wait(
                pending,
                timeout=drain_timeout,
            )
            pending = list(pending_set)

        for task in pending:
            if not task.done():
                task.cancel()

        if pending:
            await asyncio.gather(
                *(cancel(task) for task in pending),
                return_exceptions=True,
            )

        self._pending_tcp_server_responses.clear()
        self._pending_udp_server_responses.clear()
        return sum(1 for task in pending if not task.done())

    async def _shutdown_task_runner(self, drain_timeout: float) -> bool:
        """Shutdown the TaskRunner within the teardown budget."""
        if self._task_runner is None:
            return True

        try:
            if drain_timeout > 0:
                await self._clock.wait_for(
                    self._task_runner.shutdown(),
                    timeout=drain_timeout,
                )
            else:
                await self._task_runner.shutdown()
            return True
        except asyncio.TimeoutError:
            self._task_runner.abort()
            return False

    def _count_unclosed_transports(self) -> int:
        """Return count of transports still not closing after teardown."""
        transports: list[asyncio.Transport] = []
        transports.extend(self._tcp_client_transports.values())
        transports.extend(self._retired_tcp_client_transports)
        transports.extend(self._tcp_client_response_transports.values())
        transports.extend(self._tcp_server_request_transports.values())
        if self._tcp_transport is not None:
            transports.append(self._tcp_transport)
        if self._udp_transport is not None:
            transports.append(self._udp_transport)
        return sum(1 for transport in transports if not transport.is_closing())

    async def _await_quiescent(self, drain_timeout: float = 5.0) -> None:
        """Block until server-owned transports and tasks are quiescent.

        The method is the common stop barrier for every node type. It is
        bounded by ``drain_timeout``; if cooperative TaskRunner shutdown
        exceeds that budget we abort the runner, continue transport/task
        cleanup, and emit a structured warning rather than letting
        teardown hang indefinitely.
        """
        effective_timeout = drain_timeout if drain_timeout > 0 else 5.0

        self._close_tcp_transports()
        self._close_udp_transport()
        await self._close_tcp_server()

        task_runner_clean = await self._shutdown_task_runner(effective_timeout)
        unfinished_tasks = await self._await_owned_shutdown_tasks(
            self._owned_shutdown_tasks(),
            effective_timeout,
        )
        unclosed_transports = self._count_unclosed_transports()

        if task_runner_clean and unfinished_tasks == 0 and unclosed_transports == 0:
            return

        await self._udp_logger.log(
            ServerWarning(
                message=(
                    "Server shutdown quiescence incomplete: "
                    f"task_runner_clean={task_runner_clean} "
                    f"unfinished_tasks={unfinished_tasks} "
                    f"unclosed_transports={unclosed_transports}"
                ),
                node_host=self._host,
                node_port=self._udp_port,
                node_id=str(self._udp_port),
            )
        )

    async def wait(self) -> None:
        """Block until the server has stopped (``shutdown`` or ``abort``)."""
        await self._stopped.wait()

    async def shutdown(self, drain_timeout: float = 5.0) -> None:
        self._running = False

        # Cooperative wakeup for cleanup loops; the quiescence barrier
        # below owns the bounded drain/cancel/transport-close contract.
        self._wake_cleanup_loops()
        await self._await_quiescent(drain_timeout=drain_timeout)
        self._stopped.set()

    def abort(self) -> None:
        self._running = False

        # Same cooperative-wakeup as ``shutdown`` — resolve the
        # cleanup-loop futures so they exit on ``_running == False``
        # rather than via cancellation, which the loops do not honor.
        for sleep_future in (
            self._tcp_server_sleep_task,
            self._udp_server_sleep_task,
        ):
            if sleep_future is not None and not sleep_future.done():
                sleep_future.set_result(None)

        self._task_runner.abort()

        # Close UDP transport to stop receiving datagrams. Transport
        # close is deferred (scheduled via ``loop.call_soon``); since
        # ``abort`` is synchronous we cannot ``await`` it. Close the
        # underlying socket directly afterwards so the OS releases
        # the bound UDP port immediately — without this, a fast
        # kill→restart cycle (the simulation harness's FaultMatrix)
        # races the deferred close and ``restart`` fails with
        # ``OSError: [Errno 48] Address already in use``.
        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None
            self._udp_connected = False
        # Catch ``OSError`` only (covers EBADF / EINTR from a
        # double-close after the transport's deferred close already
        # ran). Anything else is unexpected and would be silenced if
        # we caught the broad ``Exception`` — preserve the contract
        # that we never swallow non-cleanup-related failures here.
        if self._udp_server_socket is not None:
            try:
                self._udp_server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._udp_server_socket.close()
            except OSError:
                pass
            self._udp_server_socket = None

        # The connections this node accepted die with it: a listener's
        # close leaves them open, answering for a node that is gone.
        if self._transport_factory is not None:
            self._transport_factory.close_stream_server((self._host, self._tcp_port))
        for accepted_connection in list(self._tcp_server_state.connections):
            accepted_connection.transport.abort()

        # Close TCP server (and its underlying socket — see UDP
        # comment above for the same kill→restart race rationale).
        # ``socket.shutdown(SHUT_RDWR)`` BEFORE ``close()`` is
        # load-bearing for the simulation harness's tight
        # kill→restart cycle: under ``SO_REUSEADDR`` macOS will
        # happily let a fresh socket bind to the same port even
        # while the previous listener still has the descriptor
        # open, and new connections can route to either socket
        # — usually the older one, because its accept queue is
        # already warm. Calling ``shutdown`` first invalidates
        # the kernel-side accept queue immediately so subsequent
        # connections cannot land on the dying instance regardless
        # of when asyncio gets around to running the deferred
        # ``Server.close`` cleanup. Without this, a restart at the
        # same port silently inherits requests for the dead
        # process's identity for many seconds.
        if self._tcp_server is not None:
            self._tcp_server.close()
            self._tcp_server = None
            self._tcp_connected = False
        if self._tcp_server_socket is not None:
            try:
                self._tcp_server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._tcp_server_socket.close()
            except OSError:
                pass
            self._tcp_server_socket = None

        # Close all TCP client transports, retired ones included
        for client in [*self._tcp_client_transports.values(), *self._retired_tcp_client_transports]:
            try:
                client.abort()
            except Exception:
                pass
        self._tcp_client_transports.clear()
        self._retired_tcp_client_transports.clear()

        cancel_and_release_task(self._drop_stats_task)
        cancel_and_release_task(self._tcp_server_sleep_task)
        cancel_and_release_task(self._tcp_server_cleanup_task)
        cancel_and_release_task(self._udp_server_sleep_task)
        cancel_and_release_task(self._udp_server_cleanup_task)

        # Pre-cancel pending UDP/TCP response tasks here in the sync
        # path. ``cancel`` only schedules cancellation; the corresponding
        # await happens in ``abort_and_wait``. Doing the schedule in the
        # sync ``abort()`` keeps the behavior layered: tests/code calling
        # the historical sync entry point still get the same effect on
        # the event loop's next tick.
        for pending in list(self._pending_udp_server_responses):
            if not pending.done():
                pending.cancel()
        for pending in list(self._pending_tcp_server_responses):
            if not pending.done():
                pending.cancel()

        self._stopped.set()

    async def abort_and_wait(
        self,
        timeout: int | None = None,
    ):
        if timeout:
            try:
                await self._clock.wait_for(
                    self._abort_and_wait(),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                # The abort itself ran (transports closed, loops
                # cancelled); only waiting out the cancelled handlers
                # overran the deadline.
                await self._udp_logger.log(
                    ServerWarning(
                        message=(
                            f"Abort did not finish within {timeout}s: some "
                            "cancelled handlers were still unwinding"
                        ),
                        node_host=self._host,
                        node_port=self._udp_port,
                        node_id=0,
                    )
                )

            return

        await self._abort_and_wait()

    async def _abort_and_wait(self) -> None:
        """Awaitable terminal abort — guarantees a dark instance on return.

        Calls the synchronous ``abort()`` (which flips ``_running``,
        closes transports, cancels named loops, and schedules
        cancellation of pending response tasks) then *awaits* the
        cancellations so no in-flight UDP/TCP handler can still emit
        wire bytes after this returns. Two yields to the loop ensure
        any handler currently in its ``except`` branch reaches the
        post-cancel ``_running`` guard rather than racing through a
        late ``transport.sendto``.

        Use this in place of ``abort()`` + ``asyncio.sleep(0.05)`` from
        any test harness that simulates a hard kill — without it, the
        killed instance can still respond to SWIM probes/suspicions for
        seconds after the test thinks it is dead, which lets the dying
        node refute its own death via captured transport references.
        """
        self.abort()

        pending = (
            list(self._pending_udp_server_responses)
            + list(self._pending_tcp_server_responses)
        )
        if pending:
            await asyncio.gather(
                *(cancel(task) for task in pending),
                return_exceptions=True,
            )
        self._pending_udp_server_responses.clear()
        self._pending_tcp_server_responses.clear()

        # Two loop yields: first lets any cancelled task's ``except
        # CancelledError`` branch run; second lets the cancellation
        # propagate into nested awaits inside SWIM handlers (refutation
        # broadcasts, etc.).
        await self._clock.sleep(0)
        await self._clock.sleep(0)
        await self._await_quiescent()
