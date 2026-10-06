from __future__ import annotations

import asyncio

from hyperscale.core.jobs.protocols.node_id_derivation import (
    derive_protocol_node_id,
)
import inspect
import pickle
import signal
import socket
from collections import defaultdict, deque
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Generic,
    Literal,
    Tuple,
    TypeVar,
    Union,
)

import cloudpickle
import zstandard

from hyperscale.core.runtime import TransportFactory

from .constants import MAX_DECOMPRESSED_SIZE
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.engines.client.udp.protocols.dtls import do_patch
from hyperscale.core.jobs.data_structures import LockedSet
from hyperscale.core.jobs.hooks.hook_type import HookType
from hyperscale.core.jobs.models import Env, JobContext, Message
from hyperscale.core.jobs.tasks import TaskRunner
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerFatal,
    ServerInfo,
    ServerTrace,
)

from .encryption import AESGCMFernet, EncryptionError
from .message_limits import (
    validate_compressed_size,
    validate_decompressed_size,
    MessageSizeError,
)
from .replay_guard import ReplayGuard, ReplayError
from .restricted_unpickler import restricted_loads, SecurityError
from .udp_socket_protocol import UDPSocketProtocol

do_patch()


T = TypeVar("T")
K = TypeVar("K")


class UDPProtocol(Generic[T, K]):
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        self._node_id_base = derive_protocol_node_id(host, port)
        self.node_id: int | None = None

        # Phase 6 SIM seam. ``transport_factory`` is ``None`` in REAL
        # mode — this server binds a real UDP socket and installs signal
        # handlers exactly as before. Under SIM a transport factory (the
        # multi-process ``CrossProcessTransport`` for a worker-pool
        # executor, or the single-process ``InProcessTransport``) routes
        # the datagram endpoint with no socket, no signal handlers, no
        # ``run_in_executor``; ``loop`` pins the server to the caller's
        # ``SimulationLoop``. Multi-process is preserved: each executor
        # still runs in its own OS process — the factory only replaces
        # the kernel-socket byte transit with the deterministic
        # coordinator boundary.
        self._transport_factory = transport_factory

        self._logger = Logger()

        self.id_generator: SnowflakeGenerator | None = None

        self.env = env

        self.host = host
        self.port = port

        self._events: Dict[str, Coroutine] = {}

        self.tasks: TaskRunner | None = None
        self.connected = False
        self._running = False

        self._transport: asyncio.DatagramTransport = None
        # Under SIM the loop is injected so lazy ``get_event_loop`` never
        # resolves the wrong (non-simulation) loop; REAL leaves it None
        # and resolves lazily exactly as before.
        self._loop: Union[asyncio.AbstractEventLoop, None] = loop
        self._waiters: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        # One waiter per send() in flight, by the id its reply echoes.
        self._request_waiters: Dict[int, asyncio.Future] = {}
        self._pending_responses: Deque[asyncio.Task] = deque()

        self._sent_values = deque()

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time

        self._encryptor = AESGCMFernet(env)
        self._semaphore: Union[asyncio.Semaphore, None] = None
        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None

        self._cleanup_task: Union[asyncio.Task, None] = None
        self._sleep_task: Union[asyncio.Task, None] = None
        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time
        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self.udp_socket: Union[socket.socket, None] = None

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time
        self._connect_timeout = TimeParser(env.MERCURY_SYNC_CONNECT_TIMEOUT).time
        self._retry_interval = TimeParser(env.MERCURY_SYNC_RETRY_INTERVAL).time
        self._shutdown_poll_rate = TimeParser(env.MERCURY_SYNC_SHUTDOWN_POLL_RATE).time
        self._max_connect_time = TimeParser(env.MERCURY_SYNC_MAX_CONNECT_TIME).time
        self._retries = env.MERCURY_SYNC_SEND_RETRIES

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_CONNECT_RETRIES
        self._run_future: asyncio.Future = None
        self._node_host_map: Dict[int, Tuple[str, int]] = {}
        self._nodes: LockedSet[int] | None = None
        self._abort_handle_created: bool = None
        self._connect_lock: asyncio.Lock | None = None
        self._shutdown_task: asyncio.Future | None = None
        
        # Replay attack protection
        self._replay_guard = ReplayGuard(
            max_age_seconds=300,  # 5 minutes
            max_future_seconds=60,  # 1 minute clock skew tolerance
            max_window_size=100000,
        )

    @property
    def nodes(self):
        return [node_id for node_id in self._node_host_map]

    def node_at(self, idx: int):
        return self.nodes[idx]

    def __iter__(self):
        for node_id in self._node_host_map:
            yield node_id

    async def run_forever(self):
        try:
            self._run_future = self._loop.create_future()
            await self._run_future

        except asyncio.CancelledError:
            pass

    async def connect_client(
        self,
        logfile: str,
        address: tuple[str, int],
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket.socket | None = None,
        worker_server: asyncio.DatagramTransport | None = None,
    ):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        # Signal handlers are process-global and need a real loop
        # selector — skip under SIM (see start_server).
        if self._transport_factory is None and not self._abort_handle_created:
            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

            self._abort_handle_created = True

        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)

        if self.node_id is None:
            self.node_id = derive_protocol_node_id(self.host, self.port)

        if self.id_generator is None:
            self.id_generator = SnowflakeGenerator(self.node_id)

        if self._nodes is None:
            self._nodes: LockedSet[int] = LockedSet()

        if self._compressor is None:
            self._compressor = zstandard.ZstdCompressor()

        if self._decompressor is None:
            self._decompressor = zstandard.ZstdDecompressor()

        instance_id: int | None = None
        # Loop time, not wall time: identical on a real loop (loop.time
        # IS the monotonic clock) and virtual under SIM, so the connect
        # budget follows the simulated timeline instead of host load.
        start_time = self._loop.time()
        attempt = 0

        # Connect retry with exponential backoff
        # Start with short timeout/interval, increase as processes may be slow to start
        base_timeout = 2.0  # Initial per-attempt timeout
        base_interval = 0.5  # Initial retry interval
        max_timeout = 10.0  # Cap per-attempt timeout
        max_interval = 5.0  # Cap retry interval

        while True:
            elapsed = self._loop.time() - start_time
            if elapsed >= self._max_connect_time:
                raise TimeoutError(
                    f"Failed to connect to {address} after {self._max_connect_time}s ({attempt} attempts)"
                )

            if self._transport is None:
                await self.start_server(
                    cert_path=cert_path,
                    key_path=key_path,
                    worker_socket=worker_socket,
                    worker_server=worker_server,
                )

            # Calculate timeouts with exponential backoff, capped at max values
            attempt_timeout = min(base_timeout * (1.5 ** min(attempt, 5)), max_timeout)
            retry_interval = min(base_interval * (1.5 ** min(attempt, 5)), max_interval)

            # The attempt's own name, which the peer echoes in its reply
            # (every node version does): it routes that reply, and only that
            # reply, to this attempt. A late reply to an earlier attempt, or
            # another node's, carries another name.
            connect_name = f"connect:{self.id_generator.generate()}"

            try:
                try:
                    result: Tuple[int, Message[None]] = await asyncio.wait_for(
                        self.send(
                            connect_name,
                            None,
                            target_address=address,
                            request_type="connect",
                        ),
                        timeout=attempt_timeout,
                    )

                finally:
                    # The name served this one attempt: its waiters go with it.
                    self._waiters.pop(connect_name, None)

                shard_id, response = result

                # Use full 64-bit node_id from message instead of 10-bit snowflake instance
                instance_id = response.node_id

                # The responder's own address, not the one dialed: every
                # concurrent connect waits on one reply queue, so this reply
                # can answer another call's request, and recording it against
                # the address dialed here would swap two nodes' addresses.
                self._node_host_map[instance_id] = (
                    response.service_host,
                    response.service_port,
                )
                self._nodes.put_no_wait(instance_id)

                # Successfully connected
                break

            except (Exception, asyncio.CancelledError, socket.error, OSError):
                attempt += 1
                # Don't sleep if we've exceeded the max time
                remaining = self._max_connect_time - (self._loop.time() - start_time)
                if remaining > 0:
                    await asyncio.sleep(min(retry_interval, remaining))

        default_config = {
            "node_id": self._node_id_base,
            "node_host": self.host,
            "node_port": self.port,
        }

        self._logger.configure(
            name=f"graph_client_{self._node_id_base}",
            path=logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (ServerTrace, default_config),
                "debug": (
                    ServerDebug,
                    default_config,
                ),
                "info": (
                    ServerInfo,
                    default_config,
                ),
                "error": (
                    ServerError,
                    default_config,
                ),
                "fatal": (
                    ServerFatal,
                    default_config,
                ),
            },
        )

        return instance_id

    async def start_server(
        self,
        logfile: str,
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket.socket | None = None,
        worker_server: asyncio.DatagramTransport | None = None,
    ) -> None:
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        # Signal handlers are process-global and rely on a real loop
        # selector — banned and meaningless under SIM (the coordinator,
        # not signals, drives shutdown). Skip when running under a
        # transport factory.
        if self._transport_factory is None and not self._abort_handle_created:
            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

            self._abort_handle_created = True

        self._events: Dict[str, Callable[[int, T], Awaitable[K]]] = {
            name: receive_hook
            for name, receive_hook in inspect.getmembers(
                self,
                predicate=lambda member: hasattr(
                    member,
                    "hook_type",
                )
                and getattr(member, "hook_type") == HookType.RECEIVE,
            )
        }

        self._events.update(
            {
                name: receive_hook
                for name, receive_hook in inspect.getmembers(
                    self,
                    predicate=lambda member: hasattr(
                        member,
                        "hook_type",
                    )
                    and getattr(member, "hook_type") == HookType.BROADCAST,
                )
            }
        )

        self._running = True

        if self._nodes is None:
            self._nodes: LockedSet[int] = LockedSet()

        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()

        if self.id_generator is None:
            self.id_generator = SnowflakeGenerator(self._node_id_base)

        if self.node_id is None:
            # Use full 64-bit UUID to avoid collisions (10-bit snowflake instance is too small)
            self.node_id = self._node_id_base

        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)

        if self._compressor is None:
            self._compressor = zstandard.ZstdCompressor()

        if self._decompressor is None:
            self._decompressor = zstandard.ZstdDecompressor()

        if self.tasks is None:
            self.tasks = TaskRunner(
                self.id_generator.generate(),
                self.env,
            )

            tasks = {
                name: receive_hook
                for name, receive_hook in inspect.getmembers(
                    self,
                    predicate=lambda member: hasattr(
                        member,
                        "hook_type",
                    )
                    and getattr(member, "hook_type") == HookType.TASK,
                )
            }

            for task in tasks.values():
                self.tasks.add(task)

        # Datagrams carry no TLS -- Python's ssl has no DTLS -- and are
        # encrypted at the message layer (AES-GCM) instead; ``cert_path``
        # and ``key_path`` configure the TCP side of a node.
        if self._transport_factory is not None:
            # SIM: register the datagram endpoint with the transport
            # factory (cross-process coordinator boundary, or in-process
            # registry) instead of binding a real UDP socket.
            # ``register_datagram_endpoint`` fires ``connection_made`` and
            # returns the send transport. No socket, no ``run_in_executor``
            # bind, no fileno cleanup loop (that cleanup polls a real
            # socket, which does not exist here).
            self._transport = self._transport_factory.register_datagram_endpoint(
                (self.host, self.port), UDPSocketProtocol(self.read)
            )
            self.connected = True
            return

        run_start = True

        while run_start:
            try:
                if self.connected is False and worker_socket is None:
                    self.udp_socket = socket.socket(
                        socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
                    )
                    self.udp_socket.setsockopt(
                        socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
                    )

                    # Increase socket buffer sizes to reduce EAGAIN errors under load
                    # Default is typically 212992 bytes, we increase to 4MB
                    try:
                        self.udp_socket.setsockopt(
                            socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024
                        )
                        self.udp_socket.setsockopt(
                            socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024
                        )
                    except (OSError, socket.error):
                        # Some systems may not allow large buffers, ignore
                        pass

                    await self._loop.run_in_executor(
                        None, self.udp_socket.bind, (self.host, self.port)
                    )

                    self.udp_socket.setblocking(False)

                elif self.connected is False and worker_socket:
                    self.udp_socket = worker_socket
                    host, port = worker_socket.getsockname()
                    self.host = host
                    self.port = port

                elif self.connected is False:
                    self._transport = worker_server

                    address_info: Tuple[str, int] = self._transport.get_extra_info(
                        "sockname"
                    )
                    self.udp_socket: socket.socket = self._transport.get_extra_info(
                        "socket"
                    )

                    host, port = address_info
                    self.host = host
                    self.port = port

                    run_start = False
                    self.connected = True

                    self._cleanup_task = self._loop.create_task(self._cleanup())

                if self.connected is False and worker_server is None:
                    server = self._loop.create_datagram_endpoint(
                        lambda: UDPSocketProtocol(self.read), sock=self.udp_socket
                    )

                    transport, _ = await server

                    self._transport = transport
                    self._cleanup_task = self._loop.create_task(self._cleanup())

                    run_start = False
                    self.connected = True

            except (Exception, socket.error, OSError):
                # Only a failed attempt waits before retrying; cancellation
                # is not a failed attempt and propagates.
                await asyncio.sleep(self._retry_interval)

        default_config = {
            "node_id": self._node_id_base,
            "node_host": self.host,
            "node_port": self.port,
        }

        self._logger.configure(
            name=f"graph_server_{self._node_id_base}",
            path=logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (ServerTrace, default_config),
                "debug": (
                    ServerDebug,
                    default_config,
                ),
                "info": (
                    ServerInfo,
                    default_config,
                ),
                "error": (
                    ServerError,
                    default_config,
                ),
                "fatal": (
                    ServerFatal,
                    default_config,
                ),
            },
        )

        self._start_tasks()

    def _start_tasks(self):
        self.tasks.start_cleanup()
        for task in self.tasks.all_tasks():
            task.call = task.call.__get__(self, self.__class__)
            setattr(self, task.name, task.call)

            if task.trigger == "ON_START":
                self.tasks.run(task.name)

    async def _cleanup(self):
        while self._running:
            self._sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            await self._sleep_task

            # Keep the futures still running; let go of the finished ones --
            # exactly those (popping from the right, as this did, dropped a
            # running future and kept a finished one) -- and log the
            # failures they hold rather than discarding them.
            finished = [pending for pending in self._pending_responses if pending.done()]
            if not finished:
                continue

            self._pending_responses = deque(
                pending for pending in self._pending_responses if not pending.done()
            )
            for pending in finished:
                if pending.cancelled() or (pending_error := pending.exception()) is None:
                    continue

                async with self._logger.context(
                    name=f"graph_server_{self._node_id_base}"
                ) as ctx:
                    await ctx.log_prepared(
                        message=(
                            f"Node {self._node_id_base} at {self.host}:{self.port} failed "
                            f"handling a message ({type(pending_error).__name__}): {pending_error}"
                        ),
                        name="error",
                    )

    async def _sendto_with_retry(
        self,
        data: bytes,
        address: Tuple[str, int],
    ) -> None:
        """Send data with retry on EAGAIN/EWOULDBLOCK (socket buffer full)."""
        for send_attempt in range(self._retries + 1):
            try:
                self._transport.sendto(data, address)
                return
            except BlockingIOError:
                # Socket buffer full, use exponential backoff: 10ms, 20ms, 40ms, 80ms...
                if send_attempt < self._retries:
                    await asyncio.sleep(0.01 * (2 ** send_attempt))
                else:
                    # All retries exhausted, let it propagate
                    raise

    async def send(
        self,
        target: str,
        data: T,
        node_id: int | None = None,
        target_address: Tuple[str, int] | None = None,
        request_type: Literal["request", "connect"] | None = None,
    ) -> Tuple[int, K]:
        if request_type == "connect":
            # A connect goes to the address dialed, never to a node already
            # known; its reply carries its own name, so it takes no part in
            # guessing a nameless reply's request.
            address = target_address

        else:
            if node_id is None:
                node_id = await self._nodes.get()

            address = self._node_host_map.get(node_id)

            if address is None and target_address:
                address = target_address

        if request_type is None:
            request_type = "request"

        # The request's id, which the server echoes in its reply: that reply,
        # and no other request's, completes this one -- however many requests
        # to the same target are in flight. Every attempt carries it, so a
        # late reply to an earlier attempt still answers this request.
        request_id = self.id_generator.generate()

        # Build message once - we'll regenerate shard_id on each retry
        message = Message(
            self.node_id,
            target,
            data=data,
            service_host=self.host,
            service_port=self.port,
            request_id=request_id,
        )

        for attempt in range(self._retries + 1):
            # Generate new shard_id for each attempt to avoid replay detection
            item = cloudpickle.dumps(
                (
                    request_type,
                    self.id_generator.generate(),
                    message,
                ),
                pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            waiter = self._loop.create_future()
            self._request_waiters[request_id] = waiter

            try:
                await self._sendto_with_retry(compressed, address)
            except BlockingIOError:
                self._request_waiters.pop(request_id, None)
                # Socket buffer full after all retries - return error response
                return (
                    self.id_generator.generate(),
                    Message(
                        self.node_id,
                        target,
                        service_host=self.host,
                        service_port=self.port,
                        error="Send failed: socket buffer full.",
                    ),
                )

            try:
                result: Tuple[int, Message[K]] = await asyncio.wait_for(
                    waiter,
                    timeout=self._request_timeout,
                )

                (shard_id, response) = result

                if request_type == "connect":
                    return (
                        shard_id,
                        response,
                    )

                return (shard_id, response.data)

            except asyncio.TimeoutError:
                # Worker may not be ready yet - retry with exponential backoff
                if attempt < self._retries:
                    await asyncio.sleep(self._retry_interval * (2 ** attempt))
            except Exception:
                await asyncio.sleep(self._retry_interval)

            finally:
                # Answered, timed out or cancelled: this attempt's waiter goes.
                if self._request_waiters.get(request_id) is waiter:
                    del self._request_waiters[request_id]

        return (
            self.id_generator.generate(),
            Message(
                self.node_id,
                target,
                service_host=self.host,
                service_port=self.port,
                error="Request timed out.",
            ),
        )

    async def broadcast(
        self,
        target: str,
        data: T,
    ) -> list[Tuple[int, K]]:
        return await asyncio.gather(
            *[
                self.send(
                    target,
                    data,
                    node_id=node,
                )
                for node in self.nodes
            ]
        )

    def read(self, data: bytes, addr: Tuple[str, int]) -> None:
        # Validate compressed message size before decompression
        try:
            validate_compressed_size(data, raise_on_error=True)
        except MessageSizeError:
            # Silently drop oversized messages - don't send error response
            return
        
        decompressed = b""
        compressed_size = len(data)

        try:
            decompressed = self._decompressor.decompress(
                data, 
                max_output_size=MAX_DECOMPRESSED_SIZE,
            )

        except Exception:
            # Sanitized error - don't leak internal details
            self._pending_responses.append(
                asyncio.ensure_future(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="protocol_error",
                            error="Message processing failed",
                        ),
                        addr,
                    )
                )
            )
            return

        # Validate decompressed size (compression bomb detection)
        try:
            validate_decompressed_size(decompressed, compressed_size, raise_on_error=True)
        except MessageSizeError:
            # Silently drop - possible compression bomb
            return

        decrypted = decompressed

        try:
            decrypted = self._encryptor.decrypt(decompressed)

        except (EncryptionError, Exception):
            # Sanitized error - don't leak encryption details
            self._pending_responses.append(
                asyncio.ensure_future(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="protocol_error",
                            error="Message processing failed",
                        ),
                        addr,
                    )
                )
            )
            return

        result: Tuple[str, int, Message] = (None, None, None)

        try:
            # Use restricted unpickler to prevent arbitrary code execution
            result: Tuple[str, int, Message] = restricted_loads(decrypted)

        except (SecurityError, Exception):
            # Sanitized error - don't leak details about what was blocked
            self._pending_responses.append(
                asyncio.ensure_future(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="protocol_error",
                            error="Message processing failed",
                        ),
                        addr,
                    )
                )
            )
            return

        message_type: str | None = None
        shard_id: int | None = None
        message: Message | None = None

        try:
            (
                message_type,
                shard_id,
                message,
            ) = result

        except Exception:
            # Sanitized error - don't leak message structure details
            self._pending_responses.append(
                asyncio.ensure_future(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="protocol_error",
                            error="Message processing failed",
                        ),
                        addr,
                    )
                )
            )
            return

        # Replay attack protection - validate message freshness and uniqueness
        # Skip for "response" (replies to our requests) and "connect" (idempotent,
        # often retried during startup when processes may be slow to spin up)
        if message_type not in ("response", "connect"):
            try:
                self._replay_guard.validate(shard_id, raise_on_error=True)
            except ReplayError:
                # Silently drop replayed messages - don't send error response
                # as that could be used for timing attacks
                return

        if message_type == "connect":
            self._pending_responses.append(
                asyncio.ensure_future(
                    self._read_connect(
                        shard_id,
                        message,
                        addr,
                    )
                )
            )

        elif message_type == "request":
            # Inject sender's node_id into JobContext if present
            data = message.data
            if isinstance(data, JobContext):
                data.node_id = message.node_id

            self._pending_responses.append(
                asyncio.ensure_future(
                    self._read(
                        shard_id,
                        message,
                        self._events.get(message.name)(
                            shard_id,
                            data,
                        ),
                        addr,
                    )
                )
            )

        else:
            self._pending_responses.append(
                asyncio.ensure_future(
                    self._receive_response(
                        shard_id,
                        message,
                    )
                )
            )

    async def _receive_response(
        self,
        shard_id: int,
        message: Message[T],
    ):
        try:
            await self._add_node_from_shard_id(shard_id, message)

            if (request_id := message.request_id) is not None:
                # A send()'s reply: it completes that request's waiter, if the
                # request still waits, and nothing else.
                if (request_waiter := self._request_waiters.pop(request_id, None)) is not None and not request_waiter.done():
                    request_waiter.set_result(
                        (
                            shard_id,
                            message,
                        )
                    )

                return

            # A reply nothing waits for any more -- a late one to a finished
            # connect attempt -- finds no waiters and adds no entry for its
            # name. One with waiters goes, without waiting, to the first that
            # still waits: a duplicate reply finds none and is dropped,
            # rather than leaving a task blocked on the queue for good.
            event_waiter = self._waiters.get(message.name)

            if event_waiter is not None:
                while not event_waiter.empty():
                    waiter: asyncio.Future = event_waiter.get_nowait()

                    if not waiter.done():
                        waiter.set_result(
                            (
                                shard_id,
                                message,
                            )
                        )

                        break

        except Exception as response_error:
            async with self._logger.context(
                name=f"graph_server_{self._node_id_base}"
            ) as ctx:
                await ctx.log_prepared(
                    message=(
                        f"Node {self._node_id_base} at {self.host}:{self.port} failed "
                        f"taking a {message.name} reply ({type(response_error).__name__}): "
                        f"{response_error}"
                    ),
                    name="error",
                )

    async def _return_error(
        self,
        error: Message[None],
        addr: tuple[str, int],
    ):
        item = cloudpickle.dumps(
            (
                "response",
                self.id_generator.generate(),
                error,
            ),
            pickle.HIGHEST_PROTOCOL,
        )

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)

        try:
            await self._sendto_with_retry(compressed, addr)
        except BlockingIOError as send_error:
            # Error responses are best-effort: the failure is logged, not
            # raised.
            async with self._logger.context(
                name=f"graph_server_{self._node_id_base}"
            ) as ctx:
                await ctx.log_prepared(
                    message=(
                        f"Node {self._node_id_base} at {self.host}:{self.port} could not "
                        f"send an error reply to {addr[0]}:{addr[1]}: {send_error}"
                    ),
                    name="error",
                )

    async def _read_connect(
        self,
        shard_id: int,
        message: Message[None],
        addr: tuple[str, int],
    ):
        await self._add_node_from_shard_id(shard_id, message)
        item = cloudpickle.dumps(
            (
                "response",
                self.id_generator.generate(),
                Message(
                    node_id=self.node_id,
                    name=message.name,
                    data=None,
                    service_host=self.host,
                    service_port=self.port,
                    request_id=message.request_id,
                ),
            ),
            pickle.HIGHEST_PROTOCOL,
        )

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)

        try:
            await self._sendto_with_retry(compressed, addr)
        except BlockingIOError as send_error:
            # Connect responses are critical but best-effort: log and go on.
            async with self._logger.context(
                name=f"graph_server_{self._node_id_base}"
            ) as ctx:
                await ctx.log_prepared(
                    message=(
                        f"Node {self._node_id_base} at {self.host}:{self.port} could not "
                        f"send a connect reply to {addr[0]}:{addr[1]}: {send_error}"
                    ),
                    name="error",
                )

    async def _read(
        self,
        shard_id: int,
        message: Message[T],
        coroutine: Coroutine,
        addr: Tuple[str, int],
    ) -> Coroutine[Any, Any, None]:
        try:
            await self._add_node_from_shard_id(shard_id, message)
            response: K = await coroutine

            item = cloudpickle.dumps(
                (
                    "response",
                    self.id_generator.generate(),
                    Message(
                        node_id=self.node_id,
                        name=message.name,
                        data=response,
                        service_host=self.host,
                        service_port=self.port,
                        request_id=message.request_id,
                    ),
                ),
                pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            await self._sendto_with_retry(compressed, addr)

        except (Exception, socket.error) as read_error:
            # The requester sees a timeout; the failure itself is logged.
            async with self._logger.context(
                name=f"graph_server_{self._node_id_base}"
            ) as ctx:
                await ctx.log_prepared(
                    message=(
                        f"Node {self._node_id_base} at {self.host}:{self.port} failed "
                        f"answering {message.name} from {addr[0]}:{addr[1]} "
                        f"({type(read_error).__name__}): {read_error}"
                    ),
                    name="error",
                )

    async def _add_node_from_shard_id(self, shard_id: int, message: Message[T | None]):
        # Use full 64-bit node_id from message instead of 10-bit snowflake instance
        node_id = message.node_id
        if (await self._nodes.exists(node_id)) is False:
            self._nodes.put_no_wait(node_id)
            self._node_host_map[node_id] = (
                message.service_host,
                message.service_port,
            )

    async def wait_for_socket_shutdown(self):
        await asyncio.sleep(self._shutdown_poll_rate)

        while await self._loop.run_in_executor(None, self.udp_socket.fileno) != -1:
            await asyncio.sleep(self._shutdown_poll_rate)

    async def close(self) -> None:
        async with self._logger.context(
            name=f"graph_server_{self._node_id_base}"
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} shutting down",
                name="info",
            )

            if self._shutdown_task:
                await self._shutdown_task

            if self._transport:
                try:
                    self._transport.abort()

                except Exception:
                    pass

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} server transport closed",
                    name="debug",
                )

            if self.udp_socket:
                self.udp_socket.close()

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} server socket closed",
                    name="debug",
                )

            if self._sleep_task:
                try:
                    self._sleep_task.cancel()

                except Exception:
                    pass

                except asyncio.CancelledError:
                    pass

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} sleep task cancelled closed",
                    name="debug",
                )

            if self._cleanup_task:
                try:
                    self._cleanup_task.cancel()

                except Exception:
                    pass

                except asyncio.CancelledError:
                    pass

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} cleanup task closed",
                    name="debug",
                )

            if self.tasks:
                self.tasks.abort()

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} task runner closed",
                    name="debug",
                )

            if self._run_future and (
                not self._run_future.done() or not self._run_future.cancelled()
            ):
                try:
                    self._run_future.set_result(None)

                except asyncio.InvalidStateError:
                    pass

                except asyncio.CancelledError:
                    pass

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} run task completed",
                    name="debug",
                )

            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} task cleanup complete",
                name="debug",
            )

        self._pending_responses.clear()

    def stop(self):
        self._shutdown_task = asyncio.ensure_future(self._shutdown())

    async def _shutdown(self):
        # Stop accepting new work first
        self._running = False

        # _pending_responses stores asyncio.Task objects, which cannot
        # be completed with set_result(). Cancellation is the correct
        # shutdown signal for pending tasks.
        pending_tasks = list(self._pending_responses)
        for task in pending_tasks:
            if not task.done():
                task.cancel()

        # Signal run_forever() to exit
        if self._run_future:
            try:
                self._run_future.set_result(None)

            except asyncio.InvalidStateError:
                pass

            except asyncio.CancelledError:
                pass

    def abort(self):
        self._running = False

        for pending in self._pending_responses:
            try:
                pending.cancel()

            except Exception:
                pass

        if self._shutdown_task and not self._shutdown_task.done():
            try:
                self._shutdown_task.cancel()

            except Exception:
                pass

        if self._transport:
            try:
                self._transport.abort()

            except Exception:
                pass

        if self.udp_socket:
            try:
                self.udp_socket.shutdown(socket.SHUT_RDWR)

            except Exception:
                pass

            try:
                self.udp_socket.close()

            except Exception:
                pass

        if self._sleep_task:
            try:
                self._sleep_task.cancel()

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            try:
                self._cleanup_task.cancel()

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

        if self.tasks:
            self.tasks.abort()

        if self._run_future:
            try:
                self._run_future.cancel()

            except asyncio.InvalidStateError:
                pass

            except asyncio.CancelledError:
                pass

        # NOTE: deliberately *not* iterating ``asyncio.all_tasks()`` here.
        # Every task this protocol owns is already accounted for via
        # ``_pending_responses``, ``_shutdown_task``, ``_sleep_task``,
        # ``_cleanup_task``, ``self.tasks`` (TaskRunner), and
        # ``_run_future``. A blanket ``all_tasks()`` cancel is only safe
        # in single-server processes — in any in-process multi-server
        # context (tests, supervisors, embedded uses) it cancels tasks
        # owned by *other* servers sharing the loop, cascading shutdown
        # across unrelated nodes. The OS reclaims everything in the
        # production single-process exit path anyway.
        self._pending_responses.clear()

        try:
            self._logger.abort()

        except Exception:
            pass
