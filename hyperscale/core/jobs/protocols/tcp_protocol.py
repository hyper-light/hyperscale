import asyncio
import inspect
import pickle
import signal
import socket
import ssl
import uuid
from collections import defaultdict, deque
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
)

import cloudpickle
import zstandard

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.jobs.data_structures import LockedSet
from hyperscale.core.jobs.hooks.hook_type import HookType
from hyperscale.core.jobs.models import Env, Message
from hyperscale.core.jobs.tasks import TaskRunner
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ControllerDebug,
    ControllerError,
    ControllerFatal,
    ControllerInfo,
    ControllerTrace,
)

from .client_protocol import MercurySyncTCPClientProtocol
from .encryption import AESGCMFernet
from .server_protocol import MercurySyncTCPServerProtocol

T = TypeVar("T")
K = TypeVar("K")


class TCPProtocol(Generic[T, K]):
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
    ) -> None:
        self._node_id_base = uuid.uuid4().int >> 64
        self.node_id: int | None = None

        self.id_generator: Optional[SnowflakeGenerator] = None
        self._logger = Logger()

        self.env = env

        self.host = host
        self.port = port

        self._events: Dict[str, Coroutine] = {}

        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]]] = defaultdict(deque)
        self.tasks: Optional[TaskRunner] = None
        self.connected = False
        self._running = False

        self._client_transports: Dict[tuple[str, int], asyncio.Transport] = {}
        self._client_sockets: Dict[tuple[str, int], socket.socket] = {}
        self._server: asyncio.Server = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses: Deque[asyncio.Task] = deque()
        self._last_call: Deque[str] = deque()

        self._sent_values = deque()
        self.server_socket = None
        self._stream = False

        self._client_key_path: Optional[str] = None
        self._client_cert_path: Optional[str] = None

        self._server_key_path: Optional[str] = None
        self._server_cert_path: Optional[str] = None

        self._client_ssl_context: Optional[ssl.SSLContext] = None
        self._server_ssl_context: Optional[ssl.SSLContext] = None

        self._encryptor = AESGCMFernet(env)
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._compressor: Optional[zstandard.ZstdCompressor] = None
        self._decompressor: Optional[zstandard.ZstdDecompressor] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._sleep_task: Optional[asyncio.Task] = None
        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time

        self._request_timeout = TimeParser(env.MERCURY_SYNC_REQUEST_TIMEOUT).time
        self._connect_timeout = TimeParser(env.MERCURY_SYNC_CONNECT_TIMEOUT).time
        self._retry_interval = TimeParser(env.MERCURY_SYNC_RETRY_INTERVAL).time
        self._shutdown_poll_rate = TimeParser(env.MERCURY_SYNC_SHUTDOWN_POLL_RATE).time
        self._retries = env.MERCURY_SYNC_SEND_RETRIES

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_CONNECT_RETRIES
        self._run_future: Optional[asyncio.Future] = None
        self._node_host_map: Dict[int, Tuple[str, int]] = {}
        self._nodes: Optional[LockedSet[int]] = None
        self._abort_handle_created: bool = None
        self._connect_lock: asyncio.Lock | None = None
        self._shutdown_task: asyncio.Future | None = None

    @property
    def nodes(self):
        return [node_id for node_id in self._node_host_map]

    def node_at(self, idx: int):
        return self.nodes[idx]

    async def start_server(
        self,
        logfile: str,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        worker_socket: Optional[socket.socket] = None,
        worker_server: Optional[asyncio.Server] = None,
    ):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        if not self._abort_handle_created:
            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

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
            snowflake_id = self.id_generator.generate()
            snowflake = Snowflake.parse(snowflake_id)

            self.node_id = snowflake.instance

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

        if cert_path and key_path:
            self._server_ssl_context = self._create_server_ssl_context(
                cert_path=cert_path, key_path=key_path
            )

        run_start = True

        while run_start:
            try:
                if self.connected is False and worker_socket is None:
                    self.server_socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.server_socket.setsockopt(
                        socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
                    )

                    await self._loop.run_in_executor(
                        None,
                        self.server_socket.bind,
                        (self.host, self.port),
                    )

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

                    run_start = False
                    self.connected = True

                    self._cleanup_task = self._loop.create_task(self._cleanup())

                if self.connected is False and worker_server is None:
                    server: asyncio.Server = await self._loop.create_server(
                        lambda: MercurySyncTCPServerProtocol(self.read),
                        sock=self.server_socket,
                        ssl=self._server_ssl_context,
                    )

                    self._server = server
                    self._cleanup_task = self._loop.create_task(self._cleanup())

                    run_start = False
                    self.connected = True

            except (Exception, asyncio.CancelledError, socket.error, OSError):
                pass

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
                "trace": (ControllerTrace, default_config),
                "debug": (
                    ControllerDebug,
                    default_config,
                ),
                "info": (
                    ControllerInfo,
                    default_config,
                ),
                "error": (
                    ControllerError,
                    default_config,
                ),
                "fatal": (
                    ControllerFatal,
                    default_config,
                ),
            },
        )

        self._start_tasks()

    def __iter__(self):
        for node_id in self._node_host_map:
            yield node_id

    async def run_forever(self):
        try:
            self._run_future = self._loop.create_future()
            await self._run_future

        except asyncio.CancelledError:
            pass

    def _create_server_ssl_context(
        self, cert_path: Optional[str] = None, key_path: Optional[str] = None
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
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ssl_ctx.set_ciphers("ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384")

        return ssl_ctx

    async def connect_client(
        self,
        logfile: str,
        address: Tuple[str, int],
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        worker_socket: Optional[socket.socket] = None,
    ) -> int | None:
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        if not self._abort_handle_created:
            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

        await self._connect_lock.acquire()

        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)

        if self.node_id is None:
            self.node_id = uuid.uuid4().int >> 64

        if self.id_generator is None:
            self.id_generator = SnowflakeGenerator(self.node_id)

        if self._nodes is None:
            self._nodes: LockedSet[int] = LockedSet()

        if self._compressor is None:
            self._compressor = zstandard.ZstdCompressor()

        if self._decompressor is None:
            self._decompressor = zstandard.ZstdDecompressor()

        if cert_path and key_path:
            self._client_ssl_context = self._create_client_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
            )

        run_start = True
        instance_id: int | None = None

        while run_start:
            try:
                if worker_socket is None:
                    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self._client_sockets[address] = tcp_socket

                    await asyncio.wait_for(
                        self._loop.run_in_executor(None, tcp_socket.connect, address),
                        timeout=self._connect_timeout,
                    )

                    tcp_socket.setblocking(False)

                else:
                    tcp_socket = worker_socket

                client_transport, _ = await asyncio.wait_for(
                    self._loop.create_connection(
                        lambda: MercurySyncTCPClientProtocol(self.read),
                        sock=tcp_socket,
                        ssl=self._client_ssl_context,
                    ),
                    timeout=self._connect_timeout,
                )

                self._client_transports[address] = client_transport

                result: Tuple[int, Message[None]] = await asyncio.wait_for(
                    self.send(
                        None,
                        None,
                        target_address=address,
                        request_type="connect",
                    ),
                    timeout=self._connect_timeout,
                )

                shard_id, _ = result

                snowflake = Snowflake.parse(shard_id)

                instance_id = snowflake.instance

                self._node_host_map[instance_id] = address
                self._nodes.put_no_wait(instance_id)

                run_start = False

            except Exception:
                pass

            except OSError:
                pass

            except asyncio.CancelledError:
                pass

            await asyncio.sleep(1)

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
                "trace": (ControllerTrace, default_config),
                "debug": (
                    ControllerDebug,
                    default_config,
                ),
                "info": (
                    ControllerInfo,
                    default_config,
                ),
                "error": (
                    ControllerError,
                    default_config,
                ),
                "fatal": (
                    ControllerFatal,
                    default_config,
                ),
            },
        )

        if self._connect_lock.locked():
            self._connect_lock.release()

        return instance_id

    def _create_client_ssl_context(
        self, cert_path: Optional[str] = None, key_path: Optional[str] = None
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

            for pending in list(self._pending_responses):
                if pending.done() or pending.cancelled():
                    try:
                        await pending

                    except (Exception, socket.error):
                        pass

                    self._pending_responses.pop()

    async def broadcast(
        self,
        target: str,
        data: T,
    ) -> List[Tuple[int, K]]:
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

    async def send(
        self,
        target: str,
        data: T,
        node_id: Optional[int] = None,
        target_address: Optional[Tuple[str, int]] = None,
        request_type: Optional[Literal["request", "connect"]] = None,
    ) -> Tuple[int, K]:
        async with self._semaphore:
            try:
                self._last_call.append(target)

                if node_id is None:
                    node_id = await self._nodes.get()

                address = self._node_host_map.get(node_id)

                if address is None and target_address:
                    address = target_address

                if request_type is None:
                    request_type = "request"

                client_transport = self._client_transports.get(address)
                if client_transport is None or client_transport.is_closing():
                    await self.connect_client(
                        address,
                        cert_path=self._client_cert_path,
                        key_path=self._client_key_path,
                    )

                    client_transport = self._client_transports.get(address)

                item = cloudpickle.dumps(
                    (
                        request_type,
                        self.id_generator.generate(),
                        Message(
                            self.node_id,
                            target,
                            data=data,
                            service_host=self.host,
                            service_port=self.port,
                        ),
                    ),
                    protocol=pickle.HIGHEST_PROTOCOL,
                )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                client_transport.write(compressed)

                while True:
                    try:
                        waiter = self._loop.create_future()
                        self._waiters[target].append(waiter)

                        result: Tuple[str, int, Message[K]] = await asyncio.wait_for(
                            waiter,
                            timeout=self._request_timeout,
                        )

                        (_, shard_id, response) = result

                        if request_type == "connect":
                            return (
                                shard_id,
                                response,
                            )

                        return (shard_id, response.data)

                    except (Exception, socket.error):
                        client_transport.close()

                        await self.connect_client(
                            address,
                            cert_path=self._client_cert_path,
                            key_path=self._client_key_path,
                        )

                        client_transport = self._client_transports.get(address)

                        await asyncio.sleep(self._retry_interval)

            except (Exception, socket.error) as err:
                return (
                    self.id_generator.generate(),
                    Message(
                        self.node_id,
                        target,
                        error=str(err),
                    ),
                )

    async def stream(
        self,
        target: str,
        data: T,
        target_address: Optional[Tuple[str, int]] = None,
        request_type: Optional[Literal["request", "connect"]] = None,
    ) -> AsyncIterable[Tuple[int, Message[K]]]:
        async with self._semaphore:
            try:
                self._last_call.append(target)

                node_id = await self._nodes.get()

                address = self._node_host_map.get(node_id)

                if address is None and target_address:
                    address = target_address

                if request_type is None:
                    request_type = "request"

                client_transport = self._client_transports.get(address)

                if self._stream is False:
                    item = cloudpickle.dumps(
                        (
                            "stream_connect",
                            self.id_generator.generate(),
                            Message(
                                self.node_id,
                                target,
                                data=data,
                                service_host=self.host,
                                service_port=self.port,
                            ),
                        ),
                        protocol=pickle.HIGHEST_PROTOCOL,
                    )

                else:
                    item = cloudpickle.dumps(
                        (
                            "stream",
                            self.id_generator.generate(),
                            Message(
                                self.node_id,
                                target,
                                data=data,
                                service_host=self.host,
                                service_port=self.port,
                            ),
                        ),
                        protocol=pickle.HIGHEST_PROTOCOL,
                    )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                if client_transport.is_closing():
                    yield (
                        self.id_generator.generate(),
                        Message(
                            self.node_id,
                            target,
                            error="Transport closed.",
                        ),
                    )

                client_transport.write(compressed)

                waiter = self._loop.create_future()
                self._waiters[target].append(waiter)

                await asyncio.wait_for(
                    waiter,
                    timeout=self._request_timeout,
                )

                if self._stream is False:
                    self.queue[target].pop()

                    self._stream = True

                    item = cloudpickle.dumps(
                        (
                            "stream",
                            self.id_generator.generate(),
                            Message(
                                self.node_id,
                                target,
                                data=data,
                                service_host=self.host,
                                service_port=self.port,
                            ),
                        ),
                        pickle.HIGHEST_PROTOCOL,
                    )

                    encrypted_message = self._encryptor.encrypt(item)
                    compressed = self._compressor.compress(encrypted_message)

                    client_transport.write(compressed)

                    waiter = self._loop.create_future()
                    self._waiters[target].append(waiter)

                    await waiter

                while bool(self.queue[target]) and self._stream:
                    (_, shard_id, response) = self.queue[target].pop()

                    yield (shard_id, response)

            except (Exception, socket.error):
                yield (
                    self.id_generator.generate(),
                    Message(
                        self.node_id,
                        target,
                        error="Request timed out.",
                    ),
                )

        self.queue.clear()

    def read(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ):
        self._pending_responses.append(
            asyncio.create_task(self._read(data, transport)),
        )

    async def _read(
        self,
        data: bytes,
        transport: asyncio.Transport,
    ) -> None:
        decompressed = b""
        try:
            decompressed = self._decompressor.decompress(data)

        except Exception as decompression_error:
            error = Message(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                name="decompression_error",
                error=str(decompression_error),
            )

            item = cloudpickle.dumps(
                (
                    "response",
                    self.id_generator.generate(),
                    error,
                ),
                protocol=pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            transport.write(compressed)

            return

        decrypted = self._encryptor.decrypt(decompressed)

        result: Tuple[str, int, Message] = None

        try:
            result: Tuple[str, int, Message] = cloudpickle.loads(decrypted)

        except Exception as err:
            error = Message(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                name="deserialization_error",
                error=str(err),
            )

            item = cloudpickle.dumps(
                (
                    "response",
                    self.id_generator.generate(),
                    error,
                ),
                protocol=pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            transport.write(compressed)

            return

        (
            message_type,
            shard_id,
            message,
        ) = result

        if transport.is_closing():
            return

        snowflake = Snowflake.parse(shard_id)
        instance = snowflake.instance
        if (await self._nodes.exists(instance)) is False:
            self._nodes.put_no_wait(instance)
            self._node_host_map[instance] = (
                message.service_host,
                message.service_port,
            )

        if message_type == "connect":
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
                    ),
                ),
                protocol=pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            transport.write(compressed)

        elif message_type == "request":
            response_call = self._events[message.name](
                shard_id,
                message.data,
            )

            response_data = await response_call

            item = cloudpickle.dumps(
                (
                    "response",
                    self.id_generator.generate(),
                    Message(
                        node_id=self.node_id,
                        name=message.name,
                        data=response_data,
                        service_host=self.host,
                        service_port=self.port,
                    ),
                ),
                protocol=pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            transport.write(compressed)

        elif message_type == "stream_connect":
            self.queue[message.name].append(
                (
                    message_type,
                    shard_id,
                    message,
                )
            )

            item = cloudpickle.dumps(
                (
                    "response",
                    self.id_generator.generate(),
                    Message(
                        node_id=self.node_id,
                        name=message.name,
                        data="Connection successful.",
                        service_host=self.host,
                        service_port=self.port,
                    ),
                ),
                protocol=pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            transport.write(compressed)

            event_waiter = self._waiters[message.name]

            if bool(event_waiter):
                waiter = event_waiter.pop()

                try:
                    waiter.set_result(None)

                except asyncio.InvalidStateError:
                    pass

        elif message_type == "stream" or message_type == "stream_connect":
            self.queue[message.name].append(
                (
                    message_type,
                    shard_id,
                    message_type,
                )
            )

            response_iterator = self._events[message.name](
                shard_id,
                message.data,
            )

            async for response in response_iterator:
                try:
                    item = cloudpickle.dumps(
                        (
                            "response",
                            self.id_generator.generate(),
                            Message(
                                self.node_id,
                                message.name,
                                response,
                                service_host=self.host,
                                service_port=self.port,
                            ),
                        ),
                        protocol=pickle.HIGHEST_PROTOCOL,
                    )

                    encrypted_message = self._encryptor.encrypt(item)
                    compressed = self._compressor.compress(encrypted_message)

                    transport.write(compressed)

                except (Exception, socket.error):
                    pass

            event_waiter = self._waiters[message.name]

            if bool(event_waiter):
                waiter = event_waiter.pop()

                try:
                    waiter.set_result(None)

                except asyncio.InvalidStateError:
                    pass

        else:
            if message.name is None and bool(self._last_call):
                message.name = self._last_call.pop()

            event_waiter = self._waiters[message.name]

            if bool(event_waiter):
                waiter = event_waiter.pop()

                try:
                    waiter.set_result(
                        (
                            message_type,
                            shard_id,
                            message,
                        )
                    )

                except asyncio.InvalidStateError:
                    pass

    async def close(self) -> None:
        self._stream = False
        self._running = False

        await self._shutdown_task

        close_task = asyncio.current_task()

        for client in self._client_transports.values():
            client.abort()

        for tcp_socket in self._client_sockets.values():
            try:
                tcp_socket.close()

            except Exception:
                pass

        if self._server:
            try:
                self._server.close()

            except Exception:
                pass

        if self.server_socket:
            try:
                self.server_socket.close()

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

        for task in asyncio.all_tasks():
            try:
                if task != close_task and task.cancelled() is False:
                    task.cancel()

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

        if self._run_future and (
            not self._run_future.done() or self._run_future.cancelled()
        ):
            try:
                self._run_future.set_result(None)

            except asyncio.InvalidStateError:
                pass

            except asyncio.CancelledError:
                pass

        self._pending_responses.clear()

    def stop(self):
        self._shutdown_task = asyncio.ensure_future(self._shutdown())

    async def _shutdown(self):
        for response in self._pending_responses:
            await response

        self._running = False

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

        if self._shutdown_task:
            try:
                self._shutdown_task.set_result(None)

            except Exception:
                pass

        if self._server:
            try:
                self._server.close()

            except Exception:
                pass

        if self.server_socket:
            try:
                self.server_socket.close()

            except Exception:
                pass

        for client in self._client_transports.values():
            client.abort()

        for tcp_socket in self._client_sockets.values():
            try:
                tcp_socket.close()

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

        close_task = asyncio.current_task()
        for task in asyncio.all_tasks():
            try:
                if task != close_task and task.cancelled() is False:
                    task.cancel()

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

        self._pending_responses.clear()

        try:
            self._logger.abort()

        except Exception:
            pass
