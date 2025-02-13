from __future__ import annotations

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
    Literal,
    Tuple,
    TypeVar,
    Union,
)

import cloudpickle
import zstandard

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.engines.client.udp.protocols.dtls import do_patch
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

from .encryption import AESGCMFernet
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
    ) -> None:
        self._node_id_base = uuid.uuid4().int >> 64
        self.node_id: int | None = None

        self._logger = Logger()

        self.id_generator: SnowflakeGenerator | None = None

        self.env = env

        self.host = host
        self.port = port

        self._events: Dict[str, Coroutine] = {}

        self.queue: Dict[str, Deque[Tuple[int, float, Any]]] = defaultdict(deque)
        self.tasks: TaskRunner | None = None
        self.connected = False
        self._running = False

        self._transport: asyncio.DatagramTransport = None
        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]]] = defaultdict(deque)
        self._waiters: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._pending_responses: Deque[asyncio.Task] = deque()
        self._last_call: Deque[str] = deque()

        self._sent_values = deque()

        self._udp_cert_path: Union[str, None] = None
        self._udp_key_path: Union[str, None] = None
        self._udp_ssl_context: Union[ssl.SSLContext, None] = None
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
        self._retries = env.MERCURY_SYNC_SEND_RETRIES

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_CONNECT_RETRIES
        self._run_future: asyncio.Future = None
        self._node_host_map: Dict[int, Tuple[str, int]] = {}
        self._nodes: LockedSet[int] | None = None
        self._abort_handle_created: bool = None
        self._connect_lock: asyncio.Lock | None = None
        self._shutdown_task: asyncio.Future | None = None

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
        if not self._abort_handle_created:
            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

            self._abort_handle_created = True

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

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
            self._udp_ssl_context = self._create_udp_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
            )

        run_start = True
        instance_id: int | None = None

        while run_start:
            if self._transport is None:
                await self.start_server(
                    cert_path=cert_path,
                    key_path=key_path,
                    worker_socket=worker_socket,
                    worker_server=worker_server,
                )

            try:
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

            except (Exception, asyncio.CancelledError, socket.error, OSError):
                pass

            await asyncio.sleep(self._retry_interval)

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

        if not self._abort_handle_created:
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
            self._server_ssl_context = self._create_udp_ssl_context(
                cert_path=cert_path, key_path=key_path
            )

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

    def _start_tasks(self):
        self.tasks.start_cleanup()
        for task in self.tasks.all_tasks():
            task.call = task.call.__get__(self, self.__class__)
            setattr(self, task.name, task.call)

            if task.trigger == "ON_START":
                self.tasks.run(task.name)

    def _create_udp_ssl_context(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
    ) -> ssl.SSLContext:
        if self._udp_cert_path is None:
            self._udp_cert_path = cert_path

        if self._udp_key_path is None:
            self._udp_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
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
                        # await self._reset_connection()
                        pass

                    if len(self._pending_responses) > 0:
                        self._pending_responses.pop()

    async def send(
        self,
        target: str,
        data: T,
        node_id: int | None = None,
        target_address: Tuple[str, int] | None = None,
        request_type: Literal["request", "connect"] | None = None,
    ) -> Tuple[int, K]:
        self._last_call.append(target)

        if node_id is None:
            node_id = await self._nodes.get()

        address = self._node_host_map.get(node_id)

        if address is None and target_address:
            address = target_address

        if request_type is None:
            request_type = "request"

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
            pickle.HIGHEST_PROTOCOL,
        )

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)

        self._transport.sendto(compressed, address)

        for _ in range(self._retries):
            try:
                waiter = self._loop.create_future()
                self._waiters[target].put_nowait(waiter)

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

            except Exception:
                await asyncio.sleep(self._retry_interval)

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

    async def send_bytes(
        self,
        target: str,
        data: bytes,
        node_id: int | None = None,
        target_address: Tuple[str, int] | None = None,
    ) -> bytes:
        self._last_call.append(target)

        if node_id is None:
            node_id = await self._nodes.get()

        address = self._node_host_map.get(node_id)

        if address is None and target_address:
            address = target_address

        encrypted_message = self._encryptor.encrypt(data)
        compressed = self._compressor.compress(encrypted_message)

        try:
            self._transport.sendto(compressed, address)

            for _ in range(self._retries):
                try:
                    waiter = self._loop.create_future()
                    self._waiters[target].put_nowait(waiter)

                    result: Tuple[int, bytes] = await asyncio.wait_for(
                        waiter,
                        timeout=self._request_timeout,
                    )

                    (shard_id, response) = result

                    return (shard_id, response)

                except Exception:
                    await asyncio.sleep(self._retry_interval)

        except (Exception, socket.error):
            return (self.id_generator.generate(), b"Request timed out.")

    async def stream(
        self,
        target: str,
        data: T,
        node_id: int | None = None,
        target_address: Tuple[str, int] | None = None,
        request_type: Literal["request", "connect"] | None = None,
    ) -> AsyncIterable[Tuple[int, Dict[str, Any]]]:
        self._last_call.append(target)

        if node_id is None:
            node_id = await self._nodes.get()

        address = self._node_host_map.get(node_id)

        if address is None and target_address:
            address = target_address

        if request_type is None:
            request_type = "request"

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
            pickle.HIGHEST_PROTOCOL,
        )

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)

        try:
            self._transport.sendto(compressed, address)

            waiter = self._loop.create_future()
            self._waiters[target].put_nowait(waiter)

            await asyncio.wait_for(waiter, timeout=self._request_timeout)

            for item in self.queue[target]:
                (shard_id, response) = item

                yield (shard_id, response)

            self.queue.clear()

        except (Exception, socket.error):
            yield (
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
        decompressed = b""

        try:
            decompressed = self._decompressor.decompress(data)

        except Exception as decompression_error:
            self._pending_responses.append(
                asyncio.create_task(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="decompression_error",
                            error=str(decompression_error),
                        ),
                        addr,
                    )
                )
            )

            return

        decrypted = decompressed

        try:
            decrypted = self._encryptor.decrypt(decompressed)

        except Exception as decryption_error:
            self._pending_responses.append(
                asyncio.create_task(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="decryption_error",
                            error=str(decryption_error),
                        ),
                        addr,
                    )
                )
            )

            return

        result: Tuple[str, int, Message] = (None, None, None)

        try:
            result: Tuple[str, int, Message] = cloudpickle.loads(decrypted)

        except Exception as err:
            self._pending_responses.append(
                asyncio.create_task(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="deserialization_error",
                            error=str(err),
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

        except Exception as err:
            self._pending_responses.append(
                asyncio.create_task(
                    self._return_error(
                        Message(
                            node_id=self.node_id,
                            service_host=self.host,
                            service_port=self.port,
                            name="deserialization_error",
                            error=str(err),
                        ),
                        addr,
                    )
                )
            )

            return

        if message_type == "connect":
            self._pending_responses.append(
                asyncio.create_task(
                    self._read_connect(
                        shard_id,
                        message,
                        addr,
                    )
                )
            )

        elif message_type == "request":
            self._pending_responses.append(
                asyncio.create_task(
                    self._read(
                        shard_id,
                        message,
                        self._events.get(message.name)(
                            shard_id,
                            message.data,
                        ),
                        addr,
                    )
                )
            )

        elif message_type == "stream":
            self._pending_responses.append(
                asyncio.create_task(
                    self._read_iterator(
                        message.name,
                        message,
                        self._events.get(message.name)(shard_id, message.data),
                        addr,
                    )
                )
            )

        else:
            self._pending_responses.append(
                asyncio.create_task(
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

            if message.name is None and bool(self._last_call):
                message.name = self._last_call.pop()

            event_waiter = self._waiters[message.name]

            if bool(event_waiter):
                waiter: asyncio.Future = await event_waiter.get()

                try:
                    waiter.set_result(
                        (
                            shard_id,
                            message,
                        )
                    )

                except asyncio.InvalidStateError:
                    pass

        except Exception:
            pass

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

        self._transport.sendto(compressed, addr)

    async def _reset_connection(self):
        try:
            await self.close()
            await self.start_server(
                cert_path=self._udp_cert_path,
                key_path=self._udp_key_path,
            )

        except Exception:
            pass

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
                ),
            ),
            pickle.HIGHEST_PROTOCOL,
        )

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)

        self._transport.sendto(compressed, addr)

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
                    ),
                ),
                pickle.HIGHEST_PROTOCOL,
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            self._transport.sendto(compressed, addr)

        except (Exception, socket.error):
            pass
            # await self._reset_connection()

    async def _read_iterator(
        self,
        shard_id: int,
        message: Message[T],
        coroutine: AsyncIterable[K],
        addr: Tuple[str, int],
    ) -> Coroutine[Any, Any, None]:
        await self._add_node_from_shard_id(shard_id, message)

        async for response in coroutine:
            try:
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
                        ),
                    ),
                    pickle.HIGHEST_PROTOCOL,
                )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)
                self._transport.sendto(compressed, addr)

            except Exception:
                pass

    async def _add_node_from_shard_id(self, shard_id: int, message: Message[T | None]):
        snowflake = Snowflake.parse(shard_id)
        instance = snowflake.instance
        if (await self._nodes.exists(instance)) is False:
            self._nodes.put_no_wait(instance)
            self._node_host_map[instance] = (
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
