import asyncio
import datetime
import functools
import io
import os
import pathlib
import struct
import sys
import threading
import zlib
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Literal,
    TypeVar,
)

import msgspec
import zstandard

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.config.stream_type import StreamType
from hyperscale.logging.models import Entry, Log, LogLevel
from hyperscale.logging.exceptions import (
    WALBatchOverflowError,
    WALWriteError,
)
from hyperscale.logging.lsn import HybridLamportClock, LSN
from hyperscale.logging.queue import (
    ConsumerStatus,
    LogConsumer,
    LogProvider,
)
from hyperscale.logging.snowflake import SnowflakeGenerator

from .protocol import LoggerProtocol
from .retention_policy import (
    RetentionPolicy,
    RetentionPolicyConfig,
)

T = TypeVar("T", bound=Entry)

BINARY_HEADER_SIZE_V1 = 16
BINARY_HEADER_SIZE = 24
DEFAULT_QUEUE_MAX_SIZE = 10000
DEFAULT_BATCH_MAX_SIZE = 100

try:
    import uvloop as uvloop

    has_uvloop = True

except Exception:
    has_uvloop = False


def patch_transport_close(
    transport: asyncio.Transport,
    loop: asyncio.AbstractEventLoop,
):
    def close(*args, **kwargs):
        try:
            transport.close()

        except Exception:
            pass

    return close


class LoggerStream:
    def __init__(
        self,
        name: str | None = None,
        template: str | None = None,
        filename: str | None = None,
        directory: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ],
        ]
        | None = None,
        durability: DurabilityMode = DurabilityMode.FLUSH,
        log_format: Literal["json", "binary"] = "json",
        enable_lsn: bool = False,
        instance_id: int = 0,
        queue_max_size: int = DEFAULT_QUEUE_MAX_SIZE,
        batch_max_size: int = DEFAULT_BATCH_MAX_SIZE,
    ) -> None:
        self._name = name if name is not None else "default"
        self._default_template = template
        self._default_logfile = filename
        self._default_log_directory = directory

        self._default_retention_policy: RetentionPolicy | None = None
        if retention_policy:
            self._default_retention_policy = RetentionPolicy(retention_policy)
            self._default_retention_policy.parse()

        self._init_lock = asyncio.Lock()
        self._stream_writers: Dict[StreamType, asyncio.StreamWriter] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._compressor: zstandard.ZstdCompressor | None = None

        self._files: Dict[str, io.FileIO] = {}
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._cwd: str | None = None
        self._default_logfile_path: str | None = None

        self._retention_policies: Dict[str, RetentionPolicy] = {}

        self._config = LoggingConfig()
        self._initialized: bool = False
        self._consumer: LogConsumer | None = None
        self._provider: LogProvider | None = None
        self._closed = False
        self._closing = False
        self._stderr: io.TextIOBase | None = None
        self._stdout: io.TextIOBase | None = None
        self._transports: List[asyncio.Transport] = []

        self._models: Dict[str, Callable[..., Entry]] = {}
        self._queue: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue(
            maxsize=queue_max_size
        )
        self._scheduled_tasks: set[asyncio.Task[None]] = set()

        if models is None:
            models = {}

        for model_name, config in models.items():
            model, defaults = config
            self._models[model_name] = (model, defaults)

        self._models.update({"default": (Entry, {"level": LogLevel.INFO})})

        self._durability = durability
        self._log_format = log_format
        self._enable_lsn = enable_lsn
        self._instance_id = instance_id

        self._sequence_generator: SnowflakeGenerator | None = None
        self._lamport_clock: HybridLamportClock | None = None
        if enable_lsn:
            self._sequence_generator = SnowflakeGenerator(instance_id)
            self._lamport_clock = HybridLamportClock(node_id=instance_id)

        self._pending_batch: list[tuple[str, asyncio.Future[None]]] = []
        self._batch_lock: asyncio.Lock | None = None
        self._batch_timeout_ms: int = 10
        self._batch_max_size: int = batch_max_size
        self._batch_timer_handle: asyncio.TimerHandle | None = None
        self._batch_flush_task: asyncio.Task[None] | None = None

        self._read_files: Dict[str, io.FileIO] = {}
        self._read_locks: Dict[str, asyncio.Lock] = {}

    @property
    def has_active_subscriptions(self) -> bool:
        if self._provider is None:
            return False
        return self._provider.subscriptions_count > 0

    async def initialize(
        self,
        stdout_writer: asyncio.StreamWriter | None = None,
        stderr_writer: asyncio.StreamWriter | None = None,
        recovery_wal_path: str | None = None,
    ) -> asyncio.StreamWriter:
        async with self._init_lock:
            if self._initialized:
                return

            if self._config.disabled:
                self._initialized = True
                return

            self._compressor = self._compressor or zstandard.ZstdCompressor()
            self._loop = self._loop or asyncio.get_event_loop()
            self._provider = self._provider or LogProvider()
            self._consumer = self._consumer or self._provider.create_consumer()

            await self._setup_stdout_writer(stdout_writer)
            await self._setup_stderr_writer(stderr_writer)

            if recovery_wal_path is not None and self._enable_lsn:
                await self._recover_clock_from_wal(recovery_wal_path)

            self._initialized = True

    async def _setup_stdout_writer(
        self, stdout_writer: asyncio.StreamWriter | None
    ) -> None:
        if stdout_writer is not None:
            self._stream_writers[StreamType.STDOUT] = stdout_writer
            return

        if self._stream_writers.get(StreamType.STDOUT) is not None:
            return

        if self._stdout is None or self._stdout.closed:
            self._stdout = await self._dup_stdout()

        transport, protocol = await self._loop.connect_write_pipe(
            lambda: LoggerProtocol(), self._stdout
        )

        if has_uvloop:
            try:
                transport.close = patch_transport_close(transport, self._loop)
            except Exception:
                pass

        self._stream_writers[StreamType.STDOUT] = asyncio.StreamWriter(
            transport,
            protocol,
            None,
            self._loop,
        )

    async def _setup_stderr_writer(
        self, stderr_writer: asyncio.StreamWriter | None
    ) -> None:
        if stderr_writer is not None:
            self._stream_writers[StreamType.STDERR] = stderr_writer
            return

        if self._stream_writers.get(StreamType.STDERR) is not None:
            return

        if self._stderr is None or self._stderr.closed:
            self._stderr = await self._dup_stderr()

        transport, protocol = await self._loop.connect_write_pipe(
            lambda: LoggerProtocol(), self._stderr
        )

        if has_uvloop:
            try:
                transport.close = patch_transport_close(transport, self._loop)
            except Exception:
                pass

        self._stream_writers[StreamType.STDERR] = asyncio.StreamWriter(
            transport,
            protocol,
            None,
            self._loop,
        )

    def _get_file_lock(self, logfile_path: str) -> asyncio.Lock:
        if logfile_path not in self._file_locks:
            self._file_locks[logfile_path] = asyncio.Lock()
        return self._file_locks[logfile_path]

    def _get_read_lock(self, logfile_path: str) -> asyncio.Lock:
        if logfile_path not in self._read_locks:
            self._read_locks[logfile_path] = asyncio.Lock()
        return self._read_locks[logfile_path]

    async def open_file(
        self,
        filename: str,
        directory: str | None = None,
        is_default: bool = False,
        retention_policy: RetentionPolicyConfig | None = None,
    ):
        if self._cwd is None:
            self._cwd = await self._loop.run_in_executor(None, os.getcwd)

        logfile_path = self._to_logfile_path(filename, directory=directory)
        file_lock = self._get_file_lock(logfile_path)

        await file_lock.acquire()
        try:
            await self._loop.run_in_executor(None, self._open_file, logfile_path)
        finally:
            file_lock.release()

        if retention_policy and self._retention_policies.get(logfile_path) is None:
            policy = RetentionPolicy(retention_policy)
            policy.parse()
            self._retention_policies[logfile_path] = policy

        if is_default:
            self._default_logfile_path = logfile_path

    def _open_file(self, logfile_path: str):
        resolved_path = pathlib.Path(logfile_path).absolute().resolve()
        logfile_directory = str(resolved_path.parent)
        path = str(resolved_path)

        if not os.path.exists(logfile_directory):
            os.makedirs(logfile_directory)

        if not os.path.exists(path):
            resolved_path.touch()

        self._files[logfile_path] = open(path, "ab+")

    async def _rotate(self, logfile_path: str, retention_policy: RetentionPolicy):
        file_lock = self._get_file_lock(logfile_path)

        await file_lock.acquire()
        try:
            await self._loop.run_in_executor(
                None,
                self._rotate_logfile,
                retention_policy,
                logfile_path,
            )
        finally:
            file_lock.release()

    def _get_logfile_metadata(self, logfile_path: str) -> Dict[str, float]:
        resolved_path = pathlib.Path(logfile_path)
        logfile_metadata_path = os.path.join(
            str(resolved_path.parent.absolute().resolve()), ".logging.json"
        )

        if not os.path.exists(logfile_metadata_path):
            return {}

        with open(logfile_metadata_path, "rb") as metadata_file:
            return msgspec.json.decode(metadata_file.read())

    def _update_logfile_metadata(
        self,
        logfile_path: str,
        logfile_metadata: Dict[str, float],
    ):
        resolved_path = pathlib.Path(logfile_path)
        logfile_metadata_path = os.path.join(
            str(resolved_path.parent.absolute().resolve()), ".logging.json"
        )

        with open(logfile_metadata_path, "wb") as metadata_file:
            metadata_file.write(msgspec.json.encode(logfile_metadata))

    def _rotate_logfile(
        self,
        retention_policy: RetentionPolicy,
        logfile_path: str,
    ):
        resolved_path = pathlib.Path(logfile_path)
        path = str(resolved_path.absolute().resolve())

        logfile_metadata = self._get_logfile_metadata(logfile_path)

        current_time = datetime.datetime.now(datetime.UTC)
        current_timestamp = current_time.timestamp()

        created_time = logfile_metadata.get(logfile_path, current_timestamp)

        policy_data = {
            "file_age": (
                current_time
                - datetime.datetime.fromtimestamp(created_time, datetime.UTC)
            ).seconds,
            "file_size": os.path.getsize(logfile_path),
            "logfile_path": resolved_path,
        }

        if retention_policy.matches_policy(policy_data):
            logfile_metadata[logfile_path] = created_time
            self._update_logfile_metadata(logfile_path, logfile_metadata)
            return

        self._files[logfile_path].close()

        with open(logfile_path, "rb") as logfile:
            logfile_data = logfile.read()

        if len(logfile_data) == 0:
            logfile_metadata[logfile_path] = created_time
            self._update_logfile_metadata(logfile_path, logfile_metadata)
            return

        archived_filename = f"{resolved_path.stem}_{current_timestamp}_archived.zst"
        archive_path = os.path.join(
            str(resolved_path.parent.absolute().resolve()),
            archived_filename,
        )

        with open(archive_path, "wb") as archived_file:
            archived_file.write(self._compressor.compress(logfile_data))

        self._files[logfile_path] = open(path, "wb+")

        logfile_metadata[logfile_path] = current_timestamp
        self._update_logfile_metadata(logfile_path, logfile_metadata)

    async def close(self, shutdown_subscribed: bool = False):
        self._closing = True

        await self._stop_consumer(shutdown_subscribed)
        await self._drain_queue()
        await self._cleanup_batch_fsync()
        await self._close_all_files()
        await self._drain_writers()

        self._initialized = False
        self._closing = False

    async def _stop_consumer(self, shutdown_subscribed: bool) -> None:
        was_running = self._consumer.status == ConsumerStatus.RUNNING
        self._consumer.stop()

        if shutdown_subscribed:
            await self._provider.signal_shutdown()

        if was_running and self._consumer.pending:
            await self._consumer.wait_for_pending()

    async def _drain_queue(self) -> None:
        while not self._queue.empty():
            task = self._queue.get_nowait()
            await task

        for task in list(self._scheduled_tasks):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._scheduled_tasks.clear()

    async def _cleanup_batch_fsync(self) -> None:
        if self._batch_timer_handle:
            self._batch_timer_handle.cancel()
            self._batch_timer_handle = None

        if self._batch_flush_task and not self._batch_flush_task.done():
            self._batch_flush_task.cancel()
            try:
                await self._batch_flush_task
            except asyncio.CancelledError:
                pass

        if not self._pending_batch or not self._batch_lock:
            return

        async with self._batch_lock:
            for _, future in self._pending_batch:
                if not future.done():
                    future.set_result(None)
            self._pending_batch.clear()

    async def _close_all_files(self) -> None:
        await asyncio.gather(
            *[self._close_file(logfile_path) for logfile_path in self._files]
        )

    async def _drain_writers(self) -> None:
        await asyncio.gather(
            *[writer.drain() for writer in self._stream_writers.values()]
        )

    def abort(self):
        for logfile_path, logfile in self._files.items():
            if logfile and not logfile.closed:
                try:
                    logfile.close()
                except Exception:
                    pass

        self._consumer.abort()

        while not self._queue.empty():
            task = self._queue.get_nowait()
            task.set_result(None)

        for task in self._scheduled_tasks:
            if not task.done():
                task.cancel()

        self._scheduled_tasks.clear()

    async def close_file(
        self,
        filename: str,
        directory: str | None = None,
    ):
        if self._cwd is None:
            self._cwd = await self._loop.run_in_executor(None, os.getcwd)

        logfile_path = self._to_logfile_path(filename, directory=directory)
        await self._close_file(logfile_path)

    async def _close_file(self, logfile_path: str):
        file_lock = self._file_locks.get(logfile_path)
        if not file_lock:
            return

        await file_lock.acquire()
        try:
            await self._loop.run_in_executor(
                None,
                self._close_file_at_path,
                logfile_path,
            )

            read_file = self._read_files.get(logfile_path)
            if read_file and not read_file.closed:
                await self._loop.run_in_executor(None, read_file.close)
        finally:
            file_lock.release()

        self._files.pop(logfile_path, None)
        self._file_locks.pop(logfile_path, None)
        self._read_files.pop(logfile_path, None)
        self._read_locks.pop(logfile_path, None)

    def _close_file_at_path(self, logfile_path: str):
        logfile = self._files.get(logfile_path)
        if logfile and not logfile.closed:
            logfile.close()

    def _to_logfile_path(
        self,
        filename: str,
        directory: str | None = None,
    ):
        filename_path = pathlib.Path(filename)

        valid_extensions = {".json", ".wal", ".log", ".bin"}
        if filename_path.suffix not in valid_extensions:
            raise ValueError(
                f"Invalid log file extension '{filename_path.suffix}'. "
                f"Valid extensions: {valid_extensions}"
            )

        if self._config.directory:
            directory = self._config.directory
        elif directory is None:
            directory = str(self._cwd) if self._cwd else os.getcwd()

        return os.path.join(directory, str(filename_path))

    async def _dup_stdout(self):
        stdout_fileno = await self._loop.run_in_executor(None, sys.stderr.fileno)
        stdout_dup = await self._loop.run_in_executor(None, os.dup, stdout_fileno)

        return await self._loop.run_in_executor(
            None, functools.partial(os.fdopen, stdout_dup, mode=sys.stdout.mode)
        )

    async def _dup_stderr(self):
        stderr_fileno = await self._loop.run_in_executor(None, sys.stderr.fileno)
        stderr_dup = await self._loop.run_in_executor(None, os.dup, stderr_fileno)

        return await self._loop.run_in_executor(
            None, functools.partial(os.fdopen, stderr_dup, mode=sys.stderr.mode)
        )

    def schedule(
        self,
        entry: T,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        if self._closing:
            return

        task = asyncio.create_task(
            self.log(
                entry,
                template=template,
                path=path,
                retention_policy=retention_policy,
                filter=filter,
            )
        )

        self._scheduled_tasks.add(task)
        task.add_done_callback(self._scheduled_tasks.discard)

        try:
            self._queue.put_nowait(task)
        except asyncio.QueueFull:
            self._log_backpressure_warning()
            task.cancel()
            self._scheduled_tasks.discard(task)

    def _log_backpressure_warning(self) -> None:
        stream_writer = self._stream_writers.get(StreamType.STDOUT)
        if not stream_writer or stream_writer.is_closing():
            return

        timestamp = datetime.datetime.now(datetime.UTC).isoformat()
        warning = f"{timestamp} - WARN - LoggerStream queue full, dropping log entry\n"

        try:
            stream_writer.write(warning.encode())
        except Exception:
            pass

    async def log_prepared_batch(
        self,
        model_messages: dict[str, list[str]],
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        entries = [
            self._to_entry(message, name)
            for name, messages in model_messages.items()
            for message in messages
        ]

        if not entries:
            return

        await asyncio.gather(
            *[
                self.log(
                    entry,
                    template=template,
                    path=path,
                    retention_policy=retention_policy,
                    filter=filter,
                )
                for entry in entries
            ],
            return_exceptions=True,
        )

    async def batch(
        self,
        entries: list[T],
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        if not entries:
            return

        await asyncio.gather(
            *[
                self.log(
                    entry,
                    template=template,
                    path=path,
                    retention_policy=retention_policy,
                    filter=filter,
                )
                for entry in entries
            ],
            return_exceptions=True,
        )

    async def log_prepared(
        self,
        message: str,
        name: str = "default",
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        entry = self._to_entry(message, name)

        await self.log(
            entry,
            template=template,
            path=path,
            retention_policy=retention_policy,
            filter=filter,
        )

    async def log(
        self,
        entry: T,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ) -> int | None:
        filename, directory = self._parse_path(path)

        template = template or self._default_template
        filename = filename or self._default_logfile
        directory = directory or self._default_log_directory
        retention_policy = retention_policy or self._default_retention_policy

        if filename or directory:
            return await self._log_to_file(
                entry,
                filename=filename,
                directory=directory,
                retention_policy=retention_policy,
                filter=filter,
            )

        await self._log(entry, template=template, filter=filter)
        return None

    def _parse_path(self, path: str | None) -> tuple[str | None, str | None]:
        if not path:
            return None, None

        logfile_path = pathlib.Path(path)
        is_logfile = len(logfile_path.suffix) > 0

        filename = logfile_path.name if is_logfile else None
        directory = (
            str(logfile_path.parent.absolute())
            if is_logfile
            else str(logfile_path.absolute())
        )

        return filename, directory

    def _to_entry(self, message: str, name: str):
        model, defaults = self._models.get(name, self._models.get("default"))
        return model(message=message, **defaults)

    async def _log(
        self,
        entry_or_log: T | Log[T],
        template: str | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        if self._config.disabled:
            return

        entry = entry_or_log.entry if isinstance(entry_or_log, Log) else entry_or_log

        if not self._config.enabled(self._name, entry.level):
            return

        if filter and not filter(entry):
            return

        if self._initialized is None:
            await self.initialize()

        stream_writer = self._stream_writers[self._config.output]
        if stream_writer.is_closing():
            return

        template = (
            template
            or "{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}"
        )

        log_file, line_number, function_name = self._get_caller_info(entry_or_log)

        await self._ensure_stdio()
        await self._write_to_stream(
            entry, template, log_file, line_number, function_name, stream_writer
        )

    def _get_caller_info(self, entry_or_log: T | Log[T]) -> tuple[str, int, str]:
        if isinstance(entry_or_log, Log):
            return (
                entry_or_log.filename,
                entry_or_log.line_number,
                entry_or_log.function_name,
            )

        return self._find_caller()

    async def _ensure_stdio(self) -> None:
        if self._stdout is None or self._stdout.closed:
            self._stdout = await self._dup_stdout()

        if self._stderr is None or self._stderr.closed:
            self._stderr = await self._dup_stderr()

    async def _write_to_stream(
        self,
        entry: Entry,
        template: str,
        log_file: str,
        line_number: int,
        function_name: str,
        stream_writer: asyncio.StreamWriter,
    ) -> None:
        context = {
            "filename": log_file,
            "function_name": function_name,
            "line_number": line_number,
            "thread_id": threading.get_native_id(),
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }

        try:
            stream_writer.write(
                entry.to_template(template, context=context).encode() + b"\n"
            )
            await stream_writer.drain()
        except Exception as err:
            await self._log_error(entry, log_file, line_number, function_name, err)

    async def _log_error(
        self,
        entry: Entry,
        log_file: str,
        line_number: int,
        function_name: str,
        err: Exception,
    ) -> None:
        if self._stderr.closed:
            return

        error_template = "{timestamp} - {level} - {thread_id}.{filename}:{function_name}.{line_number} - {error}"
        context = {
            "filename": log_file,
            "function_name": function_name,
            "line_number": line_number,
            "error": str(err),
            "thread_id": threading.get_native_id(),
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }

        await self._loop.run_in_executor(
            None,
            self._stderr.write,
            entry.to_template(error_template, context=context),
        )

    async def _log_to_file(
        self,
        entry_or_log: T | Log[T],
        filename: str | None = None,
        directory: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ) -> int | None:
        if self._config.disabled:
            return None

        entry = entry_or_log.entry if isinstance(entry_or_log, Log) else entry_or_log

        if not self._config.enabled(self._name, entry.level):
            return None

        if filter and not filter(entry):
            return None

        logfile_path = await self._resolve_logfile_path(filename, directory)
        await self._ensure_file_open(logfile_path, filename, directory)

        if retention_policy:
            self._retention_policies[logfile_path] = retention_policy

        rotation_policy = self._retention_policies.get(logfile_path)
        if rotation_policy:
            await self._rotate(logfile_path, rotation_policy)

        log = self._prepare_log(entry_or_log)

        return await self._write_log_to_file(entry, log, logfile_path)

    async def _resolve_logfile_path(
        self, filename: str | None, directory: str | None
    ) -> str:
        if self._cwd is None:
            self._cwd = await self._loop.run_in_executor(None, os.getcwd)

        if filename and directory:
            return self._to_logfile_path(filename, directory=directory)

        if self._default_logfile_path:
            return self._default_logfile_path

        return os.path.join(str(self._cwd), "logs", "logs.json")

    async def _ensure_file_open(
        self, logfile_path: str, filename: str | None, directory: str | None
    ) -> None:
        existing_file = self._files.get(logfile_path)
        if existing_file and not existing_file.closed:
            return

        resolved_filename = filename or "logs.json"
        resolved_directory = directory or os.path.join(str(self._cwd), "logs")

        await self.open_file(resolved_filename, directory=resolved_directory)

    def _prepare_log(self, entry_or_log: T | Log[T]) -> Log[T]:
        if isinstance(entry_or_log, Log):
            return entry_or_log

        log_file, line_number, function_name = self._find_caller()

        return Log(
            entry=entry_or_log,
            filename=log_file,
            function_name=function_name,
            line_number=line_number,
        )

    async def _write_log_to_file(
        self, entry: Entry, log: Log[T], logfile_path: str
    ) -> int | None:
        file_lock = self._get_file_lock(logfile_path)

        await file_lock.acquire()
        try:
            lsn = await self._loop.run_in_executor(
                None,
                self._write_to_file,
                log,
                logfile_path,
                self._durability,
            )
        except Exception as err:
            log_file, line_number, function_name = self._find_caller()
            await self._log_error(entry, log_file, line_number, function_name, err)
            return None
        finally:
            file_lock.release()

        if self._durability == DurabilityMode.FSYNC_BATCH:
            await self._schedule_batch_fsync(logfile_path)

        await asyncio.sleep(0)
        return lsn

    def _write_to_file(
        self,
        log: Log[T],
        logfile_path: str,
        durability: DurabilityMode | None = None,
    ) -> int | None:
        durability = durability or self._durability

        logfile = self._files.get(logfile_path)
        if not logfile or logfile.closed:
            return None

        lsn = self._generate_lsn(log)
        data = self._encode_log(log, lsn)

        logfile.write(data)
        self._sync_file(logfile, durability)

        return lsn

    def _generate_lsn(self, log: Log[T]) -> int | None:
        if not self._enable_lsn:
            return None

        if self._lamport_clock is not None:
            lsn_obj = self._lamport_clock.generate()
            lsn = lsn_obj.to_int()
            log.lsn = lsn
            return lsn

        if self._sequence_generator is not None:
            lsn = self._sequence_generator.generate()
            if lsn is not None:
                log.lsn = lsn
            return lsn

        return None

    def _encode_log(self, log: Log[T], lsn: int | None) -> bytes:
        if self._log_format == "binary":
            return self._encode_binary(log, lsn)

        return msgspec.json.encode(log) + b"\n"

    def _sync_file(self, logfile: io.FileIO, durability: DurabilityMode) -> None:
        match durability:
            case DurabilityMode.NONE:
                pass
            case DurabilityMode.FLUSH | DurabilityMode.FSYNC_BATCH:
                logfile.flush()
            case DurabilityMode.FSYNC:
                logfile.flush()
                os.fsync(logfile.fileno())

    def _encode_binary(self, log: Log[T], lsn: int | None) -> bytes:
        payload = msgspec.json.encode(log)
        lsn_value = lsn if lsn is not None else 0

        if self._lamport_clock is not None:
            lsn_high = (lsn_value >> 64) & 0xFFFFFFFFFFFFFFFF
            lsn_low = lsn_value & 0xFFFFFFFFFFFFFFFF
            header = struct.pack("<IQQ", len(payload), lsn_high, lsn_low)
        else:
            header = struct.pack("<IQ", len(payload), lsn_value)

        crc = zlib.crc32(header + payload) & 0xFFFFFFFF

        return struct.pack("<I", crc) + header + payload

    def _decode_binary(self, data: bytes) -> tuple[Log[T], int]:
        if len(data) < BINARY_HEADER_SIZE_V1:
            raise ValueError(f"Entry too short: {len(data)} < {BINARY_HEADER_SIZE_V1}")

        crc_stored = struct.unpack("<I", data[:4])[0]

        if len(data) >= BINARY_HEADER_SIZE and self._lamport_clock is not None:
            length, lsn_high, lsn_low = struct.unpack("<IQQ", data[4:24])
            lsn = (lsn_high << 64) | lsn_low
            header_size = BINARY_HEADER_SIZE
        else:
            length, lsn = struct.unpack("<IQ", data[4:16])
            header_size = BINARY_HEADER_SIZE_V1

        if len(data) < header_size + length:
            raise ValueError(
                f"Truncated entry: have {len(data)}, need {header_size + length}"
            )

        crc_computed = zlib.crc32(data[4 : header_size + length]) & 0xFFFFFFFF
        if crc_stored != crc_computed:
            raise ValueError(
                f"CRC mismatch: stored={crc_stored:#x}, computed={crc_computed:#x}"
            )

        payload = data[header_size : header_size + length]
        log = msgspec.json.decode(payload, type=Log)

        return log, lsn

    def _find_caller(self):
        frame = sys._getframe(3)
        code = frame.f_code

        return (code.co_filename, frame.f_lineno, code.co_name)

    async def get(self, filter: Callable[[T], bool] | None = None):
        async for log in self._consumer.iter_logs(filter=filter):
            yield log

    async def put(self, entry: T | Log[T]):
        if isinstance(entry, Log):
            await self._provider.put(entry)
            return

        frame = sys._getframe(1)
        code = frame.f_code

        log_entry = Log(
            entry=entry,
            filename=code.co_filename,
            function_name=code.co_name,
            line_number=frame.f_lineno,
            thread_id=threading.get_native_id(),
            timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
        )

        await self._provider.put(log_entry)

    async def read_entries(
        self,
        logfile_path: str,
        from_offset: int = 0,
    ) -> AsyncIterator[tuple[int, Log[T], int | None]]:
        read_lock = self._get_read_lock(logfile_path)

        await read_lock.acquire()
        try:
            async for result in self._read_entries_impl(logfile_path, from_offset):
                yield result
        finally:
            read_lock.release()

    async def _read_entries_impl(
        self,
        logfile_path: str,
        from_offset: int,
    ) -> AsyncIterator[tuple[int, Log[T], int | None]]:
        read_file = await self._loop.run_in_executor(
            None,
            functools.partial(open, logfile_path, "rb"),
        )

        try:
            await self._loop.run_in_executor(None, read_file.seek, from_offset)
            offset = from_offset
            entries_yielded = 0

            while True:
                result = await self._read_single_entry(read_file, offset)
                if result is None:
                    break

                offset, log, lsn, entry_size = result
                yield offset, log, lsn
                offset += entry_size

                entries_yielded += 1
                if entries_yielded % 100 == 0:
                    await asyncio.sleep(0)
        finally:
            await self._loop.run_in_executor(None, read_file.close)

    async def _read_single_entry(
        self, read_file: io.FileIO, offset: int
    ) -> tuple[int, Log[T], int | None, int] | None:
        if self._log_format == "binary":
            return await self._read_binary_entry(read_file, offset)

        return await self._read_json_entry(read_file, offset)

    async def _read_binary_entry(
        self, read_file: io.FileIO, offset: int
    ) -> tuple[int, Log[T], int | None, int] | None:
        header = await self._loop.run_in_executor(
            None, read_file.read, BINARY_HEADER_SIZE
        )

        if len(header) == 0:
            return None

        if len(header) < BINARY_HEADER_SIZE:
            raise ValueError(f"Truncated header at offset {offset}")

        length = struct.unpack("<I", header[4:8])[0]
        payload = await self._loop.run_in_executor(None, read_file.read, length)

        if len(payload) < length:
            raise ValueError(f"Truncated payload at offset {offset}")

        log, lsn = self._decode_binary(header + payload)
        entry_size = BINARY_HEADER_SIZE + length

        return offset, log, lsn, entry_size

    async def _read_json_entry(
        self, read_file: io.FileIO, offset: int
    ) -> tuple[int, Log[T], int | None, int] | None:
        line = await self._loop.run_in_executor(None, read_file.readline)

        if not line:
            return None

        log = msgspec.json.decode(line.rstrip(b"\n"), type=Log)
        new_offset = await self._loop.run_in_executor(None, read_file.tell)
        entry_size = new_offset - offset

        return offset, log, log.lsn, entry_size

    async def get_last_lsn(self, logfile_path: str) -> int | None:
        last_lsn: int | None = None

        try:
            async for _offset, _log, lsn in self.read_entries(logfile_path):
                if lsn is not None:
                    last_lsn = lsn
        except (FileNotFoundError, ValueError):
            pass

        return last_lsn

    async def _recover_clock_from_wal(self, wal_path: str) -> None:
        if self._lamport_clock is None:
            return

        last_lsn_int = await self.get_last_lsn(wal_path)
        if last_lsn_int is None:
            return

        last_lsn = LSN.from_int(last_lsn_int)
        self._lamport_clock = HybridLamportClock.recover(
            node_id=self._instance_id,
            last_lsn=last_lsn,
        )

    async def _schedule_batch_fsync(self, logfile_path: str) -> asyncio.Future[None]:
        if self._closing:
            future = self._loop.create_future()
            future.set_result(None)
            return future

        if self._batch_lock is None:
            self._batch_lock = asyncio.Lock()

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        future: asyncio.Future[None] = self._loop.create_future()

        async with self._batch_lock:
            if len(self._pending_batch) >= self._batch_max_size:
                future.set_result(None)
                return future

            self._pending_batch.append((logfile_path, future))

            if len(self._pending_batch) == 1:
                self._batch_timer_handle = self._loop.call_later(
                    self._batch_timeout_ms / 1000.0,
                    self._trigger_batch_flush,
                    logfile_path,
                )

            should_flush = len(self._pending_batch) >= self._batch_max_size

        if should_flush:
            if self._batch_timer_handle:
                self._batch_timer_handle.cancel()
                self._batch_timer_handle = None
            await self._flush_batch(logfile_path)

        return future

    def _trigger_batch_flush(self, logfile_path: str) -> None:
        if self._closing:
            return

        if self._batch_flush_task and not self._batch_flush_task.done():
            return

        self._batch_flush_task = asyncio.create_task(self._flush_batch(logfile_path))

    async def _flush_batch(self, logfile_path: str) -> None:
        if not self._batch_lock:
            return

        async with self._batch_lock:
            if not self._pending_batch:
                return

            if self._batch_timer_handle:
                self._batch_timer_handle.cancel()
                self._batch_timer_handle = None

            logfile = self._files.get(logfile_path)
            if logfile and not logfile.closed:
                await self._loop.run_in_executor(None, os.fsync, logfile.fileno())

            for _, future in self._pending_batch:
                if not future.done():
                    future.set_result(None)

            self._pending_batch.clear()
