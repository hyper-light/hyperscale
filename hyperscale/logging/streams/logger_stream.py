import asyncio
import datetime
import io
import os
import pathlib
import sys
import threading
import uuid
from collections import defaultdict
from typing import (
    Callable,
    Dict,
    List,
    TypeVar,
)

import msgspec
import zstandard

from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.models import Entry, Log, LogLevel
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
from .stream_type import StreamType

T = TypeVar("T", bound=Entry)


def patch_transport_close(
    transport: asyncio.Transport,
    loop: asyncio.AbstractEventLoop,
):
    def close(*args, **kwargs):
        if loop.is_closed() is False:
            transport.close()

    return close


class LoggerStream:
    def __init__(
        self,
        name: str | None = None,
        template: str | None = None,
        filename: str | None = None,
        directory: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
    ) -> None:
        if name is None:
            name = "default"

        self._name = name
        self._default_template = template
        self._default_logfile = filename
        self._default_log_directory = directory

        self._default_retention_policy = retention_policy
        if retention_policy:
            self._default_retention_policy = RetentionPolicy(retention_policy)
            self._default_retention_policy.parse()

        self._stdout: io.TextIO | None = None
        self._stderr: io.TextIO | None = None
        self._init_lock = asyncio.Lock()
        self._stream_writers: Dict[StreamType, asyncio.StreamWriter] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generator: SnowflakeGenerator | None = None
        self._compressor: zstandard.ZstdCompressor | None = None

        self._files: Dict[str, io.FileIO] = {}
        self._file_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._cwd: str | None = None
        self._default_logfile_path: str | None = None

        self._retention_policies: Dict[str, RetentionPolicy] = {}

        self._config = LoggingConfig()
        self._initialized: bool = False
        self._consumer: LogConsumer | None = None
        self._provider: LogProvider | None = None
        self._initialized: bool = False
        self._closed = False
        self._stderr: io.TextIOBase | None = None
        self._stdout: io.TextIOBase | None = None
        self._transports: List[asyncio.Transport] = []

    @property
    def has_active_subscriptions(self):
        return self._provider.subscriptions_count > 0

    async def initialize(self) -> asyncio.StreamWriter:
        async with self._init_lock:
            if self._initialized:
                return

            if self._generator is None:
                self._generator = SnowflakeGenerator(
                    (uuid.uuid1().int + threading.get_native_id()) >> 64
                )

            if self._compressor is None:
                self._compressor = zstandard.ZstdCompressor()

            if self._loop is None:
                self._loop = asyncio.get_event_loop()

            if self._consumer is None:
                self._consumer = LogConsumer()

            if self._provider is None:
                self._provider = LogProvider()

            if self._stdout is None:
                self._stdout = sys.stdout

            if self._stderr is None:
                self._stderr = sys.stderr

            if self._stream_writers.get(StreamType.STDOUT) is None:
                transport, protocol = await self._loop.connect_write_pipe(
                    lambda: LoggerProtocol(), self._stdout
                )

                transport.close = patch_transport_close(transport, self._loop)

                self._stream_writers[StreamType.STDOUT] = asyncio.StreamWriter(
                    transport,
                    protocol,
                    None,
                    self._loop,
                )

            if self._stream_writers.get(StreamType.STDERR) is None:
                transport, protocol = await self._loop.connect_write_pipe(
                    lambda: LoggerProtocol(), self._stderr
                )

                transport.close = patch_transport_close(transport, self._loop)

                self._stream_writers[StreamType.STDERR] = asyncio.StreamWriter(
                    transport,
                    protocol,
                    None,
                    self._loop,
                )

            self._initialized = True

    async def open_file(
        self,
        filename: str,
        directory: str | None = None,
        is_default: bool = False,
        retention_policy: RetentionPolicyConfig | None = None,
    ):
        if self._cwd is None:
            self._cwd = await asyncio.to_thread(os.getcwd)

        logfile_path = self._to_logfile_path(filename, directory=directory)
        await self._file_locks[logfile_path].acquire()

        await asyncio.to_thread(
            self._open_file,
            logfile_path,
        )

        self._file_locks[logfile_path].release()

        if retention_policy and self._retention_policies.get(logfile_path) is None:
            policy = RetentionPolicy(retention_policy)
            policy.parse()

            self._retention_policies[logfile_path] = policy

        if is_default:
            self._default_logfile_path = logfile_path

    def _open_file(
        self,
        logfile_path: str,
    ):
        resolved_path = pathlib.Path(logfile_path).absolute().resolve()
        logfile_directory = str(resolved_path.parent)
        path = str(resolved_path)

        if not os.path.exists(logfile_directory):
            os.makedirs(logfile_directory)

        if not os.path.exists(path):
            resolved_path.touch()

        self._files[logfile_path] = open(path, "ab+")

    async def _rotate(self, logfile_path: str, retention_policy: RetentionPolicy):
        await self._file_locks[logfile_path].acquire()
        await asyncio.to_thread(
            self._rotate_logfile,
            retention_policy,
            logfile_path,
        )

        self._file_locks[logfile_path].release()

    def _get_logfile_metadata(self, logfile_path: str) -> Dict[str, float]:
        resolved_path = pathlib.Path(logfile_path)

        logfile_metadata_path = os.path.join(
            str(resolved_path.parent.absolute().resolve()), ".logging.json"
        )

        if os.path.exists(logfile_metadata_path):
            metadata_file = open(logfile_metadata_path, "+rb")
            return msgspec.json.decode(metadata_file.read())

        return {}

    def _update_logfile_metadata(
        self,
        logfile_path: str,
        logfile_metadata: Dict[str, float],
    ):
        resolved_path = pathlib.Path(logfile_path)

        logfile_metadata_path = os.path.join(
            str(resolved_path.parent.absolute().resolve()), ".logging.json"
        )

        with open(logfile_metadata_path, "+wb") as metadata_file:
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

        created_time = logfile_metadata.get(
            logfile_path,
            current_timestamp,
        )

        archived_filename = f"{resolved_path.stem}_{current_timestamp}_archived.zst"
        logfile_data = b""

        if (
            retention_policy.matches_policy(
                {
                    "file_age": (
                        current_time
                        - datetime.datetime.fromtimestamp(created_time, datetime.UTC)
                    ).seconds,
                    "file_size": os.path.getsize(logfile_path),
                    "logfile_path": resolved_path,
                }
            )
            is False
        ):
            self._files[logfile_path].close()

            with open(logfile_path, "rb") as logfile:
                logfile_data = logfile.read()

        if len(logfile_data) > 0:
            archive_path = os.path.join(
                str(resolved_path.parent.absolute().resolve()),
                archived_filename,
            )

            with open(archive_path, "wb") as archived_file:
                archived_file.write(self._compressor.compress(logfile_data))

            self._files[logfile_path] = open(path, "wb+")
            created_time = current_timestamp

        logfile_metadata[logfile_path] = created_time

        self._update_logfile_metadata(logfile_path, logfile_metadata)

    async def close(self, shutdown_subscribed: bool = False):
        self._consumer.stop()

        if shutdown_subscribed:
            await self._provider.signal_shutdown()

        if (
            self._consumer.status
            in [
                ConsumerStatus.RUNNING,
                ConsumerStatus.CLOSING,
            ]
            and self._consumer.pending
        ):
            await self._consumer.wait_for_pending()

        await asyncio.gather(
            *[self._close_file(logfile_path) for logfile_path in self._files]
        )

        await asyncio.gather(
            *[writer.drain() for writer in self._stream_writers.values()]
        )

        self._initialized = False

    def abort(self):
        for logfile_path in self._files:
            if (logfile := self._files.get(logfile_path)) and logfile.closed is False:
                logfile.close()

        self._stderr.flush()
        self._stdout.flush()

        self._consumer.abort()

    async def close_file(
        self,
        filename: str,
        directory: str | None = None,
    ):
        if self._cwd is None:
            self._cwd = await asyncio.to_thread(os.getcwd)

        logfile_path = self._to_logfile_path(filename, directory=directory)
        await self._close_file(logfile_path)

    async def _close_file(self, logfile_path: str):
        if file_lock := self._file_locks.get(logfile_path):
            await file_lock.acquire()
            await asyncio.to_thread(
                self._close_file_at_path,
                logfile_path,
            )

            file_lock.release()

    def _close_file_at_path(self, logfile_path: str):
        if (logfile := self._files.get(logfile_path)) and logfile.closed is False:
            logfile.close()

    def _to_logfile_path(
        self,
        filename: str,
        directory: str | None = None,
    ):
        filename_path = pathlib.Path(filename)

        assert (
            filename_path.suffix == ".json"
        ), "Err. - file must be JSON file for logs."

        if directory is None:
            directory: str = os.path.join(self._cwd, "logs")

        logfile_path: str = os.path.join(directory, filename_path)

        return logfile_path

    async def log(
        self,
        entry: T,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        filename: str | None = None
        directory: str | None = None

        if path:
            logfile_path = pathlib.Path(path)
            is_logfile = len(logfile_path.suffix) > 0

            filename = logfile_path.name if is_logfile else None
            directory = (
                str(logfile_path.parent.absolute())
                if is_logfile
                else str(logfile_path.absolute())
            )

        if template is None:
            template = self._default_template

        if filename is None:
            filename = self._default_logfile

        if directory is None:
            directory = self._default_log_directory

        if retention_policy is None:
            retention_policy = self._default_retention_policy

        if filename or directory:
            await self._log_to_file(
                entry,
                filename=filename,
                directory=directory,
                retention_policy=retention_policy,
                filter=filter,
            )

        else:
            await self._log(
                entry,
                template=template,
                filter=filter,
            )

    async def _log(
        self,
        entry_or_log: T | Log[T],
        template: str | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        entry: Entry = None
        if isinstance(entry_or_log, Log):
            entry = entry_or_log.entry

        else:
            entry = entry_or_log

        stream = (
            StreamType.STDOUT
            if entry.level
            in [
                LogLevel.DEBUG,
                LogLevel.INFO,
                LogLevel.ERROR,
            ]
            else StreamType.STDERR
        )

        if self._config.enabled(self._name, entry.level) is False:
            return

        if filter and filter(entry) is False:
            return

        if self._initialized is None:
            await self.initialize()

        stream_writer = self._stream_writers[stream]

        if stream_writer.is_closing():
            return

        if template is None:
            template = "{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}"

        if isinstance(entry_or_log, Log):
            log_file = entry_or_log.filename
            line_number = entry_or_log.line_number
            function_name = entry_or_log.function_name

        else:
            log_file, line_number, function_name = self._find_caller()

        try:
            stream_writer.write(
                entry.to_template(
                    template,
                    context={
                        "filename": log_file,
                        "function_name": function_name,
                        "line_number": line_number,
                        "thread_id": threading.get_native_id(),
                        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                    },
                ).encode()
                + b"\n"
            )

            await stream_writer.drain()

        except Exception as err:
            error_template = "{timestamp} - {level} - {thread_id}.{filename}:{function_name}.{line_number} - {error}"

            if self._stderr.closed is False:
                await asyncio.to_thread(
                    self._stderr.write,
                    entry.to_template(
                        error_template,
                        context={
                            "filename": log_file,
                            "function_name": function_name,
                            "line_number": line_number,
                            "error": str(err),
                            "thread_id": threading.get_native_id(),
                            "timestamp": datetime.datetime.now(
                                datetime.UTC
                            ).isoformat(),
                        },
                    ),
                )

    async def _log_to_file(
        self,
        entry_or_log: T | Log[T],
        filename: str | None = None,
        directory: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
    ):
        entry: Entry = None
        if isinstance(entry_or_log, Log):
            entry = entry_or_log.entry

        else:
            entry = entry_or_log

        if self._config.enabled(self._name, entry.level) is False:
            return

        if filter and filter(entry) is False:
            return

        if self._cwd is None:
            self._cwd = await asyncio.to_thread(os.getcwd)

        if filename and directory:
            logfile_path = self._to_logfile_path(
                filename,
                directory=directory,
            )

        elif self._default_logfile_path:
            logfile_path = self._default_logfile_path

        else:
            filename = "logs.json"
            directory = os.path.join(self._cwd, "logs")
            logfile_path = os.path.join(directory, filename)

        if self._files.get(logfile_path) is None:
            await self.open_file(
                filename,
                directory=directory,
            )

        if retention_policy:
            self._retention_policies[logfile_path] = retention_policy

        if retention_policy := self._retention_policies.get(logfile_path):
            await self._rotate(
                logfile_path,
                retention_policy,
            )

        if isinstance(entry_or_log, Log):
            log_file = entry_or_log.filename
            line_number = entry_or_log.line_number
            function_name = entry_or_log.function_name

            log = entry_or_log

        else:
            log_file, line_number, function_name = self._find_caller()

            log = Log(
                entry=entry,
                filename=log_file,
                function_name=function_name,
                line_number=line_number,
            )

        try:
            await self._file_locks[logfile_path].acquire()

            await asyncio.to_thread(
                self._write_to_file,
                log,
                logfile_path,
            )

            await asyncio.sleep(0)

            self._file_locks[logfile_path].release()

        except Exception as err:
            error_template = "{timestamp} - {level} - {thread_id}.{filename}:{function_name}.{line_number} - {error}"

            if self._stderr.closed is False:
                await asyncio.to_thread(
                    self._stderr.write,
                    entry.to_template(
                        error_template,
                        context={
                            "filename": log_file,
                            "function_name": function_name,
                            "line_number": line_number,
                            "error": str(err),
                            "thread_id": threading.get_native_id(),
                            "timestamp": datetime.datetime.now(
                                datetime.UTC
                            ).isoformat(),
                        },
                    ),
                )

    def _write_to_file(
        self,
        entry: Entry,
        logfile_path: str,
    ):
        if (logfile := self._files.get(logfile_path)) and (logfile.closed is False):
            logfile.write(msgspec.json.encode(entry) + b"\n")

    def _find_caller(self):
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.
        """
        frame = sys._getframe(3)
        code = frame.f_code

        return (
            code.co_filename,
            frame.f_lineno,
            code.co_name,
        )

    async def get(self, filter: Callable[[T], bool] | None = None):
        async for log in self._consumer.iter_logs(filter=filter):
            yield log

    async def put(
        self,
        entry: T | Log[T],
    ):
        if not isinstance(entry, Log):
            frame = sys._getframe(1)
            code = frame.f_code
            entry = Log(
                entry=entry,
                filename=code.co_filename,
                function_name=code.co_name,
                line_number=frame.f_lineno,
                thread_id=threading.get_native_id(),
                timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
            )

        await self._provider.put(entry)
