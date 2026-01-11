import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream


@pytest.fixture(scope="function")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def configure_log_level():
    config = LoggingConfig()
    config.update(log_level="debug")
    yield
    config.update(log_level="error")


@pytest.fixture
def temp_log_directory(tmp_path) -> str:
    return str(tmp_path)


@pytest.fixture
def sample_entry() -> Entry:
    return Entry(
        message="Test log message",
        level=LogLevel.INFO,
    )


@pytest.fixture
def sample_entry_factory():
    def create_entry(
        message: str = "Test log message",
        level: LogLevel = LogLevel.INFO,
    ) -> Entry:
        return Entry(message=message, level=level)

    return create_entry


def create_mock_stream_writer() -> MagicMock:
    mock_writer = MagicMock(spec=asyncio.StreamWriter)
    mock_writer.write = MagicMock()
    mock_writer.drain = AsyncMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()
    mock_writer.is_closing = MagicMock(return_value=False)
    return mock_writer


@pytest.fixture
def mock_stdout_writer() -> MagicMock:
    return create_mock_stream_writer()


@pytest.fixture
def mock_stderr_writer() -> MagicMock:
    return create_mock_stream_writer()


@pytest.fixture
async def json_logger_stream(
    temp_log_directory: str,
    mock_stdout_writer: MagicMock,
    mock_stderr_writer: MagicMock,
) -> AsyncGenerator[LoggerStream, None]:
    stream = LoggerStream(
        name="test_json",
        filename="test.json",
        directory=temp_log_directory,
        durability=DurabilityMode.FLUSH,
        log_format="json",
        enable_lsn=True,
        instance_id=1,
    )
    await stream.initialize(
        stdout_writer=mock_stdout_writer,
        stderr_writer=mock_stderr_writer,
    )
    yield stream
    await stream.close()


@pytest.fixture
async def binary_logger_stream(
    temp_log_directory: str,
    mock_stdout_writer: MagicMock,
    mock_stderr_writer: MagicMock,
) -> AsyncGenerator[LoggerStream, None]:
    stream = LoggerStream(
        name="test_binary",
        filename="test.wal",
        directory=temp_log_directory,
        durability=DurabilityMode.FLUSH,
        log_format="binary",
        enable_lsn=True,
        instance_id=1,
    )
    await stream.initialize(
        stdout_writer=mock_stdout_writer,
        stderr_writer=mock_stderr_writer,
    )
    yield stream
    await stream.close()


@pytest.fixture
async def fsync_logger_stream(
    temp_log_directory: str,
    mock_stdout_writer: MagicMock,
    mock_stderr_writer: MagicMock,
) -> AsyncGenerator[LoggerStream, None]:
    stream = LoggerStream(
        name="test_fsync",
        filename="test_fsync.wal",
        directory=temp_log_directory,
        durability=DurabilityMode.FSYNC,
        log_format="binary",
        enable_lsn=True,
        instance_id=1,
    )
    await stream.initialize(
        stdout_writer=mock_stdout_writer,
        stderr_writer=mock_stderr_writer,
    )
    yield stream
    await stream.close()


@pytest.fixture
async def batch_fsync_logger_stream(
    temp_log_directory: str,
    mock_stdout_writer: MagicMock,
    mock_stderr_writer: MagicMock,
) -> AsyncGenerator[LoggerStream, None]:
    stream = LoggerStream(
        name="test_batch_fsync",
        filename="test_batch.wal",
        directory=temp_log_directory,
        durability=DurabilityMode.FSYNC_BATCH,
        log_format="binary",
        enable_lsn=True,
        instance_id=1,
    )
    await stream.initialize(
        stdout_writer=mock_stdout_writer,
        stderr_writer=mock_stderr_writer,
    )
    yield stream
    await stream.close()


@pytest.fixture
async def no_lsn_logger_stream(
    temp_log_directory: str,
    mock_stdout_writer: MagicMock,
    mock_stderr_writer: MagicMock,
) -> AsyncGenerator[LoggerStream, None]:
    stream = LoggerStream(
        name="test_no_lsn",
        filename="test_no_lsn.json",
        directory=temp_log_directory,
        durability=DurabilityMode.FLUSH,
        log_format="json",
        enable_lsn=False,
        instance_id=0,
    )
    await stream.initialize(
        stdout_writer=mock_stdout_writer,
        stderr_writer=mock_stderr_writer,
    )
    yield stream
    await stream.close()
