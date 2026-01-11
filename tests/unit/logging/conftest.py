import asyncio
import os
import tempfile
from typing import AsyncGenerator

import pytest
from typing import Generator

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream


@pytest.fixture(scope="function")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_log_directory() -> Generator[str, None]:
    with tempfile.TemporaryDirectory() as temp_directory:
        yield temp_directory


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


@pytest.fixture
async def json_logger_stream(
    temp_log_directory: str,
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
    await stream.initialize()
    yield stream
    await stream.close()


@pytest.fixture
async def binary_logger_stream(
    temp_log_directory: str,
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
    await stream.initialize()
    yield stream
    await stream.close()


@pytest.fixture
async def fsync_logger_stream(
    temp_log_directory: str,
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
    await stream.initialize()
    yield stream
    await stream.close()


@pytest.fixture
async def batch_fsync_logger_stream(
    temp_log_directory: str,
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
    await stream.initialize()
    yield stream
    await stream.close()


@pytest.fixture
async def no_lsn_logger_stream(
    temp_log_directory: str,
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
    await stream.initialize()
    yield stream
    await stream.close()
