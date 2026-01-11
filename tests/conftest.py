"""
Pytest configuration for integration tests.

Configures pytest-asyncio for async test support.
"""

import asyncio
import pytest
import tempfile

from typing import Generator, AsyncGenerator

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream


from tests.unit.distributed.messaging.mocks import MockServerInterface


# Configure pytest-asyncio mode in pytest.ini or pyproject.toml is preferred,
# but we can also set a default loop policy here.


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )

@pytest.fixture(scope="function")
def event_loop():
    """Create an event loop for each test function."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_server() -> MockServerInterface:
    """Create a mock server interface for testing."""
    return MockServerInterface()

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
