"""
Pytest configuration for integration tests.

Configures pytest-asyncio for async test support.
"""

import asyncio
import pytest
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

