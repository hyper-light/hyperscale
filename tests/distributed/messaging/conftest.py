"""
Shared fixtures for message_handling tests.
"""

import asyncio

import pytest

from tests.integration.messaging.mocks import (
    MockServerInterface,
    MockLeaderState,
)


@pytest.fixture
def mock_server() -> MockServerInterface:
    """Create a mock server interface for testing."""
    return MockServerInterface()


@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
