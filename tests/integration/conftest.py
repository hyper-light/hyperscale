"""
Pytest configuration for integration tests.

Configures pytest-asyncio for async test support.
"""

import asyncio
import pytest


# Configure pytest-asyncio mode in pytest.ini or pyproject.toml is preferred,
# but we can also set a default loop policy here.


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    )


@pytest.fixture(scope="session")
def event_loop_policy():
    """Use the default event loop policy."""
    return asyncio.get_event_loop_policy()


@pytest.fixture(scope="function")
def event_loop():
    """Create an event loop for each test function."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
