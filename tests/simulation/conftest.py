"""
Pytest configuration for simulation harness scenarios.

Scenarios are written as `async def` and run via `pytest-asyncio`. Each
scenario receives a `ClusterHarness` configured for its level (L1/L2/L3)
through fixtures defined here.

The harness fixtures are intentionally module-scoped function fixtures
(no session-wide reuse) so every scenario gets a clean cluster, baseline
PID snapshot, and port allocation. That is the whole point of the
supervisor — share nothing across scenarios.

Logging note: pytest captures stdout, which the Logger's stdout-pipe
transport refuses to attach to (it requires a real TTY/pipe/socket).
We disable the global logger here for the simulation suite. Phase 2's
DiagnosticDumper will configure file-backed logging for failure dumps.
"""

import pytest

from hyperscale.logging.config.logging_config import LoggingConfig


@pytest.fixture(autouse=True, scope="session")
def _disable_logger_for_simulation():
    """Disable the project's stdout/stderr pipe-based logger globally.

    The Logger calls `loop.connect_write_pipe(LoggerProtocol(), self._stdout)`
    which requires stdout to be a pipe/socket/character device. Under pytest
    capture stdout is a regular file and pipe-transport setup raises. Disable
    rather than try to reproduce a tty here.
    """
    config = LoggingConfig()
    config.disable()
    yield
    config.enable()


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers", "simulation: distributed simulation harness scenarios"
    )


@pytest.fixture
def stabilization_seconds() -> float:
    """Override per-scenario by reparametrizing this fixture.

    Phase 1 keeps a real wall-clock pause; Phase 2 replaces this with
    condition-driven `wait_until` predicates that finish as soon as the
    cluster is actually ready.
    """
    return 8.0
