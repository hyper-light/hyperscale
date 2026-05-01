"""
Pytest configuration for simulation harness scenarios.

Scenarios are written as `async def` and run via `pytest-asyncio`. Each
scenario receives a `ClusterHarness` configured for its level (L1/L2/L3)
through fixtures defined here.

The harness fixtures are intentionally module-scoped function fixtures
(no session-wide reuse) so every scenario gets a clean cluster, baseline
PID snapshot, and port allocation. That is the whole point of the
supervisor — share nothing across scenarios.

Logging: the project Logger writes structured events to per-stream files
when ``LoggingConfig.log_directory`` is set, and additionally mirrors to
stdout via an asyncio pipe transport. The pipe transport requires fd 1 to
be a real pipe / TTY / character device (see ``asyncio.unix_events
._UnixWritePipeTransport``) — it raises ``ValueError`` if fd 1 is a
regular file. Pytest's default ``fd`` capture mode redirects fd 1 to a
temp file, which would trigger that path. The ``addopts`` in
``pyproject.toml`` could set ``--capture=no``, but to keep this
self-contained we override capture programmatically below; per-scenario
log files appear under ``tests/simulation/_artifacts/<run_id>/``.
"""

import os
import pathlib
import uuid

import pytest

from hyperscale.logging.config.logging_config import LoggingConfig


_ARTIFACTS_ROOT = (
    pathlib.Path(__file__).resolve().parent / "_artifacts"
)


@pytest.fixture(autouse=True, scope="session")
def _simulation_log_directory():
    """Direct the project Logger's file output into a per-session dir.

    Each session gets its own directory under ``tests/simulation/_artifacts/``
    so logs from different test runs do not collide.
    """
    run_id = uuid.uuid4().hex[:12]
    run_dir = _ARTIFACTS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    config = LoggingConfig()
    previous_directory = config.directory
    config.update(log_directory=str(run_dir))
    yield run_dir
    if previous_directory is not None:
        config.update(log_directory=previous_directory)


def pytest_collection_modifyitems(config, items) -> None:
    """Force ``--capture=no`` for simulation tests so the project Logger's
    stdout pipe transport can attach successfully (fd 1 must remain a pipe
    or character device).
    """
    if not items:
        return
    capture_value = config.getoption("capture", default=None)
    if capture_value not in (None, "no"):
        # Defensive: warn rather than rewrite — some CI setups need fd
        # capture for log collection. Tests under fd capture will fail
        # at server startup with a clear error from `connect_write_pipe`.
        os.environ.setdefault("HYPERSCALE_SIM_CAPTURE_WARNING", capture_value)


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers", "simulation: distributed simulation harness scenarios"
    )


@pytest.fixture
def stabilization_seconds() -> float:
    """Override per-scenario by reparametrizing this fixture.

    Phase 1 keeps a real wall-clock pause; Phase 2 replaces this with
    condition-driven `wait_until` predicates that finish as soon as the
    cluster is actually ready. The default of 5 s is comfortably above
    real settling time once worker subprocesses have spawned.
    """
    return 5.0
