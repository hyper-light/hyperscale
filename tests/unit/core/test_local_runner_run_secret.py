"""
A local run with no configured cluster secret: ``LocalRunner`` generates a
per-run secret (``secrets.token_urlsafe(32)``) and hands that same ``Env``
to the worker processes it spawns, so the runner and its own workers --
and nobody else -- authenticate each other's frames. A configured secret
(``Env`` or ``MERCURY_SYNC_AUTH_SECRET``) still wins.

Driven end to end: the production ``LocalRunner.run`` spawns its worker
pool, connects to it under the generated secret, and runs a tiny workflow
against a local HTTP target.
"""

import asyncio
import pathlib
import socket
import sys

import cloudpickle
import pytest

from hyperscale.core.jobs.models import Env
from hyperscale.core.jobs.protocols.encryption import AESGCMFernet, EncryptionError
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.reporting.json import JSONConfig
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.testing import URL, HTTPResponse

HOST = "127.0.0.1"
AUTH_SECRET_ENVAR = "MERCURY_SYNC_AUTH_SECRET"
CONFIGURED_SECRET = "local-runner-configured-secret-0123456789"
ENVIRONMENT_SECRET = "local-runner-environment-secret-0123456789"
# secrets.token_urlsafe(32): 32 bytes, base64url without padding.
RUN_SECRET_LENGTH = 43
WORKER_COUNT = 2
RUN_TIMEOUT_SECONDS = 120
HTTP_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"
TESTS_ROOT = pathlib.Path(__file__).resolve().parents[2]


def free_port(socket_kind: int) -> int:
    with socket.socket(socket.AF_INET, socket_kind) as port_probe:
        port_probe.bind((HOST, 0))
        return port_probe.getsockname()[1]


class CountingTarget:
    """A local HTTP target counting the requests it answers."""

    def __init__(self) -> None:
        self.requests_answered = 0

    async def answer_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while await reader.readuntil(b"\r\n\r\n"):
                self.requests_answered += 1
                writer.write(HTTP_OK)
                await writer.drain()

        except (asyncio.IncompleteReadError, ConnectionError):
            return

        finally:
            writer.close()


@pytest.fixture
def spawnable_sys_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spawned worker processes start from this process's ``sys.path``.
    A whole-suite run puts test directories on it (pytest's rootdir-relative
    imports), where ``tests/unit/logging`` would shadow the standard
    library's ``logging`` in the child; the workers need none of them."""
    monkeypatch.setattr(
        sys,
        "path",
        [entry for entry in sys.path if not pathlib.Path(entry or ".").resolve().is_relative_to(TESTS_ROOT)],
    )


def make_workflow(target: str, results_directory: pathlib.Path) -> Workflow:
    async def hit(self, url: URL = target) -> HTTPResponse:
        return await self.client.http.get(url)

    workflow_class = type(
        "RunSecretWorkflow",
        (Workflow,),
        {
            "vus": WORKER_COUNT,
            "duration": "1s",
            "timeout": "30s",
            "hit": step()(hit),
            # The default JSON reporter's paths are fixed at import, under
            # the working directory: the test's results go to its tmp_path.
            "reporting": JSONConfig(
                workflow_results_filepath=str(results_directory / "workflow_results.json"),
                step_results_filepath=str(results_directory / "step_results.json"),
            ),
        },
    )
    return workflow_class()


async def test_unconfigured_runner_generates_a_strong_per_run_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(AUTH_SECRET_ENVAR, raising=False)

    first_runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=1)
    second_runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=1)

    first_secret = first_runner._env.MERCURY_SYNC_AUTH_SECRET
    second_secret = second_runner._env.MERCURY_SYNC_AUTH_SECRET
    assert first_secret is not None and len(first_secret) == RUN_SECRET_LENGTH
    assert second_secret is not None and len(second_secret) == RUN_SECRET_LENGTH
    assert first_secret != second_secret, "every run must get a secret of its own"
    # The encryptor accepts it: it is neither short nor a known weak value.
    AESGCMFernet(first_runner._env)


async def test_runner_hands_its_secret_to_its_workers_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """The worker processes rebuild their Env from ``env.model_dump()``
    (``LocalServerPool.run_pool`` -> ``run_thread``): what they rebuild
    decrypts what the runner encrypts, and another run's secret does not."""
    monkeypatch.delenv(AUTH_SECRET_ENVAR, raising=False)
    runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=1)
    other_runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=1)

    worker_env = Env(**runner._env.model_dump())
    frame = AESGCMFernet(runner._env).encrypt(b"run-secret-frame")

    assert AESGCMFernet(worker_env).decrypt(frame) == b"run-secret-frame"
    with pytest.raises(EncryptionError):
        AESGCMFernet(Env(**other_runner._env.model_dump())).decrypt(frame)


async def test_configured_env_secret_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(AUTH_SECRET_ENVAR, ENVIRONMENT_SECRET)

    runner = LocalRunner(
        HOST,
        free_port(socket.SOCK_DGRAM),
        env=Env(MERCURY_SYNC_AUTH_SECRET=CONFIGURED_SECRET),
        workers=1,
    )

    assert runner._env.MERCURY_SYNC_AUTH_SECRET == CONFIGURED_SECRET


async def test_environment_secret_wins_over_a_generated_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(AUTH_SECRET_ENVAR, ENVIRONMENT_SECRET)

    unconfigured_runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=1)
    secretless_env_runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), env=Env(), workers=1)

    assert unconfigured_runner._env.MERCURY_SYNC_AUTH_SECRET == ENVIRONMENT_SECRET
    assert secretless_env_runner._env.MERCURY_SYNC_AUTH_SECRET == ENVIRONMENT_SECRET


async def test_unconfigured_runner_runs_a_workflow_with_its_own_workers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    spawnable_sys_path: None,
) -> None:
    monkeypatch.delenv(AUTH_SECRET_ENVAR, raising=False)
    monkeypatch.chdir(tmp_path)
    LoggingConfig().update(log_directory=str(tmp_path), log_level="error")

    target_port = free_port(socket.SOCK_STREAM)
    target = CountingTarget()
    target_server = await asyncio.start_server(target.answer_http, HOST, target_port)
    workflow = make_workflow(f"http://{HOST}:{target_port}/", tmp_path)
    # Spawned workers receive the workflow by value, as `hyperscale run
    # workflow` sends it.
    cloudpickle.register_pickle_by_value(sys.modules[__name__])

    runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=WORKER_COUNT)
    try:
        results = await asyncio.wait_for(
            runner.run(
                "run-secret",
                [([], workflow)],
                terminal_mode="disabled",
            ),
            timeout=RUN_TIMEOUT_SECONDS,
        )

    finally:
        cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
        target_server.close()
        await target_server.wait_closed()

    assert isinstance(results, dict), f"the run failed: {results!r}"
    workflow_stats = results["results"][workflow.name]["stats"]
    assert workflow_stats["failed"] == 0, workflow_stats
    assert workflow_stats["succeeded"] > 0, workflow_stats
    # A request in flight when the duration ends is answered but not counted.
    assert target.requests_answered >= workflow_stats["succeeded"], (
        "the workers, authenticated by the per-run secret, sent every request the run reports"
    )
