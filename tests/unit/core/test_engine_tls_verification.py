"""
Engine clients verify the server they connect to, unless a Workflow opts out.

Every engine client's TLS context used to set ``check_hostname = False``
and ``CERT_NONE``: an HTTPS load test talked to whatever answered, with no
check of the certificate or the name (RFC 9110 section 4.3.4 requires
both). ``setup_client`` now verifies by default; ``verify_tls=False`` is
the explicit opt-out for a self-signed target, and a Workflow sets it with
``verify_tls = False``. ``cert_path``, ``key_path`` and
``reset_connections`` declared on a Workflow now reach ``setup_client``
too: the per-workflow config only accepted keys it already held truthy.

Driven for real against a local HTTPS target whose certificate is freshly
self-signed:
* by default the HTTP engine refuses the handshake;
* with ``verify_tls=False`` the same request succeeds;
* through ``LocalRunner`` with spawned workers, a Workflow declaring
  ``verify_tls = False`` succeeds against the target, and the same
  Workflow without it fails every request.
"""

import asyncio
import datetime
import ipaddress
import pathlib
import socket
import ssl
import sys

import cloudpickle
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from hyperscale.core.engines.client.http import MercurySyncHTTPConnection
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.testing import URL, HTTPResponse

HOST = "127.0.0.1"
AUTH_SECRET_ENVAR = "MERCURY_SYNC_AUTH_SECRET"
WORKER_COUNT = 2
RUN_TIMEOUT_SECONDS = 120
CERTIFICATE_VALIDITY = datetime.timedelta(days=1)
HTTP_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok"
TESTS_ROOT = pathlib.Path(__file__).resolve().parents[2]


def free_port(socket_kind: int) -> int:
    with socket.socket(socket.AF_INET, socket_kind) as port_probe:
        port_probe.bind((HOST, 0))
        return port_probe.getsockname()[1]


def write_self_signed_certificate(directory: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    """A certificate for 127.0.0.1 signed by its own key, as a staging
    server's often is."""
    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, HOST)])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - CERTIFICATE_VALIDITY)
        .not_valid_after(now + CERTIFICATE_VALIDITY)
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(HOST))]), critical=False)
        .sign(private_key, hashes.SHA256())
    )
    certificate_path = directory / "target.crt"
    key_path = directory / "target.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return certificate_path, key_path


class CountingHttpsTarget:
    """A local HTTPS target counting the requests it answers."""

    def __init__(self) -> None:
        self.requests_answered = 0

    async def answer_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while await reader.readuntil(b"\r\n\r\n"):
                self.requests_answered += 1
                writer.write(HTTP_OK)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, ssl.SSLError):
            return
        finally:
            writer.close()


async def start_https_target(directory: pathlib.Path) -> tuple[asyncio.Server, CountingHttpsTarget, int]:
    certificate_path, key_path = write_self_signed_certificate(directory)
    server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_context.load_cert_chain(certificate_path, key_path)
    target = CountingHttpsTarget()
    port = free_port(socket.SOCK_STREAM)
    server = await asyncio.start_server(target.answer_http, HOST, port, ssl=server_context)
    return server, target, port


@pytest.fixture
def spawnable_sys_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spawned workers start from this process's ``sys.path``; test
    directories on it would shadow the standard library's ``logging``."""
    monkeypatch.setattr(
        sys,
        "path",
        [entry for entry in sys.path if not pathlib.Path(entry or ".").resolve().is_relative_to(TESTS_ROOT)],
    )


async def request_once(url: str, **setup_options: bool) -> HTTPResponse:
    http = setup_client(MercurySyncHTTPConnection(), 1, **setup_options)
    try:
        return await http.get(url)
    finally:
        http.close()


async def test_the_http_engine_refuses_a_self_signed_server_by_default(tmp_path: pathlib.Path) -> None:
    server, target, port = await start_https_target(tmp_path)
    try:
        response = await request_once(f"https://{HOST}:{port}/")
    finally:
        server.close()
        await server.wait_closed()

    # Refused at the handshake: the target never sees a request. The
    # same target answers when verification is off (next test), so the
    # certificate check is what refuses it.
    assert response.status != 200, response.status_message
    assert target.requests_answered == 0


async def test_the_http_engine_reaches_a_self_signed_server_when_told_not_to_verify(tmp_path: pathlib.Path) -> None:
    server, target, port = await start_https_target(tmp_path)
    try:
        response = await request_once(f"https://{HOST}:{port}/", verify_tls=False)
    finally:
        server.close()
        await server.wait_closed()

    assert response.status == 200, response.status_message
    assert target.requests_answered == 1


def make_workflow(target: str, verify_tls: bool | None) -> Workflow:
    async def hit(self, url: URL = target) -> HTTPResponse:
        return await self.client.http.get(url)

    attributes: dict[str, object] = {"vus": WORKER_COUNT, "duration": "1s", "timeout": "30s", "hit": step()(hit)}
    if verify_tls is not None:
        attributes["verify_tls"] = verify_tls
    return type("TlsTargetWorkflow", (Workflow,), attributes)()


async def run_workflow_against(workflow: Workflow) -> dict:
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    runner = LocalRunner(HOST, free_port(socket.SOCK_DGRAM), workers=WORKER_COUNT)
    try:
        results = await asyncio.wait_for(
            runner.run("tls-verification", [([], workflow)], terminal_mode="disabled"),
            timeout=RUN_TIMEOUT_SECONDS,
        )
    finally:
        cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
    assert isinstance(results, dict), f"the run failed: {results!r}"
    return results["results"][workflow.name]["stats"]


@pytest.mark.parametrize("verify_tls", [False, None])
async def test_a_workflow_decides_whether_its_clients_verify_the_target(
    verify_tls: bool | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    spawnable_sys_path: None,
) -> None:
    monkeypatch.delenv(AUTH_SECRET_ENVAR, raising=False)
    monkeypatch.chdir(tmp_path)
    LoggingConfig().update(log_directory=str(tmp_path), log_level="error")
    server, target, port = await start_https_target(tmp_path)
    try:
        workflow_stats = await run_workflow_against(make_workflow(f"https://{HOST}:{port}/", verify_tls))
    finally:
        server.close()
        await server.wait_closed()

    if verify_tls is False:
        assert workflow_stats["failed"] == 0, workflow_stats
        assert workflow_stats["succeeded"] > 0, workflow_stats
        assert target.requests_answered >= workflow_stats["succeeded"]
        return

    # Declared nothing: verified, and the self-signed target is refused.
    assert workflow_stats["succeeded"] == 0, workflow_stats
    assert workflow_stats["failed"] > 0, workflow_stats
    assert target.requests_answered == 0
