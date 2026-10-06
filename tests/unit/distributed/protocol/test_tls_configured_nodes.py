"""
Nodes started with a certificate run over real sockets.

With a certificate configured, the server wrapped its datagram socket with
``SSLContext.wrap_socket`` -- which Python refuses for anything but a stream
socket (there is no DTLS in ``ssl``) -- so starting raised
``NotImplementedError`` and no certificate-configured node ever started. Its
TLS contexts were also built with the ``OP_NO_TLSv1*`` flags Python
deprecates in favour of ``minimum_version``.

* a node started with a certificate starts, answers datagrams (authenticated
  at the message layer) and answers TCP over TLS;
* its TLS contexts hold TLS 1.2 as their floor through ``minimum_version``
  and build without a deprecation warning.
"""

import datetime
import ipaddress
import socket
import ssl
import warnings
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp, udp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)

AUTH_SECRET = "tls-configured-nodes-secret-00000"
REQUEST_TIMEOUT_SECONDS = 5.0


class EchoNode(MercurySyncBaseServer):
    """A real base server with one UDP and one TCP echo handler."""

    @udp.receive()
    async def echo_udp(self, addr, data, clock_time) -> bytes:
        return b"udp-echo:" + data

    @tcp.receive()
    async def echo_tcp(self, addr, data, clock_time) -> bytes:
        return b"tcp-echo:" + data


def write_self_signed_certificate(directory: Path) -> tuple[str, str]:
    """A certificate for 127.0.0.1 and localhost, its own CA."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "hyperscale-test")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    certificate_path = directory / "node.pem"
    key_path = directory / "node.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(certificate_path), str(key_path)


def free_port(socket_type: socket.SocketKind) -> int:
    with socket.socket(socket.AF_INET, socket_type) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def make_node() -> EchoNode:
    return EchoNode(
        "127.0.0.1",
        free_port(socket.SOCK_STREAM),
        free_port(socket.SOCK_DGRAM),
        Env(MERCURY_SYNC_AUTH_SECRET=AUTH_SECRET),
    )


@pytest.mark.asyncio
async def test_a_node_started_with_a_certificate_answers_datagrams_and_tls(
    tmp_path: Path,
) -> None:
    certificate_path, key_path = write_self_signed_certificate(tmp_path)
    requester = make_node()
    responder = make_node()
    await requester.start_server(cert_path=certificate_path, key_path=key_path)
    await responder.start_server(cert_path=certificate_path, key_path=key_path)

    try:
        udp_response, _ = await requester.send_udp(
            ("127.0.0.1", responder._udp_port),
            "echo_udp",
            b"ping",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        tcp_response, _ = await requester.send_tcp(
            ("127.0.0.1", responder._tcp_port),
            "echo_tcp",
            b"ping",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        assert udp_response == b"udp-echo:ping"
        assert tcp_response == b"tcp-echo:ping"
    finally:
        await requester.shutdown()
        await responder.shutdown()


@pytest.mark.asyncio
async def test_tls_contexts_hold_tls_1_2_as_their_floor(tmp_path: Path) -> None:
    certificate_path, key_path = write_self_signed_certificate(tmp_path)
    node = make_node()
    node._server_cert_path = node._client_cert_path = certificate_path
    node._server_key_path = node._client_key_path = key_path

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        server_context = node._create_tcp_server_ssl_context()
        client_context = node._create_tcp_client_ssl_context()

    assert server_context.minimum_version == ssl.TLSVersion.TLSv1_2
    assert client_context.minimum_version == ssl.TLSVersion.TLSv1_2
