"""
The HTTP/2 client's receive path: the transport decrypts into the buffer
HTTP2Protocol owns, and the pipe parses frames in place in it. Driven
through the real client against a local TLS HTTP/2 server (h2).

* Bodies of every size arrive whole: none, within one frame, one frame
  exactly, past one frame, past the flow-control windows, and past the
  receive buffer itself, which fills, pauses reading and resumes.
* Concurrent requests over a pool of connections each get their own body.
* Frames that arrive between requests (PING, SETTINGS) wait in the buffer
  for the next request, which reads past them.
* A server that closes partway through a response fails that request, and
  the next request reconnects and succeeds.
* A redirect is followed with the request's own headers -- not the
  redirect response's -- and a chain stops at the redirect limit.
* The server's certificate is verified, as every engine client's is: the
  client trusts the test certificate authority that signed it.
* The buffer itself: an empty buffer hands out its whole view; the unparsed
  bytes -- a frame not yet whole -- move to the front to make room; a full
  buffer pauses reading, and waiting for more resumes it.
"""

import asyncio
import datetime
import ipaddress
import ssl
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import ConnectionTerminated, RequestReceived, StreamReset, WindowUpdated

import hyperscale.testing  # noqa: F401  (the engines' import order)
from hyperscale.core.engines.client.http2 import MercurySyncHTTP2Connection
from hyperscale.core.engines.client.http2.protocols.tcp.http2_protocol import (
    RECEIVE_BUFFER_SIZE,
    RECORD_SIZE,
    HTTP2Protocol,
)
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.engines.client.shared.timeouts import Timeouts

PATTERN = bytes(range(256))
# Far past anything these requests take: a wait that reaches it is a hang.
HANG_SECONDS = 10.0
BODY_SIZES = (0, 1, 300, 16384, 16385, 70000, 300000)


def body_of(size: int) -> bytes:
    return (PATTERN * (size // len(PATTERN) + 1))[:size]


def write_certificates(directory: Path) -> tuple[str, str]:
    """
    A certificate authority (ca.pem, which the client trusts) and the
    server certificate it signs for 127.0.0.1 and localhost: verified as any
    server is (RFC 9110 4.3.4), with the extensions strict X.509 checking
    requires.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    authority_key = ec.generate_private_key(ec.SECP256R1())
    authority_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "hyperscale-test-ca")])
    authority = (
        x509.CertificateBuilder()
        .subject_name(authority_name)
        .issuer_name(authority_name)
        .public_key(authority_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(authority_key.public_key()), critical=False)
        .sign(authority_key, hashes.SHA256())
    )

    key = ec.generate_private_key(ec.SECP256R1())
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "hyperscale-test")]))
        .issuer_name(authority_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(authority_key.public_key()),
            critical=False,
        )
        .sign(authority_key, hashes.SHA256())
    )

    (directory / "ca.pem").write_bytes(authority.public_bytes(serialization.Encoding.PEM))
    certificate_path = directory / "server.pem"
    key_path = directory / "server.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(certificate_path), str(key_path)


class H2Target:
    """
    A local TLS HTTP/2 server. GET /bytes/<n> answers n bytes of PATTERN in
    DATA frames within the client's flow-control windows; /ping-after/<n>
    answers the same, then sends a PING and a SETTINGS frame, which reach
    the client between requests; /close-midway/<n> sends half the body and
    closes the connection; /redirect/<n> answers 302 to /redirect/<n-1>
    (to /echo/0 at 0) with an x-probe header of its own; /echo/0 answers
    the x-probe header the request carried.
    """

    def __init__(self, directory: Path) -> None:
        certificate_path, key_path = write_certificates(directory)
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._context.load_cert_chain(certificate_path, key_path)
        self._context.set_alpn_protocols(["h2"])
        self._server: asyncio.Server | None = None

    async def __aenter__(self) -> str:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0, ssl=self._context)
        return f"https://127.0.0.1:{self._server.sockets[0].getsockname()[1]}"

    async def __aexit__(self, *_: object) -> None:
        self._server.close()
        self._server.close_clients()
        async with asyncio.timeout(HANG_SECONDS):
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = H2Connection(H2Configuration(client_side=False, header_encoding="utf-8"))
        connection.initiate_connection()
        writer.write(connection.data_to_send())
        # Each stream's body still to send, and what follows it.
        pending: dict[int, tuple[bytes, str]] = {}

        try:
            while data := await reader.read(65536):
                for event in connection.receive_data(data):
                    if isinstance(event, RequestReceived):
                        request_headers = dict(event.headers)
                        _, action, size = request_headers[":path"].split("/")
                        if action == "redirect":
                            count = int(size)
                            connection.send_headers(
                                event.stream_id,
                                [
                                    (":status", "302"),
                                    ("location", f"/redirect/{count - 1}" if count else "/echo/0"),
                                    ("x-probe", "from-the-redirect-response"),
                                    ("content-length", "0"),
                                ],
                                end_stream=True,
                            )
                            continue

                        body = (
                            request_headers.get("x-probe", "").encode()
                            if action == "echo"
                            else body_of(int(size))
                        )
                        connection.send_headers(
                            event.stream_id,
                            [(":status", "200"), ("content-length", str(len(body)))],
                            end_stream=not body,
                        )
                        if body:
                            pending[event.stream_id] = (body, action)

                    elif isinstance(event, StreamReset):
                        pending.pop(event.stream_id, None)

                    elif isinstance(event, ConnectionTerminated):
                        return

                    elif isinstance(event, WindowUpdated):
                        pass

                for stream_id, (body, action) in list(pending.items()):
                    if action == "close-midway":
                        connection.send_data(stream_id, body[: min(len(body) // 2, 16384)])
                        writer.write(connection.data_to_send())
                        await writer.drain()
                        return

                    while body and (
                        window := min(
                            connection.local_flow_control_window(stream_id),
                            connection.max_outbound_frame_size,
                            len(body),
                        )
                    ) > 0:
                        connection.send_data(stream_id, body[:window], end_stream=window == len(body))
                        body = body[window:]

                    if body:
                        pending[stream_id] = (body, action)

                    else:
                        del pending[stream_id]
                        if action == "ping-after":
                            connection.ping(b"between!")
                            connection.update_settings({})

                writer.write(connection.data_to_send())
                await writer.drain()

        except (ConnectionError, ssl.SSLError):
            # The client closed its connection: the end of its requests.
            pass

        finally:
            writer.close()


def make_client(pool_size: int, directory: Path) -> MercurySyncHTTP2Connection:
    """The client, verifying the server against the test certificate authority."""
    client = setup_client(
        MercurySyncHTTP2Connection(pool_size=pool_size, timeouts=Timeouts(request_timeout=HANG_SECONDS)),
        pool_size,
    )
    client._client_ssl_context.load_verify_locations(directory / "ca.pem")
    return client


async def test_bodies_of_every_size_arrive_whole(tmp_path: Path) -> None:
    async with H2Target(tmp_path) as target:
        client = make_client(1, tmp_path)
        for size in BODY_SIZES * 2:  # the second pass on the reused connection
            response = await client.get(f"{target}/bytes/{size}")

            assert (response.status, len(response.content)) == (200, size), response.status_message
            assert response.content == body_of(size)


async def test_concurrent_requests_each_get_their_own_body(tmp_path: Path) -> None:
    async with H2Target(tmp_path) as target:
        client = make_client(8, tmp_path)
        sizes = [BODY_SIZES[index % len(BODY_SIZES)] for index in range(64)]
        async with asyncio.timeout(HANG_SECONDS * 4):
            responses = await asyncio.gather(*[client.get(f"{target}/bytes/{size}") for size in sizes])

        for size, response in zip(sizes, responses):
            assert response.status == 200, response.status_message
            assert response.content == body_of(size)


async def test_frames_between_requests_wait_for_the_next_one(tmp_path: Path) -> None:
    async with H2Target(tmp_path) as target:
        client = make_client(1, tmp_path)
        first = await client.get(f"{target}/ping-after/300")
        # The PING and SETTINGS frames arrive after the first response ended.
        await asyncio.sleep(0.05)
        second = await client.get(f"{target}/bytes/70000")

        assert (first.status, first.content) == (200, body_of(300))
        assert (second.status, second.content) == (200, body_of(70000))


async def test_a_server_closing_midway_fails_that_request_and_the_next_reconnects(tmp_path: Path) -> None:
    async with H2Target(tmp_path) as target:
        client = make_client(1, tmp_path)
        failed = await client.get(f"{target}/close-midway/100000")
        recovered = await client.get(f"{target}/bytes/300")

        assert failed.status == 400
        assert (recovered.status, recovered.content) == (200, body_of(300))


async def test_a_redirect_is_followed_with_the_requests_own_headers(tmp_path: Path) -> None:
    async with H2Target(tmp_path) as target:
        client = make_client(1, tmp_path)
        followed = await client.get(f"{target}/redirect/1", headers={"x-probe": "from-the-request"})
        stopped = await client.get(f"{target}/redirect/5", headers={"x-probe": "from-the-request"}, redirects=2)

        assert (followed.status, followed.content) == (200, b"from-the-request")
        # Two redirects followed, the third returned as it came.
        assert stopped.status == 302
        assert stopped.headers["location"] == "/redirect/2"


class PausableTransport:
    def __init__(self) -> None:
        self.reading = True

    def get_extra_info(self, name: str, default: object = None) -> object:
        return default

    def pause_reading(self) -> None:
        self.reading = False

    def resume_reading(self) -> None:
        self.reading = True


async def test_the_buffer_hands_out_its_view_moves_unparsed_bytes_and_pauses_when_full() -> None:
    protocol = HTTP2Protocol(loop=asyncio.get_running_loop())
    transport = PausableTransport()
    protocol.connection_made(transport)

    assert protocol.get_buffer(-1) is protocol._view  # empty: the whole view, no slice

    # A frame not yet whole, received after bytes the pipe has parsed.
    filled = RECEIVE_BUFFER_SIZE - RECORD_SIZE + 1
    protocol.get_buffer(-1)[:filled] = b"p" * (filled - 5) + b"tail!"
    protocol.buffer_updated(filled)
    protocol._start = filled - 5

    free = protocol.get_buffer(-1)

    assert (protocol._start, protocol._end) == (0, 5)
    assert bytes(protocol._view[:5]) == b"tail!"
    assert len(free) == RECEIVE_BUFFER_SIZE - 5

    free[: len(free)] = b"x" * len(free)
    protocol.buffer_updated(len(free))

    assert transport.reading is False  # full: reading waits for the pipe

    protocol._start = protocol._end - 1  # the pipe parsed all but one byte
    waiter = protocol.data_waiter()

    assert transport.reading is True  # waiting for more resumes it
    waiter.cancel()
