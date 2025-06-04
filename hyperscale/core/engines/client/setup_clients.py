import asyncio
import re
import ssl
from random import randrange
from typing import Optional, TypeVar

from hyperscale.core.engines.client.tracing import HTTPTrace
from .graphql import MercurySyncGraphQLConnection
from .graphql_http2 import MercurySyncGraphQLHTTP2Connection
from .grpc import MercurySyncGRPCConnection
from .http import MercurySyncHTTPConnection
from .http.protocols import HTTPConnection
from .http2 import MercurySyncHTTP2Connection
from .http2.fast_hpack import Encoder
from .http2.pipe import HTTP2Pipe
from .http2.protocols import HTTP2Connection
from .http2.settings import Settings
from .http3 import MercurySyncHTTP3Connection
from .http3.protocols import HTTP3Connection
from .playwright import MercurySyncPlaywrightConnection
from .smtp import MercurySyncSMTPConnection
from .smtp.protocols import SMTPConnection
from .tcp import MercurySyncTCPConnection
from .tcp.protocols import TCPConnection
from .udp import MercurySyncUDPConnection
from .udp.protocols import UDPConnection
from .websocket import MercurySyncWebsocketConnection
from .websocket.protocols import WebsocketConnection


T = TypeVar('T')


def setup_client(
    client: T,
    vus: int,
    pages: Optional[int] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    reset_connections: bool = False,
) -> T:
    client._concurrency = vus

    if isinstance(
        client,
        (
            MercurySyncGraphQLConnection,
            MercurySyncHTTPConnection,
        ),
    ):
        client.reset_connections = reset_connections
        client._connections = [
            HTTPConnection(
                reset_connections=reset_connections,
            )
            for _ in range(vus)
        ]

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE


        client.trace = HTTPTrace(
            'HTTP/1.1',
            vus,
            ssl_version='TLSv1.2'
        )

        client._client_ssl_context = ctx
        client._semaphore = asyncio.Semaphore(vus)

    elif isinstance(
        client,
        (
            MercurySyncHTTP2Connection,
            MercurySyncGraphQLHTTP2Connection,
        ),
    ):
        client._reset_connections = reset_connections

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        ctx.options |= (
            ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        )

        ctx.options |= ssl.OP_NO_COMPRESSION

        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")
        ctx.set_alpn_protocols(["h2", "http/1.1"])

        try:
            if hasattr(ctx, "_set_npn_protocols"):
                ctx.set_npn_protocols(["h2", "http/1.1"])
        except NotImplementedError:
            pass

        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        client._client_ssl_context = ctx

        client._encoder = Encoder()
        client._settings = Settings(client=False)
        client._connections = [
            HTTP2Connection(
                stream_id=randrange(1, 2**20 + 2, 2),
                reset_connections=reset_connections,
            )
            for _ in range(vus)
        ]

        client._pipes = [HTTP2Pipe(vus) for _ in range(vus)]

        client._semaphore = asyncio.Semaphore(vus)

    elif isinstance(client, MercurySyncGRPCConnection):
        client._reset_connections = reset_connections

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.options |= (
            ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        )

        ctx.options |= ssl.OP_NO_COMPRESSION

        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")
        ctx.set_alpn_protocols(["h2", "http/1.1"])

        try:
            if hasattr(ctx, "_set_npn_protocols"):
                ctx.set_npn_protocols(["h2", "http/1.1"])
        except NotImplementedError:
            pass

        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        client._client_ssl_context = ctx

        client._encoder = Encoder()
        client._settings = Settings(client=False)

        client._connections = [
            HTTP2Connection(
                stream_id=randrange(1, 2**20 + 2, 2),
                reset_connections=reset_connections,
            )
            for _ in range(vus)
        ]

        client._pipes = [HTTP2Pipe(vus) for _ in range(vus)]

        client._semaphore = asyncio.Semaphore(vus)

    elif isinstance(client, MercurySyncHTTP3Connection):
        client.reset_connections = reset_connections

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        client._udp_ssl_context = ctx

        client._connections = [
            HTTP3Connection(reset_connections=reset_connections) for _ in range(vus)
        ]

        client._semaphore = asyncio.Semaphore(vus)

    elif isinstance(client, MercurySyncPlaywrightConnection):
        client._semaphore = asyncio.Semaphore(vus)
        client._max_pages = pages

    elif isinstance(client, MercurySyncSMTPConnection):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        client._ssl_context = ctx
        client._loop = asyncio.get_event_loop()
        client._OLDSTYLE_AUTH = re.compile(r"auth=(.*)", re.I)
        
        client._semaphore = asyncio.Semaphore(vus)
        client._connections = [
            SMTPConnection(
                reset_connections=reset_connections,
            ) for _ in range(vus)
        ]

    elif isinstance(client, MercurySyncTCPConnection):
        client.reset_connections = reset_connections
        client._connections = [
            TCPConnection(
                reset_connections=reset_connections,
            )
            for _ in range(vus)
        ]

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        client._tcp_ssl_context = ctx
        client._semaphore = asyncio.Semaphore(vus)

    elif isinstance(client, MercurySyncUDPConnection):
        if cert_path is None:
            cert_path = client._cert_path

        if key_path is None:
            key_path = client._key_path

        if cert_path and key_path:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
            ctx.options |= ssl.OP_NO_TLSv1
            ctx.options |= ssl.OP_NO_TLSv1_1
            ctx.options |= ssl.OP_SINGLE_DH_USE
            ctx.options |= ssl.OP_SINGLE_ECDH_USE
            ctx.load_cert_chain(cert_path, keyfile=key_path)
            ctx.load_verify_locations(cafile=cert_path)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
            ctx.set_ciphers("ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384")

            client._udp_ssl_context = ctx

        client._connections = [
            UDPConnection(
                reset_connections=reset_connections,
            )
            for _ in range(vus)
        ]

        client._semaphore = asyncio.Semaphore(vus)

    elif isinstance(client, MercurySyncWebsocketConnection):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        client._client_ssl_context = ctx

        client._connections = [
            WebsocketConnection(
                reset_connections=reset_connections,
            )
            for _ in range(vus)
        ]

        client._semaphore = asyncio.Semaphore(vus)

    return client
