import asyncio
import ssl
import time
import uuid
from collections import defaultdict
from random import randrange
from typing import (
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import (
    ParseResult,
    urlencode,
    urlparse,
)

import orjson
from pydantic import BaseModel

from hyperscale.core_rewrite.engines.client.shared.models import (
    URL,
    Cookies,
    HTTPCookie,
    HTTPEncodableValue,
    URLMetadata,
)
from hyperscale.core_rewrite.engines.client.shared.protocols import NEW_LINE
from hyperscale.core_rewrite.engines.client.shared.timeouts import Timeouts

from .fast_hpack import Encoder
from .models.http2 import (
    HTTP2Response,
)
from .pipe import HTTP2Pipe
from .protocols import HTTP2Connection
from .settings import Settings

A = TypeVar("A")
R = TypeVar("R")


class MercurySyncHTTP2Connection:
    def __init__(
        self,
        pool_size: int = 128,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        self.session_id = str(uuid.uuid4())
        self.timeouts = timeouts

        self.closed = False
        self._concurrency = pool_size
        self._reset_connections = reset_connections

        self._semaphore = asyncio.Semaphore(pool_size)

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[HTTP2Connection] = [
            HTTP2Connection(
                pool_size,
                stream_id=randrange(1, 2**20 + 2, 2),
                reset_connections=reset_connections,
            )
            for _ in range(pool_size)
        ]

        self._pipes = [HTTP2Pipe(pool_size) for _ in range(pool_size)]

        self._url_cache: Dict[str, URL] = {}

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._locks: Dict[asyncio.Transport, asyncio.Lock] = {}

        self._max_concurrency = pool_size
        self._connection_waiters: List[asyncio.Future] = []

        self.active = 0
        self.waiter = None

        self._encoder = Encoder()
        self._settings = Settings(client=False)

        self._client_ssl_context = self._create_http2_ssl_context()

    async def head(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "HEAD",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="HEAD",
                    status=408,
                    status_message="Request timed out.",
                )

    async def options(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "OPTIONS",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="OPTIONS",
                    status=408,
                    status_message="Request timed out.",
                )

    async def get(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "GET",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="GET",
                    status=408,
                    status_message="Request timed out.",
                )

    async def post(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        data: Union[Optional[str], Optional[BaseModel]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "POST",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        data=data,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="POST",
                    status=408,
                    status_message="Request timed out.",
                )

    async def put(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        data: Union[Optional[str], Optional[BaseModel]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "PUT",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        data=data,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="PUT",
                    status=408,
                    status_message="Request timed out.",
                )

    async def patch(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        data: Union[Optional[str], Optional[BaseModel]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "PATCH",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        data=data,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="PATCH",
                    status=408,
                    status_message="Request timed out.",
                )

    async def delete(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "DELETE",
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return HTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="DELETE",
                    status=408,
                    status_message="Request timed out.",
                )

    async def _request(
        self,
        url: str,
        method: str,
        cookies: Optional[List[HTTPCookie]] = None,
        auth: Optional[Tuple[str, str]] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        headers: Optional[Dict[str, str]] = {},
        data: Union[Optional[str], Optional[bytes], Optional[BaseModel]] = None,
        redirects: Optional[int] = 3,
    ):
        timings: Dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "write_start": None,
            "write_end": None,
            "read_start": None,
            "read_end": None,
            "request_end": None,
        }
        timings["request_start"] = time.monotonic()

        result, redirect, timings = await self._execute(
            url,
            method,
            cookies=cookies,
            auth=auth,
            params=params,
            headers=headers,
            data=data,
            timings=timings,
        )

        if redirect:
            location = result.headers.get("location")

            upgrade_ssl = False
            if "https" in location and "https" not in url:
                upgrade_ssl = True

            for _ in range(redirects):
                result, redirect, timings = await self._execute(
                    url,
                    method,
                    cookies=cookies,
                    auth=auth,
                    params=params,
                    headers=headers,
                    data=data,
                    timings=timings,
                    upgrade_ssl=upgrade_ssl,
                    redirect_url=location,
                )

                if redirect is False:
                    break

                location = result.headers.get("location")

                upgrade_ssl = False
                if "https" in location and "https" not in url:
                    upgrade_ssl = True

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str,
        method: str,
        cookies: Optional[List[HTTPCookie]] = None,
        auth: Optional[Tuple[str, str]] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        headers: Optional[Dict[str, str]] = {},
        data: Union[
            Optional[str],
            Optional[bytes],
            Optional[BaseModel],
        ] = None,
        upgrade_ssl: bool = False,
        redirect_url: Optional[str] = None,
        timings: Dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = {},
    ):
        if redirect_url:
            request_url = redirect_url

        connection: HTTP2Connection = None

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (error, connection, pipe, url, upgrade_ssl) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url, ssl_redirect_url=request_url if upgrade_ssl else None
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                (error, connection, pipe, url, _) = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url, ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            if error:
                timings["connect_end"] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        self._concurrency,
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )

                self._pipes.append(HTTP2Pipe(self._max_concurrency))

                return (
                    HTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message=str(error),
                        headers={
                            key.encode(): value.encode()
                            for key, value in headers.items()
                        }
                        if headers
                        else {},
                    ),
                    False,
                    timings,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            connection = pipe.send_preamble(connection)

            encoded_headers = self._encode_headers(
                url,
                method,
                params=params,
                headers=headers,
            )

            connection = pipe.send_request_headers(
                encoded_headers,
                data,
                connection,
            )

            if data:
                encoded_data = self._encode_data(data)

                connection = await asyncio.wait_for(
                    pipe.submit_request_body(
                        encoded_data,
                        connection,
                    ),
                    timeout=self.timeouts.write_timeout,
                )

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            (status, headers, body, error) = await asyncio.wait_for(
                pipe.receive_response(connection), timeout=self.timeouts.read_timeout
            )

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        self._concurrency,
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )
                self._pipes.append(HTTP2Pipe(self._max_concurrency))

                return (
                    HTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=status,
                        headers=headers,
                    ),
                    True,
                    timings,
                )

            if error:
                raise error

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get("set-cookie")
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

            self._connections.append(connection)
            self._pipes.append(pipe)

            timings["read_end"] = time.monotonic()

            return (
                HTTP2Response(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                    ),
                    cookies=cookies,
                    method=method,
                    status=status,
                    headers=headers,
                    content=body,
                ),
                False,
                timings,
            )

        except Exception as request_exception:
            self._connections.append(
                HTTP2Connection(
                    self._concurrency,
                    stream_id=randrange(1, 2**20 + 2, 2),
                    reset_connections=self._reset_connections,
                )
            )

            self._pipes.append(HTTP2Pipe(self._max_concurrency))

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            timings["read_end"] = time.monotonic()

            return (
                HTTP2Response(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                        params=request_url.params,
                        query=request_url.query,
                    ),
                    method=method,
                    status=400,
                    status_message=str(request_exception),
                ),
                False,
                timings,
            )

    def _encode_data(
        self,
        data: Union[
            Optional[str],
            Optional[bytes],
            Optional[BaseModel],
        ] = None,
    ):
        encoded_data: Optional[bytes] = None
        size = 0

        if isinstance(data, Iterator):
            chunks = []
            for chunk in data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                size += len(encoded_chunk)
                chunks.append(encoded_chunk)

            encoded_data = chunks

        else:
            if isinstance(data, dict):
                encoded_data = orjson.dumps(data)

            elif isinstance(data, BaseModel):
                return data.model_dump_json().encode()

            elif isinstance(data, tuple):
                encoded_data = urlencode(data).encode()

            elif isinstance(data, str):
                encoded_data = data.encode()

        return encoded_data

    def _encode_headers(
        self,
        url: URL,
        method: str,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        url_path = url.path
        if params:
            url_params = urlencode(params)
            url_path += f"?{url_params}"

        encoded_headers: List[Tuple[bytes, bytes]] = [
            (b":method", method.encode()),
            (b":authority", url.hostname.encode()),
            (b":scheme", url.scheme.encode()),
            (b":path", url_path.encode()),
            (b"user-agent", b"hyperscale"),
        ]

        if headers:
            encoded_headers.extend(
                [
                    (k.lower().encode(), v.encode())
                    for k, v in headers.items()
                    if k.lower()
                    not in (
                        "host",
                        "transfer-encoding",
                    )
                ]
            )

        encoded_headers: bytes = self._encoder.encode(encoded_headers)
        encoded_headers: List[bytes] = [
            encoded_headers[i : i + self._settings.max_frame_size]
            for i in range(0, len(encoded_headers), self._settings.max_frame_size)
        ]

        return encoded_headers[0]

    def _create_http2_ssl_context(self):
        """
        This function creates an SSLContext object that is suitably configured for
        HTTP/2. If you're working with Python TLS directly, you'll want to do the
        exact same setup as this function does.
        """
        # Get the basic context from the standard library.
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        # RFC 7540 Section 9.2: Implementations of HTTP/2 MUST use TLS version 1.2
        # or higher. Disable TLS 1.1 and lower.
        ctx.options |= (
            ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        )

        # RFC 7540 Section 9.2.1: A deployment of HTTP/2 over TLS 1.2 MUST disable
        # compression.
        ctx.options |= ssl.OP_NO_COMPRESSION

        # RFC 7540 Section 9.2.2: "deployments of HTTP/2 that use TLS 1.2 MUST
        # support TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256". In practice, the
        # blocklist defined in this section allows only the AES GCM and ChaCha20
        # cipher suites with ephemeral key negotiation.
        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")

        # We want to negotiate using NPN and ALPN. ALPN is mandatory, but NPN may
        # be absent, so allow that. This setup allows for negotiation of HTTP/1.1.
        ctx.set_alpn_protocols(["h2", "http/1.1"])

        try:
            if hasattr(ctx, "_set_npn_protocols"):
                ctx.set_npn_protocols(["h2", "http/1.1"])
        except NotImplementedError:
            pass

        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        return ctx

    async def _connect_to_url_location(
        self, request_url: str, ssl_redirect_url: Optional[str] = None
    ) -> Tuple[Optional[Exception], HTTP2Connection, HTTP2Pipe, URL, bool]:
        if ssl_redirect_url:
            parsed_url = URL(ssl_redirect_url)

        else:
            parsed_url = URL(request_url)

        url = self._url_cache.get(parsed_url.hostname)
        dns_lock = self._dns_lock[parsed_url.hostname]
        dns_waiter = self._dns_waiters[parsed_url.hostname]

        do_dns_lookup = url is None or ssl_redirect_url

        if do_dns_lookup and dns_lock.locked() is False:
            await dns_lock.acquire()
            url = parsed_url
            await url.lookup()

            self._dns_lock[parsed_url.hostname] = dns_lock
            self._url_cache[parsed_url.hostname] = url

            dns_waiter = self._dns_waiters[parsed_url.hostname]

            if dns_waiter.done() is False:
                dns_waiter.set_result(None)

            dns_lock.release()

        elif do_dns_lookup:
            await dns_waiter
            url = self._url_cache.get(parsed_url.hostname)

        connection = self._connections.pop()
        pipe = self._pipes.pop()

        connection_error: Optional[Exception] = None

        if url.address is None or ssl_redirect_url:
            for address, ip_info in url:
                try:
                    await connection.make_connection(
                        url.hostname,
                        address,
                        url.port,
                        ip_info,
                        ssl=self._client_ssl_context
                        if url.is_ssl or ssl_redirect_url
                        else None,
                        ssl_upgrade=ssl_redirect_url is not None,
                    )

                    url.address = address
                    url.socket_config = ip_info

                except Exception as connection_error:
                    if "server_hostname is only meaningful with ssl" in str(
                        connection_error
                    ):
                        return (None, None, None, parsed_url, True)

        else:
            try:
                await connection.make_connection(
                    url.hostname,
                    url.address,
                    url.port,
                    url.socket_config,
                    ssl=self._client_ssl_context
                    if url.is_ssl or ssl_redirect_url
                    else None,
                    ssl_upgrade=ssl_redirect_url is not None,
                )

            except Exception as connection_error:
                if "server_hostname is only meaningful with ssl" in str(
                    connection_error
                ):
                    return (None, None, None, parsed_url, True)

                raise connection_error

        return (
            connection_error,
            connection,
            pipe,
            parsed_url,
            False,
        )
