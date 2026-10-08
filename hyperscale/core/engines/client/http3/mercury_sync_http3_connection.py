import asyncio
import base64
import ssl
import time
from collections import defaultdict
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
    urljoin,
)

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.http3.protocols.quic_protocol import (
    FrameType,
    HeadersState,
    ResponseFrameCollection,
    encode_frame,
)
from hyperscale.core.engines.client.shared.models import (
    URL as HTTPUrl,
)
from hyperscale.core.engines.client.shared.models import (
    Cookies as HTTPCookies,
)
from hyperscale.core.engines.client.shared.models import (
    HTTPCookie,
    HTTPEncodableValue,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.models.url import DEFAULT_PORTS
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Params,
)

from .models.http3 import HTTP3Response
from .protocols import HTTP3Connection

A = TypeVar("A")
R = TypeVar("R")


class MercurySyncHTTP3Connection:
    def __init__(
        self,
        pool_size: Optional[int] = None,
        timeouts: Timeouts | None = None,
        reset_connections: bool = False,
    ) -> None:
        if pool_size is None:
            pool_size = 100

        self._concurrency = pool_size
        # Each engine gets its own Timeouts: a default argument would be one
        # instance shared by every engine built without timeouts.
        self.timeouts = timeouts if timeouts is not None else Timeouts()
        self.reset_connections = reset_connections

        self._client_ssl_context: Optional[ssl.SSLContext] = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[HTTP3Connection] = []

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: List[asyncio.Future] = []

        self._url_cache: Dict[str, HTTPUrl] = {}
        self._optimized: Dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}

    async def head(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "HEAD",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="HEAD",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def options(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "OPTIONS",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="OPTIONS",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def get(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "GET",
                        cookies=cookies,
                        data=None,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="GET",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def post(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[str | BaseModel | tuple | dict | list | Data] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "POST",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=data,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="POST",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def put(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[str | BaseModel | tuple | dict | list | Data] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "PUT",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=data,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="PUT",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def patch(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        data: Optional[str | BaseModel | tuple | dict | list | Data] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "PATCH",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=data,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="PATCH",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def delete(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "DELETE",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    method="DELETE",
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def _optimize(
        self,
        optimized_param: URL | Params | Headers | Cookies | Data | Auth,
    ):
        if isinstance(optimized_param, URL):
            await self._optimize_url(optimized_param)

        else:
            self._optimized[optimized_param.call_name] = optimized_param

    async def _optimize_url(self, url: URL):
        try:
            if url:
                (
                    _,
                    connection,
                    optimized_url,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(None, url),
                    timeout=self.timeouts.request_timeout,
                )

                connection.reset()
                self._connections.append(connection)

            # Plain-string requests for the same address reuse this lookup:
            # the resolved URL, under the key the connect path reads. One
            # that never resolved is left for the connect path to look up.
            if optimized_url.ip_addresses:
                self._url_cache[optimized_url.target] = optimized_url

            self._optimized[url.call_name] = url

        except Exception:
            pass

    async def _request(
        self,
        url: str | URL,
        method: str,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[str | BaseModel | tuple | dict | list | Data] = None,
        redirects: int = 3,
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
            auth=auth,
            cookies=cookies,
            headers=headers,
            params=params,
            data=data,
            timings=timings,
        )

        if redirect and (
            location := result.headers.get(b'location')
        ):
            # Each location resolves against the address it came from (RFC
            # 3986: absolute, host-relative and path-relative alike).
            previous_location = url.data if isinstance(url, URL) else url
            location = urljoin(previous_location, location.decode())

            for _ in range(redirects):
                result, redirect, timings = await self._execute(
                    url,
                    method,
                    auth=auth,
                    cookies=cookies,
                    headers=headers,
                    params=params,
                    data=data,
                    redirect_url=location,
                    timings=timings,
                )

                if redirect is False:
                    break

                if (next_location := result.headers.get(b"location")) is None:
                    break

                previous_location = location
                location = urljoin(previous_location, next_location.decode())

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str | URL,
        method: str,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[str | BaseModel | tuple | dict | list | Data] = None,
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
        ] = None,
    ) -> Tuple[
        HTTP3Response,
        bool,
        Dict[
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
        ],
    ]:
        if redirect_url:
            request_url = redirect_url

        connection: HTTP3Connection | None = None

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (error, connection, url) = await asyncio.wait_for(
                self._connect_to_url_location(
                    connection,
                    request_url,
                ),
                timeout=self.timeouts.request_timeout,
            )

            if error or connection is None or connection.protocol is None:
                timings["connect_end"] = time.monotonic()

                if connection:
                    connection.reset()
                    self._connections.append(connection)

                return (
                    HTTP3Response(
                        url=URLMetadata(host=url.hostname, path=url.path),
                        method=method,
                        status=400,
                        status_message=str(error) if error else None,
                        headers=headers,
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            stream_id = connection.protocol.quic.get_next_available_stream_id()

            stream = connection.protocol.get_or_create_stream(stream_id)
            if stream.headers_send_state == HeadersState.AFTER_TRAILERS:
                raise Exception("HEADERS frame is not allowed in this state")

            encoded_headers = self._encode_headers(
                url,
                method,
                auth=auth,
                params=params,
                headers=headers,
                cookies=cookies,
            )

            encoder, frame_data = connection.protocol.encoder.encode(
                stream_id,
                encoded_headers,
            )

            connection.protocol.encoder_bytes_sent += len(encoder)
            connection.protocol.quic.send_stream_data(
                connection.protocol._local_encoder_stream_id,
                encoder,
            )

            # The body as sent, encoded ahead of the headers: a request with
            # none -- no data, or data that encodes to nothing -- ends its
            # stream on HEADERS, and data that is falsy but encodes to bytes
            # ({} or []) is sent like any other.
            encoded_data = self._encode_data(data) if data is not None else None

            # update state and send headers
            if stream.headers_send_state == HeadersState.INITIAL:
                stream.headers_send_state = HeadersState.AFTER_HEADERS
            else:
                stream.headers_send_state = HeadersState.AFTER_TRAILERS

            connection.protocol.quic.send_stream_data(
                stream_id,
                encode_frame(FrameType.HEADERS, frame_data),
                end_stream=not encoded_data,
            )

            if encoded_data:
                stream = connection.protocol.get_or_create_stream(stream_id)
                if stream.headers_send_state != HeadersState.AFTER_HEADERS:
                    raise Exception("DATA frame is not allowed in this state")

                connection.protocol.quic.send_stream_data(
                    stream_id,
                    encode_frame(
                        FrameType.DATA,
                        encoded_data,
                    ),
                    True,
                )

            waiter = connection.protocol.loop.create_future()
            connection.protocol._request_waiter[stream_id] = waiter
            connection.protocol.transmit()

            if timings["write_end"] is None:
                timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            response_frames: ResponseFrameCollection = await asyncio.wait_for(
                waiter,
                timeout=self.timeouts.request_timeout,
            )

            headers: Dict[str, Union[bytes, int]] = {}
            for header_key, header_value in response_frames.headers_frame.headers:
                headers[header_key] = header_value

            trailers: Dict[bytes, bytes] | None = None
            if (trailers_frame := response_frames.trailers_frame) is not None:
                trailers = dict(trailers_frame.headers)

            status = int(headers.get(b":status", b"400"))

            cookies: Union[HTTPCookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b"set-cookie")
            if cookies_data:
                cookies = HTTPCookies()
                cookies.update(cookies_data)
            
            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()
                self._connections.append(connection)

                return (
                    HTTP3Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                            params=url.params,
                            query=url.query,
                        ),
                        method=method,
                        status=status,
                        headers=headers,
                        trailers=trailers,
                        timings=timings,
                    ),
                    True,
                    timings,
                )

            self._connections.append(connection)

            timings["read_end"] = time.monotonic()

            return (
                HTTP3Response(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                        params=url.params,
                        query=url.query,
                    ),
                    cookies=cookies,
                    method=method,
                    status=status,
                    headers=headers,
                    trailers=trailers,
                    content=response_frames.body,
                    timings=timings,
                ),
                False,
                timings,
            )

        except (
            BaseException,
            Exception,
        ) as request_exception:
            if connection:
                connection.reset()
                self._connections.append(connection)

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

            timings["read_end"] = time.monotonic()

            return (
                HTTP3Response(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                        params=request_url.params,
                        query=request_url.query,
                    ),
                    method=method,
                    status=400,
                    # A TimeoutError's own message is empty.
                    status_message=(
                        "Request timed out."
                        if isinstance(request_exception, asyncio.TimeoutError)
                        else str(request_exception)
                    ),
                    timings=timings,
                ),
                False,
                timings,
            )

    async def _connect_to_url_location(
        self,
        connection: HTTP3Connection | None,
        request_url: str | URL,
    ) -> Tuple[
        Optional[Exception],
        HTTP3Connection,
        HTTPUrl,
    ]:
        if isinstance(request_url, URL):
            # Resolved when the workflow prepared it: never looked up here,
            # and never read from or added to the lookup cache.
            url = parsed_url = request_url.optimized

        else:
            parsed_url = HTTPUrl(request_url)

            # A lookup serves only the address it resolved: its target (scheme
            # and authority), not the hostname every port on a host shares.
            cache_key = parsed_url.target
            url = self._url_cache.get(cache_key)

            if url is None:
                dns_lock = self._dns_lock[cache_key]
                dns_waiter = self._dns_waiters[cache_key]

                if dns_lock.locked() is False:
                    try:
                        async with dns_lock:
                            url = parsed_url
                            await url.lookup()

                            self._url_cache[cache_key] = url

                    finally:
                        # However the lookup ended, release its waiters; after
                        # a failed or cancelled lookup the next request looks
                        # up again with a fresh waiter.
                        if dns_waiter.done() is False:
                            dns_waiter.set_result(None)

                        if cache_key not in self._url_cache:
                            del self._dns_waiters[cache_key]

                else:
                    # Shielded: a waiter's cancellation must not cancel the
                    # lookup future every other waiter shares.
                    await asyncio.shield(dns_waiter)
                    url = self._url_cache.get(cache_key)

        connection = self._connections.pop()

        if url.is_ssl is False:
            # QUIC always carries TLS: an http:// address names no HTTP/3
            # server.
            return (
                ConnectionError(f"HTTP/3 requires an https:// address, not {url.full}"),
                connection,
                parsed_url,
            )

        connection_error: Optional[Exception] = None

        try:
            # Reuses the connection's QUIC connection to this host; otherwise
            # opens a new one across the host's addresses.
            address, socket_config, new_connection = await connection.connect_to_any(
                parsed_url.target,
                url.ip_addresses,
                url.port,
                url.address_rotation,
                server_name=url.hostname,
                ssl=self._client_ssl_context,
            )

            if new_connection:
                url.address = address
                url.socket_config = socket_config

        except asyncio.CancelledError as err:
            return (
                err,
                connection,
                parsed_url,
            )

        except Exception as err:
            connection_error = err

        try:
            return (
                connection_error,
                connection,
                parsed_url,
            )

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    def _encode_headers(
        self,
        url: HTTPUrl | URL,
        method: str,
        auth: tuple[str, str] | Auth | None = None,
        params: Optional[Dict[str, str] | Params] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
    ):
        if isinstance(url, URL):
            url = url.optimized

        url_path = url.path

        # The params follow any query the address has of its own: after "&"
        # then, else after "?" (RFC 3986 3.4).
        if isinstance(params, Params):
            query = params.optimized[1:]
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        elif params:
            query = urlencode(params)
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        # :authority names the target as RFC 9114 4.3.1 does: the host, an
        # IPv6 address in brackets, and the port unless it is the scheme's
        # default.
        hostname = url.hostname
        scheme = url.scheme
        authority = f"[{hostname}]" if ":" in hostname else hostname
        if url.port != DEFAULT_PORTS.get(scheme):
            authority = f"{authority}:{url.port}"

        encoded_headers: List[Tuple[bytes, bytes]] = [
            (b":method", method.encode()),
            (b":authority", authority.encode()),
            (b":scheme", scheme.encode()),
            (b":path", url_path.encode()),
        ]

        if isinstance(auth, Auth):
            encoded_headers.append(auth.optimized)

        elif auth is not None:
            encoded_headers.append(
                self._encode_auth_headers(auth),
            )

        if isinstance(headers, Headers):
            encoded_headers.extend(headers.optimized)

        elif headers:
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

        if isinstance(cookies, Cookies):
            encoded_headers.append(cookies.optimized)

        elif cookies:
            encoded_cookies: List[str] = []

            for cookie_data in cookies:
                if len(cookie_data) == 1:
                    encoded_cookies.append(cookie_data[0])

                elif len(cookie_data) == 2:
                    cookie_name, cookie_value = cookie_data
                    encoded_cookies.append(f"{cookie_name}={cookie_value}")

            # A header field as the QPACK encoder takes one: bytes.
            encoded_headers.append((b"cookie", "; ".join(encoded_cookies).encode()))

        return encoded_headers

    def _encode_data(
        self,
        data: str | BaseModel | tuple | dict | list | Data | bytes,
    ):
        encoded_data: Optional[bytes] = None

        if isinstance(data, Data):
            return data.optimized

        elif isinstance(data, Iterator) and not isinstance(data, list):
            # HTTP/3 has no chunked transfer coding: a DATA frame carries the
            # body as it is (RFC 9114 4.1).
            encoded_data = b"".join(data)

        elif isinstance(data, BaseModel):
            encoded_data = orjson.dumps(data.model_dump())

        elif isinstance(data, (dict, list)):
            encoded_data = orjson.dumps(data)

        elif isinstance(data, tuple):
            encoded_data = urlencode(data).encode()

        elif isinstance(data, str):
            encoded_data = data.encode()

        else:
            encoded_data = data

        return encoded_data
    
    def _encode_auth_headers(
        self,
        auth: tuple[str, str] | tuple[str],
    ):
        # The Basic scheme ahead of the credentials (RFC 7617 2), as the
        # HTTP/1 client sends them.
        if len(auth) > 1:
            credentials_string = f"{auth[0]}:{auth[1]}"
            return (
                b"authorization",
                b"Basic " + base64.b64encode(
                    credentials_string.encode()
                )
            )

        else:
            return (
                b"authorization",
                b"Basic " + base64.b64encode(
                    auth[0].encode()
                )
            )

    def close(self):
        for connection in self._connections:
            connection.close()
