import asyncio
import ssl
import time
from collections import defaultdict, deque
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

from hyperscale.core_rewrite.engines.client.http3.protocols.quic_protocol import (
    FrameType,
    HeadersState,
    ResponseFrameCollection,
    encode_frame,
)
from hyperscale.core_rewrite.engines.client.shared.models import (
    URL,
    Cookies,
    HTTPCookie,
    HTTPEncodableValue,
    URLMetadata,
)
from hyperscale.core_rewrite.engines.client.shared.protocols import NEW_LINE
from hyperscale.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.http3 import HTTP3Response
from .protocols import HTTP3Connection

A = TypeVar("A")
R = TypeVar("R")


class MercurySyncHTTP3Connection:
    def __init__(
        self,
        pool_size: Optional[int] = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        if pool_size is None:
            pool_size = 100

        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._client_ssl_context = self._create_general_client_ssl_context()

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[HTTP3Connection] = [
            HTTP3Connection(reset_connections=reset_connections)
            for _ in range(pool_size)
        ]

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._connections_count: Dict[str, List[asyncio.Transport]] = defaultdict(list)
        self._locks: Dict[asyncio.Transport, asyncio.Lock] = {}

        self._max_concurrency = pool_size

        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self._connection_waiters: List[asyncio.Future] = []

        self._url_cache: Dict[str, URL] = {}

    def _create_general_client_ssl_context(self):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        return ctx

    async def head(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Optional[int | float] = None,
        data: Optional[str | BaseModel | tuple | dict | list] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Optional[int | float] = None,
        data: Optional[str | BaseModel | tuple | dict | list] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Optional[int | float] = None,
        data: Optional[str | BaseModel | tuple | dict | list] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
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
                url_data = urlparse(url)

                return HTTP3Response(
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
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        data: Optional[str | BaseModel | tuple | dict | list] = None,
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

        if redirect:
            location = result.headers.get("location")

            upgrade_ssl = False
            if "https" in location and "https" not in url:
                upgrade_ssl = True

            for _ in range(redirects):
                result, redirect, timings = await self._execute(
                    url,
                    method,
                    auth=auth,
                    cookies=cookies,
                    headers=headers,
                    params=params,
                    data=data,
                    upgrade_ssl=upgrade_ssl,
                    redirect_url=location,
                    timings=timings,
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
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = None,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        data: Optional[str | BaseModel | tuple | dict | list] = None,
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

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (connection, url, upgrade_ssl) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url, ssl_redirect_url=request_url if upgrade_ssl else None
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                connection, url, _ = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url, ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            if connection.protocol is None:
                timings["connect_end"] = time.monotonic()
                self._connections.append(
                    HTTP3Connection(
                        reset_connections=self.reset_connections,
                    )
                )

                return (
                    HTTP3Response(
                        url=URLMetadata(host=url.hostname, path=url.path),
                        method=method,
                        status=400,
                        headers=headers,
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            encoded_headers = self._encode_headers(
                url,
                method,
                params=params,
                headers=headers,
            )

            stream_id = connection.protocol.quic.get_next_available_stream_id()

            stream = connection.protocol.get_or_create_stream(stream_id)
            if stream.headers_send_state == HeadersState.AFTER_TRAILERS:
                raise Exception("HEADERS frame is not allowed in this state")

            encoder, frame_data = connection.protocol.encoder.encode(
                stream_id, encoded_headers
            )

            connection.protocol.encoder_bytes_sent += len(encoder)
            connection.protocol.quic.send_stream_data(
                connection.protocol._local_encoder_stream_id, encoder
            )

            # update state and send headers
            if stream.headers_send_state == HeadersState.INITIAL:
                stream.headers_send_state = HeadersState.AFTER_HEADERS
            else:
                stream.headers_send_state = HeadersState.AFTER_TRAILERS

            connection.protocol.quic.send_stream_data(
                stream_id,
                encode_frame(FrameType.HEADERS, frame_data),
                end_stream=not data,
            )

            if data:
                encoded_data = self._encode_data()

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
            connection.protocol.request_events[stream_id] = deque()
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

            status = int(headers.get(b":status", b"400"))

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
                        timings=timings,
                    ),
                    True,
                    timings,
                )

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b"set-cookie")
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

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
                    content=response_frames.body,
                    timings=timings,
                ),
                False,
                timings,
            )

        except Exception as request_exception:
            self._connections.append(
                HTTP3Connection(reset_connections=self.reset_connections)
            )

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

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
                    status_message=str(request_exception),
                    timings=timings,
                ),
                False,
                timings,
            )

    async def _connect_to_url_location(
        self, request_url: str, ssl_redirect_url: Optional[str] = None
    ) -> Tuple[HTTP3Connection, URL, bool]:
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
                        return None, parsed_url, True

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
                    return None, parsed_url, True

                raise connection_error

        return connection, parsed_url, False

    def _encode_headers(
        self,
        url: URL,
        method: str,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        url_path = url.path
        if params:
            url_params = urlencode(params)
            url_path += f"?{url_params}"

        encoded_headers: List[Tuple[bytes, bytes]] = [
            (b":method", method.encode()),
            (b":scheme", url.scheme.encode()),
            (b":authority", url.authority.encode()),
            (b":path", url_path.encode()),
            (b"user-agent", b"hyperscale/client"),
        ]

        if headers:
            encoded_headers.extend(
                [
                    (
                        k.encode(),
                        v.encode(),
                    )
                    for (k, v) in headers.items()
                ]
            )

        return encoded_headers

    def _encode_data(
        self,
        data: str | BaseModel | tuple | dict | list,
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

            self.is_stream = True
            encoded_data = chunks

        else:
            if isinstance(data, (dict, list)):
                encoded_data = orjson.dumps(data)

            elif isinstance(data, BaseModel):
                return data.model_dump_json().encode()

            elif isinstance(data, tuple):
                encoded_data = urlencode(data).encode()

            elif isinstance(data, str):
                encoded_data = data.encode()

        return encoded_data
