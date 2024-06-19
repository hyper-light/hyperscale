import asyncio
import ssl
import time
from collections import defaultdict
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
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

from .connection import WebsocketConnection
from .models.websocket import (
    WebsocketResponse,
    get_header_bits,
    get_message_buffer_size,
)


class MercurySyncWebsocketConnection:
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
        self._connections: List[WebsocketConnection] = [
            WebsocketConnection(
                reset_connections=reset_connections,
            )
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

    async def receive(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        data: Optional[str | Dict[str, Any] | List[Any] | BaseModel] = None,
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

                return WebsocketResponse(
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

    async def send(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        data: Optional[str | Dict[str, Any] | List[Any] | BaseModel] = None,
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
                url_data = urlparse(url)

                return WebsocketResponse(
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

    async def _request(
        self,
        url: str,
        method: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        data: Optional[str | Dict[str, Any] | List[Any] | BaseModel] = None,
        redirects: int = 3,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
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
            params=params,
            cookies=cookies,
            headers=headers,
            data=data,
            timings=timings,
        )

        if redirect:
            location = result.headers.get(b"location").decode()

            upgrade_ssl = False
            if "https" in location and "https" not in url:
                upgrade_ssl = True

            for _ in range(redirects):
                result, redirect, timings = await self._execute(
                    url,
                    method,
                    auth=auth,
                    params=params,
                    cookies=cookies,
                    headers=headers,
                    data=data,
                    upgrade_ssl=upgrade_ssl,
                    redirect_url=location,
                    timings=timings,
                )

                if redirect is False:
                    break

                location = result.headers.get(b"location").decode()

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
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        data: Optional[str | Dict[str, Any] | List[Any] | BaseModel] = None,
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
        WebsocketResponse,
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

            if connection.reader is None:
                timings["connect_end"] = time.monotonic()
                self._connections.append(
                    WebsocketConnection(
                        reset_connections=self.reset_connections,
                    )
                )

                return (
                    WebsocketResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
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

            encoded_data: Optional[bytes | List[bytes]] = None
            content_type: Optional[str] = None

            if data:
                encoded_data, content_type = self._encode_data(data)

            encoded_headers = self._encode_headers(
                url,
                method,
                params=params,
                headers=headers,
                cookies=cookies,
                data=encoded_data,
                content_type=content_type,
            )

            connection.write(encoded_headers)

            if isinstance(encoded_data, Iterator):
                for chunk in encoded_data:
                    connection.write(chunk)

                connection.write(("0" + NEW_LINE * 2).encode())

            elif data:
                connection.write(encoded_data)

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            response_code = await asyncio.wait_for(
                connection.reader.readline(), timeout=self.timeouts.read_timeout
            )

            status_string: List[bytes] = response_code.split()
            status = int(status_string[1])

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()
                self._connections.append(connection)

                return (
                    WebsocketResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=status,
                        headers=headers,
                        timings=timings,
                    ),
                    True,
                    timings,
                )

            headers: Dict[bytes, bytes] = {}

            raw_headers = b""
            async for key, value, header_line in connection.reader.iter_headers(
                connection
            ):
                headers[key] = value
                raw_headers += header_line

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b"set-cookie")
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

            header_content_length = 0
            if data:
                header_bits = get_header_bits(raw_headers)
                header_content_length = get_message_buffer_size(header_bits)

            body_size = min(16384, header_content_length)

            if body_size > 0:
                body = await asyncio.wait_for(
                    connection.readexactly(body_size), self.timeouts.request_timeout
                )

            self._connections.append(connection)

            timings["read_end"] = time.monotonic()

            return (
                WebsocketResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                    ),
                    method=method,
                    status=status,
                    headers=headers,
                    content=body,
                    timings=timings,
                ),
                False,
                timings,
            )

        except Exception as request_exception:
            self._connections.append(
                WebsocketConnection(
                    reset_connection=self.reset_connections,
                )
            )

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            timings["read_end"] = time.monotonic()

            return (
                WebsocketResponse(
                    url=URLMetadata(host=request_url.hostname, path=request_url.path),
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
    ) -> Tuple[WebsocketConnection, URL, bool]:
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

    def _encode_data(
        self,
        data: str | bytes | BaseModel | bytes,
    ):
        content_type: Optional[str] = None
        encoded_data: bytes | List[bytes] = None

        if isinstance(data, Iterator):
            chunks: List[bytes] = []
            for chunk in data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                chunks.append(encoded_chunk)

            encoded_data = chunks

        elif isinstance(data, BaseModel):
            encoded_data = orjson.dumps(data.model_dump())
            content_type = "application/json"

        elif isinstance(data, (dict, list)):
            encoded_data = orjson.dumps(data)
            content_type = "application/json"

        elif isinstance(data, str):
            encoded_data = data.encode()

        elif isinstance(data, (memoryview, bytearray)):
            encoded_data = bytes(data)

        return encoded_data, content_type

    def _encode_headers(
        self,
        url: URL,
        method: str,
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        data: Optional[bytes | List[bytes]] = None,
        content_type: Optional[str] = None,
    ):
        url_path = url.path

        if params and len(params) > 0:
            url_params = urlencode(params)
            url_path += f"?{url_params}"

        get_base = f"{method} {url.path} HTTP/1.1{NEW_LINE}"

        port = url.port or (443 if url.scheme == "https" else 80)

        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        header_items = {
            "user-agent": "hyperscale/client",
        }

        if data and isinstance(data, Iterator):
            header_items["content-length"] = sum([len(chunk) for chunk in data])

        elif data:
            header_items["content-length"] = len(data)

        if content_type:
            header_items["content-type"] = content_type

        if headers:
            header_items.update(headers)

        for key, value in header_items.items():
            get_base += f"{key}: {value}{NEW_LINE}"

        if cookies:
            cookies = []

            for cookie_data in cookies:
                if len(cookie_data) == 1:
                    cookies.append(cookie_data[0])

                elif len(cookie_data) == 2:
                    cookie_name, cookie_value = cookie_data
                    cookies.append(f"{cookie_name}={cookie_value}")

            cookies = "; ".join(cookies)
            get_base += f"cookie: {cookies}{NEW_LINE}"

        return (get_base + NEW_LINE).encode(), data
