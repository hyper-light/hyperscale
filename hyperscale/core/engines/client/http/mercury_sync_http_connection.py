from __future__ import annotations

import asyncio
import ssl
import socket
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
    urljoin
)

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import (
    URL as HTTPUrl,
)
from hyperscale.core.engines.client.shared.models import (
    Cookies as HTTPCookies,
)
from hyperscale.core.engines.client.shared.models import (
    HTTPCookie,
    HTTPEncodableValue,
    RequestType,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.protocols import (
    NEW_LINE,
    ProtocolMap,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Params,
)

from .models.http import (
    HTTPResponse,
)
from .protocols import HTTPConnection


class MercurySyncHTTPConnection:
    def __init__(
        self,
        pool_size: Optional[int] = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        if pool_size is None:
            pool_size = 100

        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._client_ssl_context: Optional[ssl.SSLContext] = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[HTTPConnection] = []

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: List[asyncio.Future] = []

        self._url_cache: Dict[str, HTTPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.HTTP]
        self._optimized: Dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}

        self.address_family = address_family
        self.address_protocol = protocol

    async def head(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
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

                return HTTPResponse(
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
                    timings={},
                )

    async def options(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
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

                return HTTPResponse(
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
                    timings={},
                )

    async def get(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
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

                return HTTPResponse(
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
                    timings={},
                )

    async def post(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data
        ] = None,
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

                return HTTPResponse(
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
                    timings={},
                )

    async def put(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        data: Optional[
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data
        ] = None,
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

                return HTTPResponse(
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
                    timings={},
                )

    async def patch(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data
        ] = None,
        timeout: Optional[int | float] = None,
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

                return HTTPResponse(
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
                    timings={},
                )

    async def delete(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
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

                return HTTPResponse(
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

    async def _optimize_url(self, optimized_url: URL):

        upgrade_ssl: bool = False
        (
            _,
            connection,
            url,
            upgrade_ssl,
        ) = await asyncio.wait_for(
            self._connect_to_url_location(optimized_url),
            timeout=self.timeouts.connect_timeout,
        )
        if upgrade_ssl:
            optimized_url.data = optimized_url.data.replace("http://", "https://")

            await optimized_url.optimize()

            (
                _,
                connection,
                url,
                _,
            ) = await asyncio.wait_for(
                self._connect_to_url_location(optimized_url),
                timeout=self.timeouts.connect_timeout,
            )

        self._url_cache[optimized_url.optimized.hostname] = url
        self._optimized[optimized_url.call_name] = url

        self._connections.append(connection)
        
    async def _request(
        self,
        url: str | URL,
        method: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data
        ] = None,
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
            location = location.decode()

            redirects_taken = 1

            upgrade_ssl = False

            if "http" not in location and "https" not in location:
                parsed_url: ParseResult = urlparse(url)

                if parsed_url.params:
                    location += parsed_url.params

                location = urljoin(
                    f'{parsed_url.scheme}://{parsed_url.hostname}',
                    location
                )

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

                location = result.headers.get(b"location").decode()

                upgrade_ssl = False
                if "https" in location and "https" not in url:
                    upgrade_ssl = True

                redirects_taken += 1

            result.redirects = redirects_taken

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str | URL,
        method: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data
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
        ] = None,
    ) -> Tuple[
        HTTPResponse,
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

            (error, connection, url, upgrade_ssl) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url,
                    ssl_redirect_url=request_url if upgrade_ssl else None,
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                (error, connection, url, _) = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url,
                        ssl_redirect_url=ssl_redirect_url,
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            encoded_data: Optional[bytes | List[bytes]] = None
            content_type: Optional[str] = None

            if connection.reader is None or error:
                timings["connect_end"] = time.monotonic()
                self._connections.append(
                    HTTPConnection(
                        reset_connections=self.reset_connections,
                    )
                )

                return (
                    HTTPResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                            params=url.params,
                            query=url.query,
                        ),
                        method=method,
                        status=400,
                        status_message="Connection failed.",
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
                data=data,
                encoded_data=encoded_data,
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

            headers: Dict[bytes, bytes] = await asyncio.wait_for(
                connection.read_headers(),
                timeout=self.timeouts.read_timeout,
            )

            content_length = headers.get(b"content-length")
            transfer_encoding = headers.get(b"transfer-encoding")

            cookies: Union[HTTPCookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b"set-cookie")
            if cookies_data:
                cookies = HTTPCookies()
                cookies.update(cookies_data)
                


            # We require Content-Length or Transfer-Encoding headers to read a
            # request body, otherwise it's anyone's guess as to how big the body
            # is, and we ain't playing that game.

            body = b''

            if content_length:
                body = await asyncio.wait_for(
                    connection.readexactly(int(content_length)),
                    timeout=self.timeouts.read_timeout,
                )

            elif transfer_encoding:
                body = bytearray()
                all_chunks_read = False

                while True and not all_chunks_read:
                    chunk_size = int(
                        (
                            await asyncio.wait_for(
                                connection.readline(),
                                timeout=self.timeouts.read_timeout,
                            )
                        ).rstrip(),
                        16,
                    )

                    if not chunk_size:
                        # read last CRLF
                        await asyncio.wait_for(
                            connection.readline(), timeout=self.timeouts.read_timeout
                        )
                        break

                    chunk = await asyncio.wait_for(
                        connection.readexactly(chunk_size + 2),
                        self.timeouts.read_timeout,
                    )
                    body.extend(chunk[:-2])

                all_chunks_read = True

            self._connections.append(connection)

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()
                self._connections.append(connection)

                return (
                    HTTPResponse(
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

            timings["read_end"] = time.monotonic()

            return (
                HTTPResponse(
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
                    content=body,
                    timings=timings,
                ),
                False,
                timings,
            )

        except (
            Exception,
            socket.error
        ):
            self._connections.append(
                HTTPConnection(
                    reset_connections=self.reset_connections,
                )
            )

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

            timings["read_end"] = time.monotonic()

            return (
                HTTPResponse(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                        params=request_url.params,
                        query=request_url.query,
                    ),
                    method=method,
                    status=400,
                    status_message="Request failed or timed out.",
                    timings=timings,
                ),
                False,
                timings,
            )
        
    async def _connect_to_url_location(
        self,
        request_url: str | URL,
        ssl_redirect_url: Optional[str | URL] = None,
    ) -> Tuple[
        Optional[Exception],
        HTTPConnection,
        HTTPUrl,
        bool,
    ]:
        has_optimized_url = isinstance(request_url, URL)

        if has_optimized_url:
            parsed_url = request_url.optimized

        elif ssl_redirect_url:
            parsed_url = HTTPUrl(
                ssl_redirect_url,
                family=self.address_family,
                protocol=self.address_protocol,
            )

        else:
            parsed_url = HTTPUrl(
                request_url,
                family=self.address_family,
                protocol=self.address_protocol,
            )

        url = self._url_cache.get(parsed_url.hostname)
        dns_lock = self._dns_lock[parsed_url.hostname]
        dns_waiter = self._dns_waiters[parsed_url.hostname]

        do_dns_lookup = (
            url is None or ssl_redirect_url
        ) and has_optimized_url is False

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

        elif has_optimized_url:
            url = request_url.optimized

        connection = self._connections.pop()
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

                except Exception as err:
                    if "server_hostname is only meaningful with ssl" in str(err):
                        return (
                            None,
                            parsed_url,
                            True,
                        )

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

            except Exception as err:
                if "server_hostname is only meaningful with ssl" in str(err):
                    return None, parsed_url, True

                connection_error = err

        return (
            connection_error,
            connection,
            parsed_url,
            False,
        )

    def _encode_data(
        self,
        data: str | bytes | BaseModel | bytes | Data,
    ):
        content_type: Optional[str] = None
        encoded_data: bytes | List[bytes] = None

        if isinstance(data, Data):
            encoded_data = data.optimized
            content_type = data.content_type

        elif isinstance(data, Iterator) and not isinstance(data, list):
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
        url: URL | HTTPUrl,
        method: str,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        data: Optional[
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data
        ] = None,
        encoded_data: Optional[bytes | List[bytes]] = None,
        content_type: Optional[str] = None,
    ):
        if isinstance(url, URL):
            url = url.optimized

        url_path = url.path

        if isinstance(params, Params):
            url_path += params.optimized

        elif params and len(params) > 0:
            url_params = urlencode(params)
            url_path += f"?{url_params}"

        port = url.port or (443 if url.scheme == "https" else 80)
        hostname = url.parsed.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        header_items = (
            f"{method} {url_path} HTTP/1.1{NEW_LINE}HOST: {hostname}{NEW_LINE}"
        )

        if isinstance(headers, Headers):
            header_items += headers.optimized
        elif headers:
            header_items += f"Keep-Alive: timeout=60, max=100000{NEW_LINE}User-Agent: hyperscale/client{NEW_LINE}"

            for key, value in headers.items():
                header_items += f"{key}: {value}{NEW_LINE}"

        else:
            header_items += f"Keep-Alive: timeout=60, max=100000{NEW_LINE}User-Agent: hyperscale/client{NEW_LINE}"

        size: int = 0

        if isinstance(data, Data):
            size = data.content_length

        elif encoded_data and isinstance(encoded_data, Iterator):
            size = sum([len(chunk) for chunk in encoded_data])

        elif encoded_data:
            size = len(encoded_data)

        header_items += f"Content-Length: {size}{NEW_LINE}"

        if content_type:
            header_items += f"Content-Type: {content_type}{NEW_LINE}"

        if isinstance(cookies, Cookies):
            header_items += cookies.optimized

        elif cookies:
            encoded_cookies: List[str] = []

            for cookie_data in cookies:
                if len(cookie_data) == 1:
                    encoded_cookies.append(cookie_data[0])

                elif len(cookie_data) == 2:
                    cookie_name, cookie_value = cookie_data
                    encoded_cookies.append(f"{cookie_name}={cookie_value}")

            encoded = "; ".join(encoded_cookies)
            header_items += f"cookie: {encoded}{NEW_LINE}"

        return f"{header_items}{NEW_LINE}".encode()

    def close(self):
        for connection in self._connections:
            connection.close()
