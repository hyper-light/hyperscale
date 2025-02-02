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
    urljoin,
)

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import URL as HTTPUrl
from hyperscale.core.engines.client.shared.models import Cookies as HTTPCookies
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

        self._semaphore: asyncio.Semaphore = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)

        self._connections: List[HTTP2Connection] = []

        self._pipes: List[HTTP2Pipe] = []

        self._url_cache: Dict[str, HTTPUrl] = {}

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._encoder: Encoder = None
        self._settings: Settings = None

        self._client_ssl_context: Optional[ssl.SSLContext] = None
        self._optimized: Dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.HTTP2]

        self.address_family = address_family
        self.address_protocol = protocol

    async def head(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str]] = None,
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
                        auth=auth,
                        cookies=cookies,
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
                        auth=auth,
                        cookies=cookies,
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
                        auth=auth,
                        cookies=cookies,
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
                    timings={},
                )

    async def post(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str
            | bytes
            | Iterator
            | Dict[str, HTTPEncodableValue]
            | List[str]
            | BaseModel
            | Data
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
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

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
                    timings={},
                )

    async def put(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str
            | bytes
            | Iterator
            | Dict[str, HTTPEncodableValue]
            | List[str]
            | BaseModel
            | Data
        ] = None,
        timeout: Optional[int | float] = None,
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
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

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
                    timings={},
                )

    async def patch(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            str
            | bytes
            | Iterator
            | Dict[str, HTTPEncodableValue]
            | List[str]
            | BaseModel
            | Data
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
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

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
                        auth=auth,
                        cookies=cookies,
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
            upgrade_ssl: bool = False
            (
                _,
                connection,
                pipe,
                optimized_url,
                upgrade_ssl,
            ) = await asyncio.wait_for(
                self._connect_to_url_location(url),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                url.data = url.data.replace("http://", "https://")

                await url.optimize()

                (
                    _,
                    connection,
                    pipe,
                    optimized_url,
                    _,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )

            self._connections.append(connection)
            self._pipes.append(pipe)

            self._url_cache[optimized_url.hostname] = optimized_url
            self._optimized[url.call_name] = url

        except Exception:
            pass

    async def _request(
        self,
        url: str | URL,
        method: str,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        auth: Optional[Tuple[str, str] | Auth] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
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

        if redirect and (
            location := result.headers.get('location')
        ):

            if "http" not in location and "https" not in location:
                parsed_url: ParseResult = urlparse(url)

                if parsed_url.params:
                    location += parsed_url.params

                location = urljoin(
                    f'{parsed_url.scheme}://{parsed_url.hostname}',
                    location
                )

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
        request_url: str | URL,
        method: str,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        auth: Optional[Tuple[str, str] | Auth] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
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
        ] = None,
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
                        request_url,
                        ssl_redirect_url=ssl_redirect_url,
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            if error:
                timings["connect_end"] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )

                self._pipes.append(HTTP2Pipe(self._concurrency))

                return (
                    HTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message="Connection failed.",
                        headers={
                            key.encode(): value.encode()
                            for key, value in headers.items()
                        }
                        if headers
                        else {},
                        timings=timings,
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
                cookies=cookies,
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
                pipe.receive_response(connection),
                timeout=self.timeouts.read_timeout,
            )

            if error:
                raise error

            cookies: Union[HTTPCookies, None] = None

            cookies_data: Union[str, None] = headers.get("set-cookie")
            if cookies_data:
                cookies = HTTPCookies()
                cookies.update(cookies_data.encode())

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()

                self._connections.append(connection)
                self._pipes.append(pipe)

                return (
                    HTTP2Response(
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
                    timings=timings,
                ),
                False,
                timings,
            )

        except Exception:
            self._connections.append(
                HTTP2Connection(
                    stream_id=randrange(1, 2**20 + 2, 2),
                    reset_connections=self._reset_connections,
                )
            )
            self._pipes.append(HTTP2Pipe(self._concurrency))

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

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
                    status_message="Request failed or timed out.",
                    timings=timings,
                ),
                False,
                timings,
            )

    def _encode_data(
        self,
        data: str | bytes | BaseModel | bytes | Data,
    ):
        encoded_data: Optional[bytes] = None

        if isinstance(data, Data):
            encoded_data = data.optimized

        elif isinstance(data, Iterator) and not isinstance(data, list):
            chunks = []
            for chunk in data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                chunks.append(encoded_chunk)

            encoded_data = chunks

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

    def _encode_headers(
        self,
        url: HTTPUrl,
        method: str,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
    ):
        if isinstance(url, URL):
            url = url.optimized

        url_path = url.path

        if isinstance(params, Params):
            url_path += params.optimized

        elif params:
            url_params = urlencode(params)
            url_path += f"?{url_params}"

        if isinstance(headers, Headers):
            encoded_headers: List[Tuple[bytes, bytes]] = [
                (b":method", method.encode()),
                (b":authority", url.hostname.encode()),
                (b":scheme", url.scheme.encode()),
                (b":path", url_path.encode()),
            ]

            encoded_headers.extend(headers.optimized)

        elif headers:
            encoded_headers: List[Tuple[bytes, bytes]] = [
                (b":method", method.encode()),
                (b":authority", url.hostname.encode()),
                (b":scheme", url.scheme.encode()),
                (b":path", url_path.encode()),
            ]

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

        else:
            encoded_headers: List[Tuple[bytes, bytes]] = [
                (b":method", method.encode()),
                (b":authority", url.hostname.encode()),
                (b":scheme", url.scheme.encode()),
                (b":path", url_path.encode()),
            ]

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

            encoded_headers.append(
                (
                    b"cookie",
                    "; ".join(encoded_cookies).encode(),
                )
            )

        encoded_headers: bytes = self._encoder.encode(encoded_headers)
        encoded_headers: List[bytes] = [
            encoded_headers[i : i + self._settings.max_frame_size]
            for i in range(0, len(encoded_headers), self._settings.max_frame_size)
        ]

        return encoded_headers[0]

    async def _connect_to_url_location(
        self,
        request_url: str | URL,
        ssl_redirect_url: Optional[str] = None,
    ) -> Tuple[
        Optional[Exception],
        HTTP2Connection,
        HTTP2Pipe,
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

        do_dns_lookup = (url is None or ssl_redirect_url) and has_optimized_url is False

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

                except Exception as err:
                    if "server_hostname is only meaningful with ssl" in str(err):
                        return (
                            None,
                            None,
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
                    return (
                        None,
                        None,
                        None,
                        parsed_url,
                        True,
                    )

                connection_error = err

        return (
            connection_error,
            connection,
            pipe,
            parsed_url,
            False,
        )

    def close(self):
        for connection in self._connections:
            connection.close()
