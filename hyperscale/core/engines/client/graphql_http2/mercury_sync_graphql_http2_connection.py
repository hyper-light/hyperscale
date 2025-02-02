import asyncio
import time
import uuid
from random import randrange
from typing import (
    Dict,
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

from hyperscale.core.engines.client.http2 import MercurySyncHTTP2Connection
from hyperscale.core.engines.client.http2.pipe import HTTP2Pipe
from hyperscale.core.engines.client.http2.protocols import HTTP2Connection
from hyperscale.core.engines.client.shared.models import (
    URL as HTTPUrl,
)
from hyperscale.core.engines.client.shared.models import (
    Cookies as HTTPCookies,
)
from hyperscale.core.engines.client.shared.models import (
    HTTPCookie,
    HTTPEncodableValue,
    Metadata,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Headers,
    Mutation,
    Params,
    Query,
)

from .models.graphql_http2 import (
    GraphQLHTTP2Response,
)

T = TypeVar("T")


def mock_fn():
    return None


try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source = None
    parse = mock_fn
    print_ast = mock_fn


class MercurySyncGraphQLHTTP2Connection(MercurySyncHTTP2Connection):
    def __init__(
        self,
        pool_size: int = 10**3,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        super(MercurySyncGraphQLHTTP2Connection, self).__init__(
            pool_size=pool_size,
            timeouts=timeouts,
            reset_connections=reset_connections,
        )

        self.session_id = str(uuid.uuid4())

    async def query(
        self,
        url: str | URL,
        query: str | Query,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | HTTPCookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ) -> GraphQLHTTP2Response:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url=url,
                        method="GET",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        data={
                            "query": query,
                        },
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return GraphQLHTTP2Response(
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

    async def mutate(
        self,
        url: str | URL,
        mutation: Dict[
            Literal[
                "query",
                "operation_name",
                "variables",
            ],
            str | Dict[str, HTTPEncodableValue],
        ]
        | Mutation,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | HTTPCookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ) -> GraphQLHTTP2Response:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url=url,
                        method="POST",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=mutation,
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return GraphQLHTTP2Response(
                    metadata=Metadata(),
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

    async def _optimize(
        self,
        optimized_param: URL | Params | Headers | HTTPCookies | Auth | Query | Mutation,
    ):
        if isinstance(optimized_param, URL):
            await self._optimize_url(optimized_param)

        else:
            self._optimized[optimized_param.call_name] = optimized_param

    async def _optimize_url(self, url: URL):
        try:
            upgrade_ssl: bool = False
            if url:
                (
                    _,
                    connection,
                    pipe,
                    url,
                    upgrade_ssl,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )
                self._connections.append(connection)
                self._pipes.append(pipe)

            if upgrade_ssl:
                url.data = url.data.replace("http://", "https://")

                await url.optimize()

                (
                    _,
                    connection,
                    pipe,
                    url,
                    _,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )

                self._connections.append(connection)
                self._pipes.append(pipe)

            self._url_cache[url.optimized.hostname] = url

        except Exception:
            pass

    async def _request(
        self,
        url: str | URL,
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie] | HTTPCookies] = None,
        auth: Optional[Tuple[str, str] | Auth] = None,
        headers: Optional[Dict[str, str]] = {},
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: (
            Dict[
                Literal["query"],
                str,
            ]
            | Dict[
                Literal[
                    "query",
                    "operation_name",
                    "variables",
                ],
                str | Dict[str, HTTPEncodableValue],
            ]
            | Mutation
        ) = None,
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
            headers=headers,
            params=params,
            data=data,
            timings=timings,
        )

        if redirect and (
            location := result.headers.get('location')
        ):
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
                    cookies=cookies,
                    auth=auth,
                    headers=headers,
                    params=params,
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
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie] | HTTPCookies] = None,
        auth: Optional[Tuple[str, str] | Auth] = None,
        headers: Optional[Dict[str, str]] = {},
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: (
            Dict[
                Literal["query"],
                str,
            ]
            | Dict[
                Literal[
                    "query",
                    "operation_name",
                    "variables",
                ],
                str | Dict[str, HTTPEncodableValue],
            ]
            | Mutation
        ) = None,
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
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )

                self._pipes.append(HTTP2Pipe(self._concurrency))

                return (
                    GraphQLHTTP2Response(
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
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            connection = pipe.send_preamble(connection)

            if method == "POST":
                encoded_data = self._encode_data(data)

                encoded_headers = self._encode_headers(
                    url,
                    method,
                    cookies=cookies,
                    data=data,
                    headers=headers,
                    params=params,
                )

                connection = pipe.send_request_headers(
                    encoded_headers,
                    data,
                    connection,
                )

                connection = await asyncio.wait_for(
                    pipe.submit_request_body(
                        encoded_data,
                        connection,
                    ),
                    timeout=self.timeouts.write_timeout,
                )

            else:
                encoded_headers = self._encode_headers(
                    url,
                    method,
                    cookies=cookies,
                    data=data,
                    headers=headers,
                    params=params,
                )

                connection = pipe.send_request_headers(
                    encoded_headers,
                    data,
                    connection,
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
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )
                self._pipes.append(HTTP2Pipe(self._concurrency))

                return (
                    GraphQLHTTP2Response(
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
                GraphQLHTTP2Response(
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

        except Exception as request_exception:
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
                GraphQLHTTP2Response(
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

    def _encode_data(
        self,
        data: (
            Dict[
                Literal["query"],
                str,
            ]
            | Dict[
                Literal[
                    "query",
                    "operation_name",
                    "variables",
                ],
                str | Dict[str, HTTPEncodableValue],
            ]
            | Mutation
        ),
    ):
        if isinstance(data, Mutation):
            return data.optimized, data.content_type

        source = Source(data.get("query"))
        document_node = parse(source)
        query_string = print_ast(document_node)

        query = {"query": query_string}

        operation_name = data.get("operation_name")
        variables = data.get("variables")

        if operation_name:
            query["operationName"] = operation_name

        if variables:
            query["variables"] = variables

        encoded_data = orjson.dumps(query)

        return encoded_data

    def _encode_headers(
        self,
        url: HTTPUrl,
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie] | HTTPCookies] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: (
            Dict[
                Literal["query"],
                str,
            ]
            | Dict[
                Literal[
                    "query",
                    "operation_name",
                    "variables",
                ],
                str | Dict[str, HTTPEncodableValue],
            ]
            | Mutation
        ) = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        if isinstance(url, URL):
            url = url.optimized

        url_path = url.path

        query_string: str | Query = data.get("query")

        if method == "GET" and isinstance(query_string, Query):
            url_path += query_string

        elif method == "GET":
            query_string = "".join(query_string.replace("query", "").split())
            url_path += f"?query={{{query_string}}}"

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
                (b"User-Agent", b"hyperscale/client"),
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
                (b"user-agent", b"hyperscale/client"),
            ]

        if isinstance(data, Mutation) or (
            data and method == "POST"
        ):
            encoded_headers.extend(
                [
                    (b"Content-Type", b"application/graphql-response+json"),
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

            encoded_headers.append(("cookie", "; ".join(encoded_cookies)))

        encoded_headers: bytes = self._encoder.encode(encoded_headers)
        encoded_headers: List[bytes] = [
            encoded_headers[i : i + self._settings.max_frame_size]
            for i in range(0, len(encoded_headers), self._settings.max_frame_size)
        ]

        return encoded_headers[0]

    def close(self):
        for connection in self._connections:
            connection.close()
