import asyncio
import base64
import time
import uuid
from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
)
from urllib.parse import (
    ParseResult,
    urlencode,
    urlparse,
    urljoin,
)

import orjson

from hyperscale.core.engines.client.http2 import MercurySyncHTTP2Connection
from hyperscale.core.engines.client.http2.fast_hpack import ConnectionEncoder
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
from hyperscale.core.engines.client.shared.models.url import DEFAULT_PORTS
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
        timeouts: Timeouts | None = None,
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
            if url:
                (
                    _,
                    connection,
                    pipe,
                    optimized_url,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(None, url),
                    timeout=self.timeouts.request_timeout,
                )
                connection.reset()
                self._connections.append(connection)
                self._pipes.append(pipe)

            # Plain-string requests for the same address reuse this lookup:
            # the resolved URL, under the key the connect path reads. One
            # that never resolved is left for the connect path to look up.
            if optimized_url.ip_addresses:
                self._url_cache[optimized_url.target] = optimized_url

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
            # Each location resolves against the address it came from (RFC
            # 3986: absolute, host-relative and path-relative alike).
            location = urljoin(url.data if isinstance(url, URL) else url, location)

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
                    redirect_url=location,
                )

                if redirect is False:
                    break

                if (next_location := result.headers.get("location")) is None:
                    break

                location = urljoin(location, next_location)

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
        reading_response = False

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (error, connection, pipe, url) = await asyncio.wait_for(
                self._connect_to_url_location(
                    connection,
                    request_url,
                ),
                timeout=self.timeouts.request_timeout,
            )

            if error or connection is None or connection.stream.reader is None:
                timings["connect_end"] = time.monotonic()

                if connection:
                    connection.reset()
                    self._connections.append(connection)
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
                    pipe._encoder,
                    auth=auth,
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
                    timeout=self.timeouts.request_timeout,
                )

            else:
                encoded_headers = self._encode_headers(
                    url,
                    method,
                    pipe._encoder,
                    auth=auth,
                    cookies=cookies,
                    data=data,
                    headers=headers,
                    params=params,
                )

                # The query rides in the path and no body follows: the
                # HEADERS frame must end the stream, or the server waits.
                connection = pipe.send_request_headers(
                    encoded_headers,
                    None,
                    connection,
                )

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            reading_response = True
            (status, response_headers, body, error, trailers) = await asyncio.wait_for(
                pipe.receive_response(connection),
                timeout=self.timeouts.request_timeout,
            )
            reading_response = False
            connection.consecutive_read_timeouts = 0

            if error:
                # A failed read fails the request, a redirect's included -- its
                # status may have arrived before the error did -- and leaves
                # the connection in an unknown state: reset, with a new pipe.
                connection.reset()
                self._connections.append(connection)
                self._pipes.append(HTTP2Pipe(self._concurrency))

                timings["read_end"] = time.monotonic()

                return (
                    GraphQLHTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message=str(error),
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()

                self._connections.append(connection)
                self._pipes.append(pipe)

                return (
                    GraphQLHTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=status,
                        headers=response_headers,
                        trailers=trailers,
                        timings=timings,
                    ),
                    True,
                    timings,
                )

            cookies: HTTPCookies | None = None
            cookies_data: str | None = response_headers.get("set-cookie")
            if cookies_data:
                cookies = HTTPCookies()
                cookies.update(cookies_data.encode())

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
                    headers=response_headers,
                    trailers=trailers,
                    content=body,
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
                if (
                    reading_response
                    and isinstance(request_exception, asyncio.TimeoutError)
                    and connection.consecutive_read_timeouts == 0
                ):
                    # A slow response, not a dead connection: cancel only this
                    # stream and keep the connection. A second timeout in a row
                    # on it means the connection itself is dead.
                    pipe.cancel_stream(connection)
                    connection.consecutive_read_timeouts += 1
                    self._connections.append(connection)
                    self._pipes.append(pipe)

                else:
                    connection.reset()
                    self._connections.append(connection)
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
            # The body itself: the headers carry its content type.
            return data.optimized

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
        header_encoder: ConnectionEncoder,
        auth: tuple[str, str] | Auth | None = None,
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
            # The model's query parameter, URL-encoded when it was optimized:
            # appending the model itself raised TypeError.
            query = query_string.optimized[1:]
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        elif method == "GET":
            # The query document unaltered and URL-encoded, as GraphQL over
            # HTTP carries it in a GET -- after "&" when the address has a
            # query of its own, else after "?" (RFC 3986 3.4).
            query = urlencode({"query": query_string})
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        elif params:
            # The params follow any query the address has of its own.
            query = urlencode(params)
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        # :authority names the target as RFC 9113 8.3.1 does: the host, an
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

        else:
            # No headers given: the client's own user agent, after any
            # authorization above -- rebuilding the list dropped it.
            encoded_headers.append((b"user-agent", b"hyperscale/client"))

        if isinstance(data, Mutation) or (
            data and method == "POST"
        ):
            encoded_headers.extend(
                [
                    # The body is a JSON request: application/graphql-response+json
                    # names GraphQL over HTTP's response, not its request.
                    (b"content-type", b"application/json"),
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

            # A header field as the HPACK encoder takes one: bytes.
            encoded_headers.append((b"cookie", "; ".join(encoded_cookies).encode()))

        # The whole header block: the pipe frames it, in CONTINUATION frames
        # past the peer's largest frame.
        return header_encoder.encode(encoded_headers)
    
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
