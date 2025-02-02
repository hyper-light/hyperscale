import asyncio
import time
import uuid
from typing import (
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
    urljoin,
)

import orjson

from hyperscale.core.engines.client.http import MercurySyncHTTPConnection
from hyperscale.core.engines.client.http.protocols import HTTPConnection
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
from hyperscale.core.engines.client.shared.protocols import NEW_LINE
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

from .models.graphql import (
    GraphQLResponse,
)


def mock_fn():
    return None


try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source = None
    parse = mock_fn
    print_ast = mock_fn


class MercurySyncGraphQLConnection(MercurySyncHTTPConnection):
    def __init__(
        self,
        pool_size: int = 10**3,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        super(MercurySyncGraphQLConnection, self).__init__(
            pool_size=pool_size,
            timeouts=timeouts,
            reset_connections=reset_connections,
        )

        self.session_id = str(uuid.uuid4())

    async def query(
        self,
        url: str | URL,
        query: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ) -> GraphQLResponse:
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

                return GraphQLResponse(
                    metadata=Metadata(),
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
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
    ) -> GraphQLResponse:
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

                return GraphQLResponse(
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
        optimized_param: URL | Params | Headers | Cookies | Auth | Query | Mutation,
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
                    url,
                    upgrade_ssl,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )

                self._connections.append(connection)

            if upgrade_ssl:
                url.data = url.data.replace("http://", "https://")

                await url.optimize()

                (
                    _,
                    connection,
                    url,
                    _,
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(url),
                    timeout=self.timeouts.connect_timeout,
                )

                self._connections.append(connection)

            self._url_cache[url.optimized.hostname] = url

        except Exception:
            pass

    async def _request(
        self,
        url: str | URL,
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        auth: Optional[Tuple[str, str] | Auth] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
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
            cookies=cookies,
            auth=auth,
            params=params,
            headers=headers,
            data=data,
            timings=timings,
        )

        if redirect and (
            location := result.headers.get(b'location')
        ):
            location = location.decode()
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
                    params=params,
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
        request_url: str | URL,
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        auth: Optional[Tuple[str, str] | Auth] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
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
    ) -> Tuple[
        GraphQLResponse,
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
                    request_url, ssl_redirect_url=request_url if upgrade_ssl else None
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                (error, connection, url, _) = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url, ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            if connection.reader is None or error:
                timings["connect_end"] = time.monotonic()
                self._connections.append(
                    HTTPConnection(
                        reset_connections=self.reset_connections,
                    )
                )

                return (
                    GraphQLResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
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

            if method == "GET":
                encoded_headers = self._encode_headers(
                    url,
                    method,
                    data,
                    headers=headers,
                    cookies=cookies,
                    params=params,
                )

                connection.write(encoded_headers)

            else:
                encoded_data = self._encode_data(data)

                encoded_headers = self._encode_headers(
                    url,
                    method,
                    data,
                    headers=headers,
                    params=params,
                    cookies=cookies,
                    data=data,
                    encoded_data=encoded_data,
                )

                connection.write(encoded_headers)
                connection.write(encoded_data)

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            response_code = await asyncio.wait_for(
                connection.reader.readline(), timeout=self.timeouts.read_timeout
            )

            headers: Dict[bytes, bytes] = await asyncio.wait_for(
                connection.read_headers(), timeout=self.timeouts.read_timeout
            )

            status_string: List[bytes] = response_code.split()
            status = int(status_string[1])

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()
                self._connections.append(connection)

                return (
                    GraphQLResponse(
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

            timings["read_end"] = time.monotonic()

            return (
                GraphQLResponse(
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
                HTTPConnection(reset_connections=self.reset_connections)
            )

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

            timings["read_end"] = time.monotonic()

            return (
                GraphQLResponse(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
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
            Dict[Literal["query"], str]
            | Dict[
                Literal[
                    "query",
                    "operation_name",
                    "variables",
                ],
                str,
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

        return orjson.dumps(query)

    def _encode_headers(
        self,
        url: HTTPUrl | URL,
        method: Literal["GET", "POST"],
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[
            Dict[Literal["query"], str]
            | Dict[
                Literal[
                    "query",
                    "operation_name",
                    "variables",
                ],
                str,
            ]
            | Mutation
        ] = None,
        encoded_data: Optional[bytes | List[bytes]] = None,
    ):
        if isinstance(url, URL):
            url = url.optimized

        url_path = url.path
        query_string: str | Query = data.get("query")

        if method == "GET" and isinstance(query_string, Query):
            url_path += query_string

        elif method == "GET":
            query_string = data.get("query")
            query_string = "".join(query_string.replace("query", "").split())

            url_path += f"?query={{{query_string}}}"

        elif params:
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

        if isinstance(data, Mutation):
            size = data.content_length

        elif encoded_data and isinstance(encoded_data, Iterator):
            size = sum([len(chunk) for chunk in encoded_data])

        elif encoded_data:
            size = len(encoded_data)

        header_items += f"Content-Length: {size}{NEW_LINE}"

        if method == "POST":
            header_items += f"Content-Type: application/graphql-response+json{NEW_LINE}"

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
