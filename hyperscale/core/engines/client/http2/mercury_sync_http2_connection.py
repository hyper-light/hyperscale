import asyncio
import base64
import ssl
import time
import uuid
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

from hyperscale.core.engines.client.shared.models import URL as HTTPUrl
from hyperscale.core.engines.client.shared.models.url import DEFAULT_PORTS
from hyperscale.core.engines.client.shared.models import Cookies as HTTPCookies
from hyperscale.core.engines.client.shared.models import (
    HTTPCookie,
    HTTPEncodableValue,
    RequestType,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.protocols import (
    ProtocolMap,
)
from hyperscale.core.engines.client.shared.concurrency_limit import ConcurrencyLimit
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.phase_timeout import PhaseTimeout, within_timeout
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Params,
)

from .fast_hpack import ConnectionEncoder
from .models.http2 import (
    HTTP2Response,
)
from .pipe import HTTP2Pipe
from .protocols import HTTP2Connection
from .settings import Settings

A = TypeVar("A")
R = TypeVar("R")

# Each method's :method pseudo-header, built once: every request with the
# method sends the same tuple.
_METHOD_HEADERS: Dict[str, Tuple[bytes, bytes]] = {
    method: (b":method", method.encode())
    for method in ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
}


class MercurySyncHTTP2Connection:
    def __init__(
        self,
        pool_size: int = 128,
        timeouts: Timeouts | None = None,
        reset_connections: bool = False,
    ) -> None:
        self.session_id = str(uuid.uuid4())
        # Each engine gets its own Timeouts: a default argument would be one
        # instance shared by every engine built without timeouts.
        self.timeouts = timeouts if timeouts is not None else Timeouts()
        # Connecting and reading the response are each bounded by half of
        # request_timeout, so a request stuck in either fails while the run
        # still has time for the VU's next one; computed once.
        self._connect_timeout = self.timeouts.request_timeout / 2
        self._read_timeout = self.timeouts.request_timeout / 2

        self.closed = False
        self._concurrency = pool_size
        self._reset_connections = reset_connections

        # At most one request per pooled connection; set up by setup_client.
        self._semaphore: ConcurrencyLimit = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)

        self._connections: List[HTTP2Connection] = []
        # One per in-flight request; see _execute.
        self._phase_timeouts: List[PhaseTimeout] = []

        self._pipes: List[HTTP2Pipe] = []

        self._url_cache: Dict[str, HTTPUrl] = {}

        self._hosts: Dict[str, Tuple[str, int]] = {}

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        concurrency_limit = self._semaphore
        if not concurrency_limit.try_acquire():
            await concurrency_limit.acquire()

        try:
            return await within_timeout(
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

        finally:
            concurrency_limit.release()

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
        """
        The request, and each redirect it follows (up to ``redirects``), in
        this one coroutine: a coroutine level costs on every wake-up.
        """
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
            "request_start": time.monotonic(),
            "connect_start": None,
            "connect_end": None,
            "write_start": None,
            "write_end": None,
            "read_start": None,
            "read_end": None,
            "request_end": None,
        }

        request_url = url

        while True:
            connection: HTTP2Connection = None
            reading_response = False

            # Bounds each phase below by request_timeout, as wait_for did, with a
            # timer reused across requests rather than a new one per phase.
            phase_timeout = self._phase_timeouts.pop() if self._phase_timeouts else PhaseTimeout()
            request_timeout = self.timeouts.request_timeout

            try:
                if timings["connect_start"] is None:
                    timings["connect_start"] = time.monotonic()

                # A prepared URL whose pooled connection already serves it -- every
                # request after a connection's first: connecting is the stream-id
                # step reuse_transport takes, exactly as _connect_to_url_location
                # would take it, with nothing to await or bound.
                if (
                    isinstance(request_url, URL)
                    and (url := request_url.optimized) is not None
                    and self._connections[-1].reuse_transport(url.target, url.ip_addresses) is not None
                ):
                    connection = self._connections.pop()
                    pipe = self._pipes.pop()

                else:
                    with phase_timeout.within(self._connect_timeout):
                        (error, connection, pipe, url) = await self._connect_to_url_location(
                            connection,
                            request_url,
                        )

                    if error or connection is None or connection.stream.reader is None:
                        timings["connect_end"] = time.monotonic()

                        if connection:
                            connection.reset()
                            self._connections.append(connection)
                            self._pipes.append(HTTP2Pipe(self._concurrency))

                        timings["request_end"] = time.monotonic()

                        return HTTP2Response(
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
                        )

                # Writing starts the moment connecting ends: one clock reading.
                connect_end = timings["connect_end"] = time.monotonic()

                if timings["write_start"] is None:
                    timings["write_start"] = connect_end

                encoded_headers = self._encode_headers(
                    url,
                    method,
                    pipe._encoder,
                    auth=auth,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                )

                # The body as sent, encoded ahead of the headers: a request with
                # none -- no data, or data that encodes to nothing -- ends its
                # stream on HEADERS, and data that is falsy but encodes to bytes
                # ({} or []) is sent like any other.
                encoded_data = self._encode_data(data) if data is not None else None

                connection = pipe.send_request_headers(
                    encoded_headers,
                    encoded_data or None,
                    connection,
                )

                if encoded_data:
                    with phase_timeout.within(request_timeout):
                        connection = await pipe.submit_request_body(
                            encoded_data,
                            connection,
                        )

                # Reading starts the moment writing ends: one clock reading.
                write_end = timings["write_end"] = time.monotonic()

                if timings["read_start"] is None:
                    timings["read_start"] = write_end

                reading_response = True
                with phase_timeout.within(self._read_timeout):
                    (status, response_headers, body, error, trailers) = await pipe.receive_response(
                        connection,
                        head_request=method == "HEAD",
                    )

                reading_response = False
                connection.consecutive_read_timeouts = 0

                if error:
                    # A failed read fails the request and leaves the connection in
                    # an unknown state: reset, with a new pipe.
                    connection.reset()
                    self._connections.append(connection)
                    self._pipes.append(HTTP2Pipe(self._concurrency))

                    timings["read_end"] = timings["request_end"] = time.monotonic()

                    return HTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message=str(error),
                        timings=timings,
                    )

                response_cookies: Union[HTTPCookies, None] = None

                cookies_data: Union[str, None] = response_headers.get("set-cookie")
                if cookies_data:
                    response_cookies = HTTPCookies()
                    response_cookies.update(cookies_data.encode())

                if status >= 300 and status < 400:
                    timings["read_end"] = time.monotonic()

                    self._connections.append(connection)
                    self._pipes.append(pipe)

                    if redirects and (location := response_headers.get("location")):
                        # Each location resolves against the address it came
                        # from (RFC 3986: absolute, host-relative and
                        # path-relative alike).
                        redirects -= 1
                        request_url = urljoin(
                            request_url.data if isinstance(request_url, URL) else request_url,
                            location,
                        )
                        continue

                    timings["request_end"] = time.monotonic()

                    return HTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=status,
                        headers=response_headers,
                        trailers=trailers,
                        timings=timings,
                    )

                self._connections.append(connection)
                self._pipes.append(pipe)

                timings["read_end"] = timings["request_end"] = time.monotonic()

                return HTTP2Response(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                    ),
                    cookies=response_cookies,
                    method=method,
                    status=status,
                    headers=response_headers,
                    trailers=trailers,
                    content=body,
                    timings=timings,
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

                timings["read_end"] = timings["request_end"] = time.monotonic()

                return HTTP2Response(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                        params=request_url.params,
                        query=request_url.query,
                    ),
                    method=method,
                    status=400,
                    # The failure's own reason: a TimeoutError's message is empty.
                    status_message=(
                        "Request timed out."
                        if isinstance(request_exception, asyncio.TimeoutError)
                        else str(request_exception)
                    ),
                    timings=timings,
                )

            finally:
                self._phase_timeouts.append(phase_timeout)

    def _encode_data(
        self,
        data: str | bytes | BaseModel | bytes | Data,
    ):
        encoded_data: Optional[bytes] = None

        if isinstance(data, Data):
            encoded_data = data.optimized

        elif isinstance(data, Iterator) and not isinstance(data, list):
            # HTTP/2 has no chunked transfer coding: DATA frames carry the body
            # as it is (RFC 9113 8.2.2).
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

    def _encode_headers(
        self,
        url: HTTPUrl,
        method: str,
        header_encoder: ConnectionEncoder,
        auth: tuple[str, str] | Auth | None = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
    ):
        method_header = _METHOD_HEADERS.get(method) or (b":method", method.encode())

        # The address's own pseudo-headers, encoded on its first request and
        # the same tuples on every one after: nothing encoded per request, and
        # the HPACK encoder recognizes the list by identity.
        if (pseudo_headers := url.http2_pseudo_headers) is None:
            # :authority names the target as RFC 9113 8.3.1 does: the host, an
            # IPv6 address in brackets, and the port unless it is the
            # scheme's default.
            hostname = url.hostname
            scheme = url.scheme
            authority = f"[{hostname}]" if ":" in hostname else hostname
            if url.port != DEFAULT_PORTS.get(scheme):
                authority = f"{authority}:{url.port}"

            pseudo_headers = url.http2_pseudo_headers = (
                (b":authority", authority.encode()),
                (b":scheme", scheme.encode()),
                (b":path", url.path.encode()),
            )

        # Absent arguments are tested first, so a plain request makes no
        # isinstance call: the argument models are always truthy.
        if not params:
            encoded_headers: List[Tuple[bytes, bytes]] = [method_header, *pseudo_headers]

        else:
            # The params follow any query the address has of its own: after
            # "&" then, else after "?" (RFC 3986 3.4).
            url_path = url.path
            query = params.optimized[1:] if isinstance(params, Params) else urlencode(params)
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

            encoded_headers = [
                method_header,
                pseudo_headers[0],
                pseudo_headers[1],
                (b":path", url_path.encode()),
            ]

        if auth is not None:
            if isinstance(auth, Auth):
                encoded_headers.append(auth.optimized)

            else:
                encoded_headers.append(
                    self._encode_auth_headers(auth),
                )

        if headers:
            if isinstance(headers, Headers):
                encoded_headers.extend(headers.optimized)

            else:
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

        if cookies:
            if isinstance(cookies, Cookies):
                encoded_headers.append(cookies.optimized)

            else:
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

        # The whole header block: the pipe frames it, in CONTINUATION frames
        # past the peer's largest frame.
        return header_encoder.encode(encoded_headers)

    async def _connect_to_url_location(
        self,
        connection: HTTP2Connection | None,
        request_url: str | URL,
    ) -> Tuple[
        Optional[Exception],
        HTTP2Connection,
        HTTP2Pipe,
        HTTPUrl,
    ]:
        if isinstance(request_url, URL):
            # Resolved when the workflow prepared it: never looked up here,
            # and never read from or added to the lookup cache.
            url = parsed_url = request_url.optimized

        else:
            parsed_url = HTTPUrl(
                request_url,
                family=self.address_family,
                protocol=self.address_protocol,
            )

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
        pipe = self._pipes.pop()

        connection_error: Optional[Exception] = None

        try:
            was_connected = connection.connected

            # Reuses the connection's transport when it reaches one of the
            # host's addresses; otherwise races a new one across them.
            if (
                reused := connection.reuse_transport(
                    parsed_url.target,
                    url.ip_addresses,
                )
            ) is not None:
                address, socket_config = reused
                new_transport = False

            elif url.is_ssl is False:
                # HTTP/2 here is TLS only, as browsers run it: an http://
                # address has no HTTP/2 transport to open.
                return (
                    ConnectionError(f"HTTP/2 requires an https:// address, not {url.full}"),
                    connection,
                    pipe,
                    parsed_url,
                )

            else:
                address, socket_config, new_transport = await connection.connect_to_any(
                    parsed_url.target,
                    url.hostname,
                    url.ip_addresses,
                    url.port,
                    url.address_rotation,
                    ssl=self._client_ssl_context,
                )

            if new_transport:
                url.address = address
                url.socket_config = socket_config

                if was_connected:
                    # The pipe's HPACK and flow-control state belong to the
                    # transport just replaced.
                    pipe = HTTP2Pipe(self._concurrency)

        except asyncio.CancelledError as err:
            return (
                err,
                connection,
                pipe,
                parsed_url,
            )

        except Exception as err:
            connection_error = err

        try:
            return (
                connection_error,
                connection,
                pipe,
                parsed_url,
            )

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None
    
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

        for phase_timeout in self._phase_timeouts:
            phase_timeout.cancel()

        self._phase_timeouts.clear()
