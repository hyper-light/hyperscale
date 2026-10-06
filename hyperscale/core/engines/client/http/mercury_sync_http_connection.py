from __future__ import annotations

import asyncio
import binascii
import base64
import mimetypes
import pathlib
import ssl
import secrets
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
from hyperscale.core.engines.client.shared.phase_timeout import PhaseTimeout, within_timeout
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    File,
    Headers,
    Params,
)
from hyperscale.core.engines.client.tracing import HTTPTrace, Span

from .models.http import (
    HTTPResponse,
)
from .protocols import HTTPConnection

# A file's content type by its name: guess_file_type from Python 3.13,
# guess_type before it.
_guess_file_type = getattr(mimetypes, "guess_file_type", mimetypes.guess_type)


class MercurySyncHTTPConnection:
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
        self._connections: List[HTTPConnection] = []
        # One per in-flight request; see _execute.
        self._phase_timeouts: List[PhaseTimeout] = []

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: List[asyncio.Future] = []

        self._url_cache: Dict[str, HTTPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.HTTP]
        self._optimized: Dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}
        self._loop: asyncio.AbstractEventLoop = None

        self.address_family = address_family
        self.address_protocol = protocol
        self.trace: HTTPTrace | None = None

        self._boundary = binascii.hexlify(secrets.token_bytes(16)).decode()
        self._boundary_break = f"--{self._boundary}".encode("latin-1")

    async def head(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='HEAD',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "HEAD",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'HEAD',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

                return HTTPResponse(
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
                    trace=span,
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
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='OPTIONS',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "OPTIONS",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'OPTIONS',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

                return HTTPResponse(
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
                    trace=span,
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
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='GET',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "GET",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'GET',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

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
                    trace=span,
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
        files: str | File | list[File | str] | None = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='POST',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "POST",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=data,
                        files=files,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'POST',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

                return HTTPResponse(
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
                    trace=span,
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
        files: str | File | list[File | str] | None = None,
        redirects: int = 3,
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='PUT',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "PUT",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=data,
                        files=files,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'PUT',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

                return HTTPResponse(
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
                    trace=span,
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
        files: str | File | list[File | str] | None = None,
        timeout: Optional[int | float] = None,
        redirects: int = 3,
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='PATCH',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "PATCH",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data=data,
                        files=files,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'PATCH',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

                return HTTPResponse(
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
                    trace=span,
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
        trace_request: bool = False,
    ):
        span: Span | None = None
        if trace_request and self.trace.enabled:
            span = await self.trace.on_request_start(
                url,
                method='DELETE',
                headers=headers,
            )

        if span and self.trace.enabled:
            span = await self.trace.on_request_queued_start(span)

        async with self._semaphore:
            try:
                if span and self.trace.enabled:
                    span = await self.trace.on_request_queued_end(span)

                return await within_timeout(
                    self._request(
                        url,
                        "DELETE",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        redirects=redirects,
                        span=span,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        'DELETE',
                        asyncio.TimeoutError('Request timed out.'),
                        status=408,
                        headers=headers,
                    )

                return HTTPResponse(
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
                    trace=span,
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
        (
            _,
            connection,
            url,
            _
        ) = await asyncio.wait_for(
            self._connect_to_url_location(
                None,
                optimized_url,
            ),
            timeout=self.timeouts.request_timeout,
        )

        # Plain-string requests for the same address reuse this lookup: the
        # resolved URL, under the key the connect path reads. One that never
        # resolved is left for the connect path to look up.
        if url.ip_addresses:
            self._url_cache[url.target] = url

        self._optimized[optimized_url.call_name] = url

        connection.reset()
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
        files: str | File | list[File | str] | None = None,
        redirects: int = 3,
        span: Span | None = None,
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

        (
            result, 
            redirect,
            timings,
            span,
        ) = await self._execute(
            url,
            method,
            cookies=cookies,
            headers=headers,
            auth=auth,
            params=params,
            data=data,
            files=files,
            timings=timings,
            span=span,
        )

        if redirect and (
            location := result.headers.get(b'location')
        ):
            # Each location resolves against the address it came from (RFC
            # 3986: absolute, host-relative and path-relative alike).
            location = urljoin(url.data if isinstance(url, URL) else url, location.decode())

            redirects_taken = 1

            for idx in range(redirects):

                if span and self.trace.enabled:
                    span = await self.trace.on_request_redirect(
                        span,
                        location,
                        idx + 1,
                        redirects,
                    )

                (
                    result,
                    redirect,
                    timings,
                    span,
                ) = await self._execute(
                    url,
                    method,
                    cookies=cookies,
                    headers=headers,
                    auth=auth,
                    params=params,
                    data=data,
                    files=files,
                    redirect_url=location,
                    timings=timings,
                    span=span,
                )

                if redirect is False:
                    break

                if (next_location := result.headers.get(b"location")) is None:
                    break

                location = urljoin(location, next_location.decode())

                redirects_taken += 1

            result.redirects = redirects_taken

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str | URL,
        method: str,
        cookies: List[HTTPCookie] | Cookies = None,
        headers: Dict[str, str] | Headers = None,
        auth: tuple[str, str] | Auth | None = None,
        params: Dict[str, HTTPEncodableValue] | Params = None,
        data: (
            str
            | bytes
            | Iterator
            | Dict[str, Any]
            | List[str]
            | BaseModel
            | Data
        ) = None,
        files: str | File | list[File | str] | None = None,
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
        span: Span | None = None,
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
        Span | None
    ]:
        if redirect_url:
            request_url = redirect_url

        connection: HTTPConnection | None = None

        # Bounds each phase below by request_timeout, as wait_for did, with a
        # timer reused across requests rather than a new one per phase.
        phase_timeout = self._phase_timeouts.pop() if self._phase_timeouts else PhaseTimeout()

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (
                error,
                connection,
                url,
                span,
            ) = await phase_timeout.run(
                self._connect_to_url_location(
                    connection,
                    request_url,
                    span=span,
                ),
                timeout=self.timeouts.request_timeout,
            )

            encoded_data: Optional[bytes | List[bytes]] = None
            content_type: Optional[str] = None

            if error or connection is None or connection.reader is None:

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        method,
                        error if error else Exception('Connection failed.'),
                        status=400,
                        headers=headers,
                    )

                timings["connect_end"] = time.monotonic()       

                if connection:
                    connection.reset()
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
                        status=400,
                        status_message="Connection failed.",
                        timings=timings,
                        trace=span,
                    ),
                    False,
                    timings,
                    span,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            encoded_data: Optional[bytes | List[bytes]] = None
            content_type: Optional[str] = None

            # Data that is falsy but encodes to bytes ({} or []) is a body like
            # any other.
            if data is not None:
                encoded_data, content_type = self._encode_data(data)

            if files:
                (
                    headers,
                    encoded_data,
                    content_type,
                    error,
                ) = await self._upload_files(
                    files,
                    encoded_data,
                    content_type,
                    headers,
                )

            if files and (error or encoded_data is None):
                timings["write_end"] = time.monotonic()

                if span and self.trace.enabled:
                    span = await self.trace.on_request_exception(
                        span,
                        url,
                        method,
                        error if error else Exception('Write failed.'),
                        status=400,
                        headers=headers,
                    )

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
                        status=500,
                        status_message=str(error) if error else "Write failed.",
                        timings=timings,
                        trace=span,
                    ),
                    False,
                    timings,
                    span,
                )

            encoded_headers = self._encode_headers(
                url,
                method,
                auth=auth,
                params=params,
                headers=headers,
                cookies=cookies,
                # With files the body is the multipart one: its length, not
                # a Data model's.
                data=None if files else data,
                encoded_data=encoded_data,
                content_type=content_type,
            )

            connection.write(encoded_headers)

            if span and self.trace.enabled:
                span = await self.trace.on_request_headers_sent(
                    span,
                    encoded_headers,
                )

            if isinstance(encoded_data, list):
                # The framed chunks, then the last chunk that ends the body.
                for chunk in encoded_data:
                    connection.write(chunk)

                    if span and self.trace.enabled:
                        span = await self.trace.on_request_chunk_sent(
                            span,
                            chunk,
                        )  

                connection.write(("0" + NEW_LINE * 2).encode())

            elif data or encoded_data:
                connection.write(encoded_data)

                if span and self.trace.enabled:
                    span = await self.trace.on_request_data_sent(span)

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            response_code = await phase_timeout.run(
                connection.reader.readline(),
                timeout=self.timeouts.request_timeout,
            )

            if span and self.trace.enabled:
                span = await self.trace.on_response_header_line_received(
                    span,
                    response_code,
                )
            
            status_string: List[bytes] = response_code.split()
            status = int(status_string[1])

            response_headers: Dict[bytes, bytes] = await phase_timeout.run(
                connection.reader.read_header_block(),
                timeout=self.timeouts.request_timeout,
            )

            if span and self.trace.enabled:
                span = await self.trace.on_response_headers_received(
                    span,
                    response_headers,
                )

            content_length = response_headers.get(b"content-length")
            transfer_encoding = response_headers.get(b"transfer-encoding")

            cookies: Union[HTTPCookies, None] = None
            cookies_data: Union[bytes, None] = response_headers.get(b"set-cookie")
            if cookies_data:
                cookies = HTTPCookies()
                cookies.update(cookies_data)
                

            # We require Content-Length or Transfer-Encoding headers to read a
            # request body, otherwise it's anyone's guess as to how big the body
            # is, and we ain't playing that game.

            body = b''

            if content_length:
                body = await phase_timeout.run(
                    connection.readexactly(int(content_length)),
                    timeout=self.timeouts.request_timeout,
                )

                if span and self.trace.enabled:
                    span = await self.trace.on_response_data_received(
                        span,
                        body,
                    )

            elif transfer_encoding:
                body = bytearray()
                all_chunks_read = False

                while True and not all_chunks_read:
                    chunk_size = int(
                        (
                            await phase_timeout.run(
                                connection.readline(),
                                timeout=self.timeouts.request_timeout,
                            )
                        ).rstrip(),
                        16,
                    )

                    if not chunk_size:
                        # read last CRLF
                        await phase_timeout.run(
                            connection.readline(),
                            timeout=self.timeouts.request_timeout,
                        )
                        break

                    chunk = await phase_timeout.run(
                        connection.readexactly(chunk_size + 2),
                        timeout=self.timeouts.request_timeout,
                    )

                    if span and self.trace.enabled:
                        span = await self.trace.on_response_chunk_received(
                            span,
                            chunk,
                        )
                    
                    body.extend(chunk[:-2])

                all_chunks_read = True

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
                        headers=response_headers,
                        timings=timings,
                        trace=span,
                    ),
                    True,
                    timings,
                    span,
                )

            timings["read_end"] = time.monotonic()
            self._connections.append(connection)

            if span and self.trace.enabled:
                span = await self.trace.on_request_end(
                    span,
                    url,
                    method,
                    status,
                    headers=response_headers,
                )

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
                    headers=response_headers,
                    content=body,
                    timings=timings,
                    trace=span,
                ),
                False,
                timings,
                span,
            )
        
        except (
            BaseException,
            Exception,
            socket.error
        ) as err:

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

            if span and self.trace.enabled:
                span = await self.trace.on_request_exception(
                    span,
                    url,
                    method,
                    str(err),
                    status=status,
                    headers=headers,
                )

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
                    status_message=str(err),
                    timings=timings,
                    trace=span,
                ),
                False,
                timings,
                span,
            )

        finally:
            self._phase_timeouts.append(phase_timeout)

    async def _connect_to_url_location(
        self,
        connection: HTTPConnection | None,
        request_url: str | URL,
        span: Span | None = None
    ) -> Tuple[
        Optional[Exception],
        HTTPConnection,
        HTTPUrl,
        Span,
    ]:
        if span and self.trace.enabled:
            span = await self.trace.on_connection_create_start(
                span,
                request_url,
            )

        if isinstance(request_url, URL):
            # Resolved when the workflow prepared it: never looked up here,
            # and never read from or added to the lookup cache.
            url = parsed_url = request_url.optimized
            do_dns_lookup = False

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
            do_dns_lookup = url is None

            if do_dns_lookup:
                if span and self.trace.enabled:
                    span = await self.trace.on_dns_cache_miss(span)

                dns_lock = self._dns_lock[cache_key]
                dns_waiter = self._dns_waiters[cache_key]

                if dns_lock.locked() is False:
                    if span and self.trace.enabled:
                        span = await self.trace.on_dns_resolve_host_start(span)

                    try:
                        async with dns_lock:
                            url = parsed_url
                            await url.lookup()

                            if span and self.trace.enabled:
                                span = await self.trace.on_dns_resolve_host_end(
                                    span,
                                    [address for address, _ in url],
                                    url.port,
                                )

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

                    if span and self.trace.enabled:
                        span = await self.trace.on_dns_cache_hit(
                            span,
                            [address for address, _ in url],
                            url.port,
                        )

        if span and self.trace.enabled and do_dns_lookup is False:
            span = await self.trace.on_dns_cache_hit(
                span,
                [address for address, _ in url],
                url.port,
            )

        connection = self._connections.pop()
        connection_error: Optional[Exception] = None

        try:
            # Reuses the connection's transport for this host; otherwise
            # races a new one across the host's addresses.
            address, socket_config, new_transport = await connection.connect_to_any(
                parsed_url.target,
                url.hostname,
                url.ip_addresses,
                url.port,
                url.address_rotation,
                ssl=self._client_ssl_context if url.is_ssl else None,
            )

            if new_transport:
                url.address = address
                url.socket_config = socket_config

            elif span and self.trace.enabled:
                span = await self.trace.on_connection_reuse(
                    span,
                    [address for address, _ in url],
                    url.port,
                )

        except asyncio.CancelledError as err:
            return (
                err,
                connection,
                parsed_url,
                span,
            )

        except Exception as err:
            connection_error = err

        if span and self.trace.enabled:
            span = await self.trace.on_connection_create_end(
                span,
                url.address,
                url.port,
            )

        try:
            return (
                connection_error,
                connection,
                parsed_url,
                span,
            )

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

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

        elif isinstance(data, (bytes, memoryview, bytearray)):
            encoded_data = bytes(data)

        return encoded_data, content_type

    def _encode_headers(
        self,
        url: URL | HTTPUrl,
        method: str,
        auth: tuple[str, str] | Auth | None = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        data: (
            str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel | Data | None
        ) = None,
        encoded_data: Optional[bytes | List[bytes]] = None,
        content_type: Optional[str] = None,
    ):
        if isinstance(url, URL):
            url = url.optimized

        url_path = url.path

        # The params follow any query the address has of its own: after "&"
        # then, else after "?" (RFC 3986 3.4).
        if params and isinstance(params, Params):
            query = params.optimized[1:]
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        elif params and len(params) > 0:
            query = urlencode(params)
            url_path = f"{url_path}&{query}" if "?" in url_path else f"{url_path}?{query}"

        port = url.port or (443 if url.scheme == "https" else 80)
        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        header_items = (
            f"{method} {url_path} HTTP/1.1{NEW_LINE}HOST: {hostname}{NEW_LINE}"
        )

        if auth and isinstance(auth, Auth):
            header_items += auth.optimized

        elif auth:
            header_items += self._serialize_auth(auth)

        if headers and isinstance(headers, Headers):
            header_items += headers.optimized
        elif headers:
            header_items += f"Keep-Alive: timeout=60, max=100000{NEW_LINE}User-Agent: hyperscale/client{NEW_LINE}"

            for key, value in headers.items():
                header_items += f"{key}: {value}{NEW_LINE}"

        else:
            header_items += f"Keep-Alive: timeout=60, max=100000{NEW_LINE}User-Agent: hyperscale/client{NEW_LINE}"

        if isinstance(encoded_data, list):
            # An iterator's body, framed chunk by chunk (RFC 9112 7.1): its
            # length is not known ahead, so it goes chunked.
            header_items += f"Transfer-Encoding: chunked{NEW_LINE}"

        else:
            size: int = 0

            if data and isinstance(data, Data):
                size = data.content_length

            elif encoded_data:
                size = len(encoded_data)

            header_items += f"Content-Length: {size}{NEW_LINE}"

        if content_type:
            header_items += f"Content-Type: {content_type}{NEW_LINE}"

        if cookies and isinstance(cookies, Cookies):
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
    
    def _serialize_auth(
        self,
        auth: tuple[str, str] | tuple[str],
    ):
        if len(auth) > 1:
            credentials_string = f"{auth[0]}:{auth[1]}"
            encoded_credentials = base64.b64encode(
                credentials_string.encode(),
            ).decode()

        else:

            encoded_credentials = base64.b64encode(
                auth[0].encode()
            ).decode()

        return f'Authorization: Basic {encoded_credentials}{NEW_LINE}'
    
    async def _upload_files(
        self,
        files: str | File | list[File | str],
        body: bytes | list[bytes] | None,
        body_content_type: str | None,
        headers: dict[str, str] | Headers,
    ):
        """
        The request's body as multipart/form-data (RFC 7578): a "data" part
        holding the request's own data, when it has some, then one "file" part
        per file -- its filename and content type, then its bytes -- and the
        closing delimiter. Files named by path are read in the loop's executor;
        File models were read when optimized. Returns the headers (unchanged),
        the body, its content type, and the error that stopped it, if any.
        """
        try:
            if isinstance(body, list):
                raise ValueError("Data from an iterator cannot be sent alongside files")

            file_list = files if isinstance(files, list) else [files]
            loop = asyncio.get_running_loop()

            # Every path read at once, off the event loop.
            loaded = await asyncio.gather(
                *[
                    loop.run_in_executor(None, self._load_file, file)
                    for file in file_list
                    if not isinstance(file, File)
                ]
            )

            delimiter = self._boundary_break
            buffer = bytearray()

            if body:
                buffer += delimiter + b'\r\nContent-Disposition: form-data; name="data"\r\n'
                if body_content_type:
                    buffer += f"Content-Type: {body_content_type}\r\n".encode("latin-1")

                buffer += b"\r\n" + body + b"\r\n"

            loaded_index = 0
            for file in file_list:
                if isinstance(file, File):
                    (_, file_data, attributes) = file.optimized
                    if file_data is None:
                        raise IsADirectoryError(f"Cannot upload a directory: {file.data['path']}")

                    filename = pathlib.Path(file.data["path"]).name
                    file_content_type = attributes.mime_type or "application/octet-stream"

                else:
                    filename, file_content_type, file_data = loaded[loaded_index]
                    loaded_index += 1

                # A quote, CR or LF in the filename is escaped as browsers
                # escape it (WHATWG multipart/form-data): %22, %0D, %0A.
                escaped_filename = filename.replace('"', "%22").replace("\r", "%0D").replace("\n", "%0A")
                buffer += delimiter + (
                    f'\r\nContent-Disposition: form-data; name="file"; filename="{escaped_filename}"'
                    f"\r\nContent-Type: {file_content_type}\r\n\r\n"
                ).encode()
                buffer += file_data + b"\r\n"

            buffer += delimiter + b"--\r\n"

            return (
                headers,
                bytes(buffer),
                f"multipart/form-data; boundary={self._boundary}",
                None,
            )

        except Exception as err:
            return (
                None,
                None,
                None,
                err,
            )

    def _load_file(
        self,
        path: str,
    ) -> tuple[str, str, bytes]:
        """A file's name, content type and bytes: run in the loop's executor."""
        filepath = pathlib.Path(path)
        file_content_type, _ = _guess_file_type(filepath)

        return filepath.name, file_content_type or "application/octet-stream", filepath.read_bytes()

    def close(self):
        for connection in self._connections:
            connection.close()

        for phase_timeout in self._phase_timeouts:
            phase_timeout.cancel()

        self._phase_timeouts.clear()
