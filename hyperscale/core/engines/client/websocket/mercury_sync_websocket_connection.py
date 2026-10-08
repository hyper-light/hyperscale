import asyncio
import base64
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
)
from urllib.parse import (
    urljoin,
    ParseResult,
    urlencode,
    urlparse,
)

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import URL as HTTPUrl
from hyperscale.core.engines.client.shared.models import (
    HTTPCookie,
    HTTPEncodableValue,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.protocols import NEW_LINE
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Params,
)

from .models.websocket import (
    WebsocketResponse,
    create_sec_websocket_key,
    pack_hostname,
    websocket_accept,
)
from .models.websocket.constants import (
    OPCODE_BINARY,
    OPCODE_CLOSE,
    OPCODE_TEXT,
    WEBSOCKETS_VERSION,
)
from .protocols import WebsocketConnection

# Handshake fields the engine writes itself, and handshake options that
# shape the request without being sent.
HANDSHAKE_HEADERS = frozenset(
    (
        "host",
        "upgrade",
        "connection",
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-protocol",
        "origin",
        "suppress_origin",
        "subprotocols",
    )
)


class MercurySyncWebsocketConnection:
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

        self._client_ssl_context: Optional[None] = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[WebsocketConnection] = []

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._connections_count: Dict[str, List[asyncio.Transport]] = defaultdict(list)
        self._locks: Dict[asyncio.Transport, asyncio.Lock] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: List[asyncio.Future] = []

        self._url_cache: Dict[str, HTTPUrl] = {}
        self._optimized: Dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}

    async def receive(
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

                return WebsocketResponse(
                    URLMetadata(
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

    async def send(
        self,
        url: str | URL,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[str | Dict[str, Any] | List[Any] | BaseModel | Data] = None,
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

                return WebsocketResponse(
                    URLMetadata(
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
                    _,
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
        data: Optional[str | Dict[str, Any] | List[Any] | BaseModel | Data] = None,
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
            params=params,
            cookies=cookies,
            headers=headers,
            data=data,
            timings=timings,
        )

        if redirect and (
            location := result.headers.get(b'location')
        ):
            # Each location resolves against the address it came from (RFC
            # 3986: absolute, host-relative and path-relative alike).
            location = urljoin(url.data if isinstance(url, URL) else url, location.decode())

            for _ in range(redirects):
                result, redirect, timings = await self._execute(
                    url,
                    method,
                    auth=auth,
                    params=params,
                    cookies=cookies,
                    headers=headers,
                    data=data,
                    redirect_url=location,
                    timings=timings,
                )

                if redirect is False:
                    break

                if (next_location := result.headers.get(b"location")) is None:
                    break

                location = urljoin(location, next_location.decode())

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str | URL,
        method: Literal["GET", "POST"],
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
        data: Optional[str | bytes | Dict[str, Any] | List[Any] | BaseModel | Data] = None,
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
    ) -> Tuple[WebsocketResponse, bool, Dict[
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
        ]]:
        """
        One exchange on a WebSocket: send ``data`` as a message (when given)
        and read the next message back. A new transport first completes the
        opening handshake (RFC 6455, 4.1); a reused one is already open.
        """
        if redirect_url:
            request_url = redirect_url

        connection: WebsocketConnection | None = None

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (
                error,
                connection,
                url,
                new_transport,
            ) = await asyncio.wait_for(
                self._connect_to_url_location(
                    connection,
                    request_url,
                ),
                timeout=self.timeouts.request_timeout,
            )

            if error or connection is None or connection.reader is None:
                timings["connect_end"] = time.monotonic()

                if connection:
                    connection.reset()
                    self._connections.append(connection)

                return (
                    WebsocketResponse(
                        URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message=str(error) if error else None,
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            response_headers: Dict[bytes, bytes] | None = None

            if new_transport:
                key = create_sec_websocket_key()
                connection.write(
                    self._encode_handshake(
                        url,
                        key,
                        auth=auth,
                        cookies=cookies,
                        headers=headers,
                        params=params,
                    )
                )

                status_line = await asyncio.wait_for(
                    connection.reader.readline(),
                    timeout=self.timeouts.request_timeout,
                )
                status = int(status_line.split()[1])

                response_headers = {}
                async for header_name, header_value, _ in connection.reader.iter_headers():
                    response_headers[header_name] = header_value

                timings["connect_end"] = time.monotonic()

                if status != 101 or response_headers.get(b"sec-websocket-accept", b"").strip() != websocket_accept(key):
                    # Not a WebSocket: a redirect (_request follows it on a
                    # fresh transport), a refusal, or an answer to another
                    # key. This transport never opened.
                    connection.reset()
                    self._connections.append(connection)

                    return (
                        WebsocketResponse(
                            URLMetadata(
                                host=url.hostname,
                                path=url.path,
                            ),
                            headers=response_headers,
                            method=method,
                            status=status if status != 101 else 400,
                            status_message=None if status != 101 else "Sec-WebSocket-Accept does not match the key sent",
                            timings=timings,
                        ),
                        300 <= status < 400,
                        timings,
                    )

            else:
                timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            if data is not None:
                connection.send_message(*self._encode_message(data))

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            opcode, content, close_code = await asyncio.wait_for(
                connection.read_message(),
                timeout=self.timeouts.request_timeout,
            )

            timings["read_end"] = time.monotonic()

            if opcode == OPCODE_CLOSE:
                # The server closed this WebSocket, or broke the protocol and
                # this side closed it: its transport is done either way.
                connection.reset()
                self._connections.append(connection)

                return (
                    WebsocketResponse(
                        URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message=f"WebSocket closed ({close_code}): {content.decode(errors='replace')}",
                        headers=response_headers,
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            self._connections.append(connection)

            return (
                WebsocketResponse(
                    URLMetadata(
                        host=url.hostname,
                        path=url.path,
                    ),
                    method=method,
                    status=101,
                    headers=response_headers,
                    content=content,
                    timings=timings,
                ),
                False,
                timings,
            )

        except (
            BaseException,
            Exception,
        ) as request_exception:
            timings["read_end"] = time.monotonic()

            if connection:
                connection.reset()
                self._connections.append(connection)

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

            return (
                WebsocketResponse(
                    URLMetadata(
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

    async def _connect_to_url_location(
        self,
        connection: WebsocketConnection | None,
        request_url: str | URL,
    ) -> Tuple[
        Optional[Exception],
        WebsocketConnection,
        HTTPUrl,
        bool,
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
        connection_error: Optional[Exception] = None
        new_transport = False

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

        except asyncio.CancelledError as err:
            return (
                err,
                connection,
                parsed_url,
                False,
            )

        except Exception as err:
            connection_error = err

        try:
            return (
                connection_error,
                connection,
                parsed_url,
                new_transport,
            )

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    def _encode_message(
        self,
        data: str | bytes | Dict[str, Any] | List[Any] | BaseModel | Data | Iterator,
    ) -> Tuple[int, bytes | List[bytes]]:
        """
        A message's opcode and payload: text for strings and JSON, binary for
        bytes, and for an iterator its chunks, sent as one fragmented message.
        """
        if isinstance(data, Data):
            return (OPCODE_TEXT if data.content_type else OPCODE_BINARY), data.optimized

        if isinstance(data, str):
            return OPCODE_TEXT, data.encode()

        if isinstance(data, (bytes, bytearray, memoryview)):
            return OPCODE_BINARY, bytes(data)

        if isinstance(data, BaseModel):
            return OPCODE_TEXT, orjson.dumps(data.model_dump())

        if isinstance(data, (dict, list)):
            return OPCODE_TEXT, orjson.dumps(data)

        chunks = list(data)
        opcode = OPCODE_TEXT if chunks and isinstance(chunks[0], str) else OPCODE_BINARY
        return opcode, [chunk.encode() if isinstance(chunk, str) else bytes(chunk) for chunk in chunks]

    def _encode_handshake(
        self,
        url: HTTPUrl,
        key: str,
        auth: Optional[Tuple[str, str] | Auth] = None,
        cookies: Optional[List[HTTPCookie] | Cookies] = None,
        headers: Optional[Dict[str, str] | Headers] = None,
        params: Optional[Dict[str, HTTPEncodableValue] | Params] = None,
    ) -> bytes:
        """
        The opening handshake request (RFC 6455, 4.1): a GET carrying the
        upgrade, ``key`` and version, then the caller's own headers. The
        options suppress_origin and subprotocols shape it; they are not sent.
        """
        url_path = url.path

        if isinstance(params, Params):
            url_path += params.optimized

        elif params:
            url_path += f"?{urlencode(params)}"

        caller_headers = headers.data if isinstance(headers, Headers) else (headers or {})
        options = {name.lower(): value for name, value in caller_headers.items()}

        hostport = pack_hostname(url.hostname)
        if url.port not in (80, 443):
            hostport = f"{hostport}:{url.port}"

        encoded_headers = [
            f"GET {url_path} HTTP/1.1",
            f"Host: {options.get('host', hostport)}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}",
        ]

        if not options.get("suppress_origin"):
            scheme = "https" if url.is_ssl else "http"
            encoded_headers.append(f"Origin: {options.get('origin') or f'{scheme}://{hostport}'}")

        if subprotocols := options.get("subprotocols"):
            encoded_headers.append(f"Sec-WebSocket-Protocol: {','.join(subprotocols)}")

        if isinstance(auth, Auth):
            encoded_headers.append(auth.optimized)

        elif auth is not None:
            encoded_headers.append(self._serialize_auth(auth))

        if isinstance(cookies, Cookies):
            encoded_headers.append(cookies.optimized)

        elif cookies:
            encoded_cookies = "; ".join(
                cookie_data[0] if len(cookie_data) == 1 else f"{cookie_data[0]}={cookie_data[1]}"
                for cookie_data in cookies
            )
            encoded_headers.append(f"cookie: {encoded_cookies}")

        encoded_headers.extend(
            f"{name}: {value}"
            for name, value in caller_headers.items()
            if name.lower() not in HANDSHAKE_HEADERS
        )

        encoded_headers.extend(("", ""))

        return NEW_LINE.join(encoded_headers).encode()

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

        return f'Authorization: Basic {encoded_credentials}'

    def close(self):
        for connection in self._connections:
            connection.close()
