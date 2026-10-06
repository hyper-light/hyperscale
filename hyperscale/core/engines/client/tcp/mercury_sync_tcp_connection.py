import asyncio
import ssl
import time
from collections import defaultdict
from typing import Dict, List, Literal, Optional, Tuple, Iterator
from urllib.parse import ParseResult, urlparse

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import (
    URL as TCPUrl,
)
from hyperscale.core.engines.client.shared.models import (
    RequestType,
    URLMetadata,
)
from hyperscale.core.engines.client.shared.protocols import ProtocolMap
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Params,
)

from .models.tcp import TCPResponse
from .protocols import TCPConnection


class MercurySyncTCPConnection:
    def __init__(
        self,
        pool_size: Optional[int] = None,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        timeouts: Timeouts | None = None,
        reset_connections: bool = False,
    ) -> None:
        self._concurrency = pool_size
        # Each engine gets its own Timeouts: a default argument would be one
        # instance shared by every engine built without timeouts.
        self.timeouts = timeouts if timeouts is not None else Timeouts()
        self.reset_connections = reset_connections

        self._cert_path = cert_path
        self._key_path = key_path

        self._tcp_ssl_context: Optional[ssl.SSLContext] = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[TCPConnection] = []

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._connections_count: Dict[str, List[asyncio.Transport]] = defaultdict(list)

        self._semaphore: asyncio.Semaphore = None

        self._url_cache: Dict[str, TCPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.TCP]
        self._optimized: Dict[str, URL | Params | Headers | Auth | Data | Cookies] = {}

        self.address_family = address_family
        self.address_protocol = protocol

    async def send(
        self,
        url: str | URL,
        data: str | bytes | BaseModel | Data,
        timeout: Optional[int | float] = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        "SEND",
                        data=data,
                    ),
                    timeout=timeout,
                )

            except Exception as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return TCPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                    ),
                    error=str(err),
                    timings={},
                )

    async def receive(
        self,
        url: str | URL,
        delimiter: Optional[str | bytes] = b"\n",
        response_size: Optional[int] = None,
        timeout: Optional[int | float] = None,
    ):
        async with self._semaphore:
            try:
                if isinstance(delimiter, str):
                    delimiter = delimiter.encode()

                return await asyncio.wait_for(
                    self._request(
                        url,
                        "RECEIVE",
                        response_size=response_size,
                        delimiter=delimiter,
                    ),
                    timeout=timeout,
                )

            except Exception as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return TCPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                    ),
                    error=str(err),
                    timings={},
                )

    async def bidirectional(
        self,
        url: str | URL,
        data: str | bytes | BaseModel | Data,
        delimiter: Optional[str | bytes] = b"\n",
        response_size: Optional[int] = None,
        timeout: Optional[int | float] = None,
    ):
        async with self._semaphore:
            try:
                if isinstance(delimiter, str):
                    delimiter = delimiter.encode()

                return await asyncio.wait_for(
                    self._request(
                        url,
                        "BIDIRECTIONAL",
                        data=data,
                        response_size=response_size,
                        delimiter=delimiter,
                    ),
                    timeout=timeout,
                )

            except Exception as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return TCPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                    ),
                    error=str(err),
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

    async def _optimize_url(
        self,
        url: URL,
    ):
        try:
            if url:
                (
                    _,
                    connection,
                    optimized_url,
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
        request_url: str | URL,
        method: Literal["BIDIRECTIONAL", "RECEIVE", "SEND"],
        data: str | bytes | BaseModel | Data,
        delimiter: Optional[str | bytes] = b"\n",
        response_size: Optional[int] = None,
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
            "request_start": time.monotonic(),
            "connect_start": None,
            "connect_end": None,
            "write_start": None,
            "write_end": None,
            "read_start": None,
            "read_end": None,
            "request_end": None,
        }


        connection: TCPConnection | None = None

        try:
            timings["connect_start"] = time.monotonic()

            (
                error,
                connection,
                url,
            ) = await asyncio.wait_for(
                self._connect_to_url_location(connection, request_url),
                timeout=self.timeouts.request_timeout,
            )

            if error or connection is None or connection.reader is None:
                timings["connect_end"] = time.monotonic()

                if connection:
                    connection.reset()
                    self._connections.append(connection)

                if error is None:
                    error = Exception('Err. - no connection')

                return TCPResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        error=str(error),
                        timings=timings,
                    )
            
            timings["connect_end"] = time.monotonic()

            response_data = b""

            match method:
                case "BIDIRECTIONAL":

                    raw_data = data
                    if isinstance(data, Data):
                        raw_data = data.optimized

                    else:
                        raw_data = self._encode_data(data)

                    timings["write_start"] = time.monotonic()
                    if isinstance(raw_data, (Iterator, list)):
                        for chunk in raw_data:
                            connection.writer.write(chunk)

                    else:
                        connection.writer.write(raw_data)

                    timings["write_end"] = time.monotonic()
                    timings["read_start"] = time.monotonic()

                    if response_size:
                        response_data = await asyncio.wait_for(
                            connection.reader.readexactly(
                                response_size
                            ),
                            timeout=self.timeouts.request_timeout,
                        )

                    else:
                        response_data = await asyncio.wait_for(
                            connection.reader.readuntil(
                                separator=delimiter
                            ),
                            timeout=self.timeouts.request_timeout,
                        )

                    timings["read_end"] = time.monotonic()

                case "SEND":
                    raw_data = data
                    if isinstance(data, Data):
                        raw_data = data.optimized

                    else:
                        raw_data = self._encode_data(data)

                    timings["write_start"] = time.monotonic()
                    if isinstance(raw_data, (Iterator, list)):
                        for chunk in raw_data:
                            connection.writer.write(chunk)

                    else:
                        connection.writer.write(raw_data)

                    timings["write_end"] = time.monotonic()

                case "RECEIVE":
                    timings["read_start"] = time.monotonic()

                    if response_size:
                        response_data = await connection.reader.readexactly(
                            response_size
                        )

                    else:
                        response_data = await connection.reader.readuntil(
                            separator=delimiter
                        )

                    timings["read_end"] = time.monotonic()

                case _:
                    timings["request_end"] = time.monotonic()

                    raise Exception(
                        "Err. - invalid TCP operation. Must be one of - BIDIRECTIONAL, SEND, or RECEIVE."
                    )
                
            timings["request_end"] = time.monotonic()
            self._connections.append(connection)

            return TCPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path,
                ),
                content=response_data,
                timings=timings,
            )

        except (
            BaseException,
            Exception,
        ) as err:
            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

            if connection:
                connection.reset()
                self._connections.append(connection)

            timings["request_end"] = time.monotonic()

            return TCPResponse(
                url=URLMetadata(
                    host=request_url.hostname,
                    path=request_url.path,
                ),
                error=str(err),
                timings=timings,
            )

    async def _connect_to_url_location(
        self,
        connection: TCPConnection | None,
        request_url: str | URL,
    ) -> Tuple[
        Optional[Exception],
        TCPConnection,
        TCPUrl,
    ]:
        if isinstance(request_url, URL):
            # Resolved when the workflow prepared it: never looked up here,
            # and never read from or added to the lookup cache.
            url = parsed_url = request_url.optimized

        else:
            parsed_url = TCPUrl(
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

        connection_error: Optional[Exception] = None
        connection = self._connections.pop()

        try:
            # Reuses the connection's transport for this host; otherwise
            # races a new one across the host's addresses.
            address, socket_config, new_transport = await connection.connect_to_any(
                parsed_url.target,
                url.hostname,
                url.ip_addresses,
                url.port,
                url.address_rotation,
                ssl=self._tcp_ssl_context if url.scheme in ['ssl', 'tls', 'https'] else None,
            )

            if new_transport:
                url.address = address
                url.socket_config = socket_config

        except asyncio.CancelledError as err:
            return (
                err,
                connection,
                parsed_url,
            )

        except Exception as err:
            connection_error = err

        try:
            return (
                connection_error,
                connection,
                parsed_url,
            )

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    def _encode_data(
        self,
        data: str | list | dict | BaseModel | bytes | Data,
    ):
        if isinstance(data, Data):
            return data.optimized

        elif isinstance(data, BaseModel):
            return orjson.dumps(data.model_dump())

        elif isinstance(data, (list, dict)):
            return orjson.dumps(data)

        elif isinstance(data, str):
            return data.encode()

        elif isinstance(data, (memoryview, bytearray)):
            return bytes(data)

        else:
            return data

    def close(self):
        for connection in self._connections:
            connection.close()
