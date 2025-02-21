import asyncio
import ssl
import time
from collections import defaultdict
from typing import Dict, List, Literal, Optional, Tuple, Iterator
from urllib.parse import ParseResult, urlparse

import orjson
from pydantic import BaseModel

from hyperscale.core.engines.client.shared.models import (
    URL as UDPUrl,
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

from .models.udp import UDPResponse
from .protocols import UDPConnection


class MercurySyncUDPConnection:
    def __init__(
        self,
        pool_size: Optional[int] = None,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._cert_path = cert_path
        self._key_path = key_path

        self._udp_ssl_context: Optional[ssl.SSLContext] = None

        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[UDPConnection] = []

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._connections_count: Dict[str, List[asyncio.Transport]] = defaultdict(list)

        self._semaphore: asyncio.Semaphore = None

        self._url_cache: Dict[str, UDPUrl] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.UDP]
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

                return UDPResponse(
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

                return UDPResponse(
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

                return UDPResponse(
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

        try:

            timings["connect_start"] = time.monotonic()

            (
                error,
                connection,
                url,
            ) = await asyncio.wait_for(
                self._connect_to_url_location(request_url),
                timeout=self.timeouts.connect_timeout,
            )

            if connection.reader is None:
                timings["connect_end"] = time.monotonic()
                self._connections.append(
                    UDPConnection(
                        reset_connections=self.reset_connections,
                    )
                )

                return UDPResponse(
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
                        response_data = await connection.reader.readexactly(
                            response_size
                        )

                    else:
                        response_data = await connection.reader.readuntil(
                            separator=delimiter
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
                        response_data = await asyncio.wait_for(
                            connection.reader.readexactly(
                                response_size
                            ),
                            timeout=self.timeouts.read_timeout
                        )

                    else:
                        response_data = await asyncio.wait_for(
                            connection.reader.readuntil(
                                separator=delimiter
                            ),
                            timeout=self.timeouts.read_timeout,
                        )
                    timings["read_end"] = time.monotonic()

                case _:
                    timings["request_end"] = time.monotonic()

                    raise Exception(
                        "Err. - invalid UDP operation. Must be one of - BIDIRECTIONAL, SEND, or RECEIVE."
                    )
  
            timings["request_end"] = time.monotonic()
            self._connections.append(connection)

            return UDPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path,
                ),
                content=response_data,
                timings=timings,
            )

        except Exception as err:
            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed

            elif isinstance(request_url, URL):
                request_url: ParseResult = urlparse(request_url.data)

            self._connections.append(
                UDPConnection(
                    reset_connections=self.reset_connections,
                )
            )

            return UDPResponse(
                url=URLMetadata(
                    host=request_url.hostname,
                    path=request_url.path,
                ),
                error=str(err),
                timings=timings,
            )

    async def _connect_to_url_location(
        self,
        request_url: str | URL,
        ssl_redirect_url=None,
    ) -> Tuple[
        Optional[Exception],
        UDPConnection,
        UDPUrl,
    ]:
        has_optimized_url = isinstance(request_url, URL)

        if has_optimized_url:
            parsed_url = request_url.optimized

        else:
            parsed_url = UDPUrl(
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

        connection_error: Optional[Exception] = None
        connection = self._connections.pop()

        if url.address is None:
            for address, ip_info in url:
                try:
                    await connection.make_connection(
                        url.address,
                        url.port,
                        url.socket_config,
                        tls=self._udp_ssl_context if "wss" in url.scheme else None,
                    )

                    url.address = address
                    url.socket_config = ip_info

                except Exception:
                    pass

        else:
            try:
                await connection.make_connection(
                    url.address,
                    url.port,
                    url.socket_config,
                    tls=self._udp_ssl_context if "wss" in url.scheme else None,
                )

            except Exception as err:
                connection_error = err

        return (
            connection_error,
            connection,
            parsed_url,
        )

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
