import asyncio
import binascii
import time
import uuid
from random import randrange
from typing import (
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
)
from urllib.parse import ParseResult, urlparse

from hyperscale.core.engines.client.http2 import MercurySyncHTTP2Connection
from hyperscale.core.engines.client.http2.pipe import HTTP2Pipe
from hyperscale.core.engines.client.http2.protocols import HTTP2Connection
from hyperscale.core.engines.client.shared.models import URL as GRPCUrl
from hyperscale.core.engines.client.shared.models import URLMetadata
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import (
    URL,
    Protobuf,
)

from .models.grpc import GRPCResponse
from .models.grpc import Protobuf as GRPCProtobuf

T = TypeVar("T")


class MercurySyncGRPCConnection(MercurySyncHTTP2Connection, Generic[T]):
    def __init__(
        self,
        pool_size: int = 10**3,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        super(MercurySyncGRPCConnection, self).__init__(
            pool_size=pool_size,
            timeouts=timeouts,
            reset_connections=reset_connections,
        )

        self.session_id = str(uuid.uuid4())

    async def send(
        self,
        url: str | URL,
        protobuf: GRPCProtobuf[T] | Protobuf[T],
        timeout: Optional[int | float] = None,
    ) -> GRPCResponse:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url,
                        protobuf,
                        timeout=timeout,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return GRPCResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    status=408,
                    status_message="Request timed out.",
                    timings={},
                )

    async def _optimize(
        self,
        optimized_param: URL | Protobuf[T],
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
            self._optimized[url.call_name] = url

        except Exception:
            pass

    async def _request(
        self,
        url: str | URL,
        protobuf: GRPCProtobuf[T] | Protobuf[T],
        timeout: Optional[int | float] = None,
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

        result, _, timings = await self._execute(
            url,
            protobuf,
            timeout=timeout,
            timings=timings,
        )

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str | URL,
        protobuf: GRPCProtobuf[T] | Protobuf[T],
        timeout: Optional[int | float] = None,
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

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (error, connection, pipe, url, upgrade_ssl) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url,
                    ssl_redirect_url=request_url if upgrade_ssl else None,
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                (
                    error,
                    connection,
                    pipe,
                    url,
                    _,
                ) = await asyncio.wait_for(
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
                    GRPCResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method="POST",
                        status=400,
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
                timeout=timeout,
            )

            connection = pipe.send_request_headers(
                encoded_headers,
                protobuf,
                connection,
            )

            encoded_data = self._encode_data(protobuf)

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

            (
                status,
                headers,
                body,
                error,
            ) = await asyncio.wait_for(
                pipe.receive_response(connection), timeout=self.timeouts.read_timeout
            )

            if error:
                raise error

            self._connections.append(connection)
            self._pipes.append(pipe)

            timings["read_end"] = time.monotonic()

            return (
                GRPCResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                    ),
                    method="POST",
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
                GRPCResponse(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                    ),
                    method="POST",
                    status=400,
                    status_message=str(request_exception),
                    timings=timings,
                ),
                False,
                timings,
            )

    async def _connect_to_url_location(
        self,
        request_url: str | URL,
        ssl_redirect_url: Optional[str] = None,
    ) -> Tuple[
        Exception,
        HTTP2Connection,
        HTTP2Pipe,
        GRPCUrl,
        bool,
    ]:
        has_optimized_url = isinstance(request_url, URL)

        if has_optimized_url:
            parsed_url = request_url.optimized

        elif ssl_redirect_url:
            parsed_url = GRPCUrl(ssl_redirect_url)

        else:
            parsed_url = GRPCUrl(request_url)

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

                    connection_error = err

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
                if "server_hostname is only meaningful with ssl" in str(
                    connection_error
                ):
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

    def _encode_data(
        self,
        data: GRPCProtobuf[T] | Protobuf[T],
    ) -> bytes:
        if isinstance(data, Protobuf):
            return data.optimized

        encoded_protobuf = str(
            binascii.b2a_hex(data.SerializeToString()),
            encoding="raw_unicode_escape",
        )
        encoded_message_length = (
            hex(int(len(encoded_protobuf) / 2)).lstrip("0x").zfill(8)
        )
        encoded_protobuf = f"00{encoded_message_length}{encoded_protobuf}"

        return binascii.a2b_hex(encoded_protobuf)

    def _encode_headers(
        self,
        url: GRPCUrl | URL,
        timeout: int | float = 60,
    ):
        if isinstance(url, URL):
            url = url.optimized

        encoded_headers = [
            (b":method", b"POST"),
            (b":authority", url.hostname.encode()),
            (b":scheme", url.scheme.encode()),
            (b":path", url.path.encode()),
            (b"Content-Type", b"application/grpc"),
            (b"Grpc-Timeout", f"{timeout}".encode()),
            (b"TE", b"trailers"),
        ]

        encoded_headers: bytes = self._encoder.encode(encoded_headers)
        encoded_headers: List[bytes] = [
            encoded_headers[i : i + self._settings.max_frame_size]
            for i in range(0, len(encoded_headers), self._settings.max_frame_size)
        ]

        return encoded_headers[0]

    def close(self):
        for connection in self._connections:
            connection.close()
