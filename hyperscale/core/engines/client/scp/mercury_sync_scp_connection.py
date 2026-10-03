import asyncio
import pathlib
import time
from collections import defaultdict
from typing import Any, Literal
from urllib.parse import ParseResult, urlparse

from hyperscale.core.engines.client.shared.models import (
    URL as SFTPUrl,
    RequestType,
    URLMetadata,
)
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
    Directory,
    File,
    FileGlob,
)
from hyperscale.core.testing.models.file.file_attributes import FileAttributes
from hyperscale.core.engines.client.shared.protocols import (
    ProtocolMap,
)

from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.sftp.models import TransferResult
from hyperscale.core.engines.client.ssh.models import ConnectionOptions
from .models.scp import SCPResponse
from .models.scp.scp_response import SCPTimings
from .scp_command import SCPCommand
from .protocols import (
    SCPConnection,
    SCPHandler,
    ConnectionType
)


CommandType = Literal["COPY", "SEND", "RECEIVE"]
DataType = str | list[str] | File | FileGlob | Directory


class MercurySyncSCPConnection:

    def __init__(
        self,
        pool_size: int | None = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ):
        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._destination_connections: list[SCPConnection] = []
        self._source_connections: list[SCPConnection] = []

        self._hosts: dict[str, tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None

        self._url_cache: dict[str, SFTPUrl] = {}
        self._optimized: dict[str, URL | Auth | Data ] = {}
        self._connection_options: ConnectionOptions = None

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.SCP]

        self.address_family = address_family
        self.address_protocol = protocol
        self.disable_host_check = True
        
    async def copy(
        self,
        source_url: str | URL,
        destination_url: str | URL,
        source_path: str | pathlib.Path,
        destination_path: str | pathlib.Path,
        connection_options: ConnectionOptions | None = None,
        username: str | None = None,
        password: str | None = None,
        insecure: bool = False,
        enforce_path_as_directory: bool = False,
        preserve_file_attributes: bool = False,
        recurse: bool = False,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "COPY",
                        source_url,
                        destination_url,
                        source_path,
                        destination_path,
                        connection_options=connection_options,
                        data=None,
                        username=username,
                        password=password,
                        insecure=insecure,
                        must_be_dir=enforce_path_as_directory,
                        preserve=preserve_file_attributes,
                        recurse=recurse,
                    ),
                    timeout=timeout,
                )
            
            except asyncio.TimeoutError as err:

                if isinstance(source_url, str):
                    source_url_data = urlparse(source_url)
                else:
                    source_url_data = source_url.optimized.parsed

                if isinstance(destination_url, str):
                    dest_url_data = urlparse(destination_url)
                else:
                    dest_url_data = destination_url.optimized.parsed

                source_path = str(source_path) if isinstance(source_path, (pathlib.Path, pathlib.PurePath)) else source_path
                destination_path = str(destination_path) if isinstance(destination_path, (pathlib.Path, pathlib.PurePath)) else destination_path

                return SCPResponse(
                    source_url=URLMetadata(
                        host=source_url_data.hostname,
                        path=source_path,
                    ),
                    destination_url=URLMetadata(
                        host=dest_url_data.hostname,
                        path=destination_path,
                    ),
                    operation="COPY",
                    error=err,
                    timings={},
                )
    
    async def send(
        self,
        url: str | URL,
        path: str | pathlib.Path,
        data: DataType,
        attributes: FileAttributes | None = None,
        connection_options: ConnectionOptions | None = None,
        username: str | None = None,
        password: str | None = None,
        insecure: bool = False,
        enforce_path_as_directory: bool = False,
        preserve_file_attributes: bool = False,
        recurse: bool = False,
        timeout: int | float | None = None,
    ):
        
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "SEND",
                        url,
                        url,
                        path,
                        path,
                        attributes=attributes,
                        connection_options=connection_options,
                        data=data,
                        username=username,
                        password=password,
                        insecure=insecure,
                        must_be_dir=enforce_path_as_directory,
                        preserve=preserve_file_attributes,
                        recurse=recurse,
                    ),
                    timeout=timeout,
                )
            
            except asyncio.TimeoutError as err:

                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                receive_path = str(path) if isinstance(path, (pathlib.Path, pathlib.PurePath)) else path

                return SCPResponse(
                    source_url=URLMetadata(
                        host=url_data.hostname,
                        path=receive_path,
                    ),
                    destination_url=URLMetadata(
                        host=url_data.hostname,
                        path=receive_path,
                    ),
                    operation="SEND",
                    error=err,
                    timings={},
                )

    async def receive(
        self,
        url: str | URL,
        path: str | pathlib.Path,
        connection_options: ConnectionOptions | None = None,
        username: str | None = None,
        password: str | None = None,
        insecure: bool = False,
        enforce_path_as_directory: bool = False,
        preserve_file_attributes: bool = False,
        recurse: bool = False,
        timeout: int | float | None = None,
    ):
        
        async with self._semaphore:
            try:

                return await asyncio.wait_for(
                    self._execute(
                        "RECEIVE",
                        url,
                        url,
                        path,
                        path,
                        data=None,
                        connection_options=connection_options,
                        username=username,
                        password=password,
                        insecure=insecure,
                        must_be_dir=enforce_path_as_directory,
                        preserve=preserve_file_attributes,
                        recurse=recurse,
                    ),
                    timeout=timeout,
                )
            
            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                receive_path = str(path) if isinstance(path, (pathlib.Path, pathlib.PurePath)) else path

                return SCPResponse(
                    source_url=URLMetadata(
                        host=url_data.hostname,
                        path=receive_path,
                    ),
                    destination_url=URLMetadata(
                        host=url_data.hostname,
                        path=receive_path,
                    ),
                    operation="RECEIVE",
                    error=err,
                    timings={},
                )
                
    async def _optimize(
        self,
        optimized_param: URL | Data | File,
    ):
        if isinstance(optimized_param, URL):
            self._url_cache[optimized_param.optimized.hostname] = optimized_param
            self._optimized[optimized_param.call_name] = optimized_param

        else:
            self._optimized[optimized_param.call_name] = optimized_param
            
    async def _execute(
        self,
        command_type: CommandType,
        source_url: str | URL,
        destination_url: str | URL,
        local_path: str | pathlib.Path | pathlib.PurePath,
        dest_path: str | pathlib.Path| pathlib.PurePath,
        connection_options: ConnectionOptions | None = None,
        data: DataType | None = None,
        attributes: FileAttributes | None = None,
        username: str | None = None,
        password: str | None = None,
        insecure: bool = False,
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
    ) -> SCPResponse:
        timings: dict[
            SCPTimings,
            float | None,
        ] = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "initialization_start": None,
            "initialization_end": None,
            "transfer_start": None,
            "transfer_end": None,
            "request_end": None,
        }

        timings["request_start"] = time.monotonic()

        source: SCPConnection | None = None
        dest: SCPConnection | None = None
        source_handler: SCPHandler | None = None
        dest_handler: SCPHandler | None = None

        try:
            # A request holds its (source, destination) pair from here on,
            # so every exit -- including cancellation -- returns both.
            source = self._source_connections.pop()
            dest = self._destination_connections.pop()

            timings["connect_start"] = time.monotonic()

            (
                src_err,
                dest_err,
                source_url_parsed,
                destination_url_parsed,
            ) = await self._create_connections(
                source,
                dest,
                source_url,
                destination_url,
                options=connection_options,
                username=username,
                password=password,
                disable_host_check=insecure,
            )

            if src_err or dest_err:
                timings["connect_end"] = time.monotonic()

                if src_err:
                    source.reset()

                if dest_err:
                    dest.reset()

                self._source_connections.append(source)
                self._destination_connections.append(dest)

                return SCPResponse(
                    source_url=URLMetadata(
                        host=source_url_parsed.hostname,
                        path=str(local_path) if isinstance(local_path, (pathlib.Path, pathlib.PurePath)) else local_path,
                    ),
                    destination_url=URLMetadata(
                        host=destination_url_parsed.hostname,
                        path=str(dest_path) if isinstance(dest_path, (pathlib.Path, pathlib.PurePath)) else dest_path,
                    ),
                    operation=command_type,
                    error=src_err or dest_err,
                    timings=timings,
                )
            
            timings["connect_end"] = time.monotonic()
            timings["initialization_start"] = time.monotonic()


            if isinstance(local_path, (pathlib.PurePath, pathlib.Path)):
                local_path: bytes = str(local_path).encode()

            elif isinstance(local_path, str):
                local_path: bytes = local_path.encode()

            if isinstance(dest_path, (pathlib.PurePath, pathlib.Path)):
                dest_path: bytes = str(dest_path).encode()

            elif isinstance(dest_path, str):
                dest_path: bytes = dest_path.encode()

            (
                source_session,
                dest_session,
            ) = await asyncio.gather(
                source.create_session(local_path),
                dest.create_session(dest_path),
                return_exceptions=True,
            )

            if not isinstance(source_session, BaseException):
                source_handler, _ = source_session

            if not isinstance(dest_session, BaseException):
                dest_handler, _ = dest_session

            if isinstance(source_session, BaseException):
                raise source_session

            if isinstance(dest_session, BaseException):
                raise dest_session

            timings["initialization_end"] = time.monotonic()
            timings["transfer_start"] = time.monotonic()

            command = SCPCommand(
                source_handler,
                dest_handler,
                asyncio.get_running_loop(),
                recurse=recurse,
                preserve=preserve,
                must_be_dir=must_be_dir,
            )

            results: tuple[
                float,
                dict[bytes, TransferResult]
            ] = (0, {})

            match command_type:
                case "COPY":
                    results = await command.copy()

                case "RECEIVE":
                    results = await command.receive(dest_path)

                case "SEND":

                    if isinstance(data, Data):
                        encoded_data: bytes = data.optimized

                        transfer_data = [
                            (
                                dest_path,
                                encoded_data,
                                self._get_or_create_attributes(
                                    attributes,
                                    encoded_data,
                                ),
                            ),
                        ]

                    elif isinstance(data, Directory):
                        transfer_data = [

                        ]

                    elif isinstance(data, (FileGlob, Directory)):
                        transfer_data = data.optimized

                    elif isinstance(data, str):
                        encoded_data = data.encode()
                        transfer_data = [
                            (
                                dest_path,
                                encoded_data,
                                self._get_or_create_attributes(
                                    attributes,
                                    encoded_data,
                                ),
                            )
                        ]

                    else:
                        transfer_data = [
                            (
                                dest_path,
                                data,
                                self._get_or_create_attributes(
                                    attributes,
                                    data,
                                )
                            )
                        ]

                    results = await command.send(transfer_data)

            elapsed, transferred = results            

            timings["transfer_end"] = elapsed

            # Each request's SSH sessions end with it; servers cap the
            # sessions open on one connection.
            await asyncio.gather(
                source_handler.close(),
                dest_handler.close(),
            )

            self._source_connections.append(source)
            self._destination_connections.append(dest)

            timings["request_end"] = time.monotonic()

            return SCPResponse(
                source_url=URLMetadata(
                    host=source_url_parsed.hostname,
                    path=str(local_path) if isinstance(local_path, (pathlib.Path, pathlib.PurePath)) else local_path,
                ),
                destination_url=URLMetadata(
                    host=destination_url_parsed.hostname,
                    path=str(dest_path) if isinstance(dest_path, (pathlib.Path, pathlib.PurePath)) else dest_path,
                ),
                operation=command_type,
                transferred=transferred,
                timings=timings,
            )
            
        except (
            BaseException,
            Exception,
        ) as err:
            timings["request_end"] = time.monotonic()

            if source_handler:
                source_handler.writer.channel.abort()

            if dest_handler:
                dest_handler.writer.channel.abort()

            if source:
                source.reset()
                self._source_connections.append(source)

            if dest:
                dest.reset()
                self._destination_connections.append(dest)

            if isinstance(source_url, str):
                source_url_data = urlparse(source_url)
            else:
                source_url_data = source_url.optimized.parsed

            if isinstance(destination_url, str):
                dest_url_data = urlparse(destination_url)
            else:
                dest_url_data = destination_url.optimized.parsed

            return SCPResponse(
                source_url=URLMetadata(
                    host=source_url_data.hostname,
                    path=str(local_path) if isinstance(local_path, (pathlib.Path, pathlib.PurePath)) else local_path,
                ),
                destination_url=URLMetadata(
                    host=dest_url_data.hostname,
                    path=str(dest_path) if isinstance(dest_path, (pathlib.Path, pathlib.PurePath)) else dest_path,
                ),
                operation=command_type,
                error=err,
                timings=timings,
            )
        
    async def _create_connections(
        self,
        source: SCPConnection,
        dest: SCPConnection,
        source_url: str | URL,
        destination_url: str | URL,
        options: ConnectionOptions | None = None,
        username: str | None = None,
        password: str | None = None,
        disable_host_check: bool = False,
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
    ) -> tuple[
        Exception | None,
        Exception | None,
        SFTPUrl | None,
        SFTPUrl | None,
    ]:
        
        connection_options = self._connection_options.to_dict()
        if options:
            connection_options.update(options.to_dict())
        
        if username:
            connection_options["username"] = username

        if password:
            connection_options["password"] = password

        if disable_host_check:
            connection_options['known_hosts'] = None

        (
            (source_err, source_url_parsed),
            (dest_err, dest_url_parsed),
        ) = await asyncio.gather(
            self._connect(
                source,
                source_url,
                connection_type='SOURCE',
                must_be_dir=must_be_dir,
                preserve=preserve,
                recurse=recurse,
                **connection_options,
            ),
            self._connect(
                dest,
                destination_url,
                connection_type='DEST',
                must_be_dir=must_be_dir,
                preserve=preserve,
                recurse=recurse,
                **connection_options,
            ),
        )

        return (
            source_err,
            dest_err,
            source_url_parsed,
            dest_url_parsed,
        )

    async def _connect(
        self,
        scp_connection: SCPConnection,
        request_url: str | URL,
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        connection_type: ConnectionType = "SOURCE",
        **kwargs: dict[str, Any],

    ) -> tuple[
        Exception | None,
        SFTPUrl | None,
    ]:
        has_optimized_url = isinstance(request_url, URL)
        
        if has_optimized_url:
            parsed_url = request_url.optimized

        else:
            parsed_url = SFTPUrl(
                request_url,
                family=self.address_family,
                protocol=self.address_protocol,
            )

        url = self._url_cache.get(parsed_url.hostname)
        dns_lock = self._dns_lock[parsed_url.hostname]
        dns_waiter = self._dns_waiters[parsed_url.hostname]

        do_dns_lookup = url is None and has_optimized_url is False

        if do_dns_lookup and dns_lock.locked() is False:
            try:
                async with dns_lock:
                    url = parsed_url
                    await url.lookup_ssh()

                    self._url_cache[parsed_url.hostname] = url

            finally:
                # However the lookup ended, release its waiters; after a
                # failed or cancelled lookup the next request looks up
                # again with a fresh waiter.
                if dns_waiter.done() is False:
                    dns_waiter.set_result(None)

                if parsed_url.hostname not in self._url_cache:
                    del self._dns_waiters[parsed_url.hostname]

        elif do_dns_lookup:
            # Shielded: a waiter's cancellation must not cancel the
            # lookup future every other waiter shares.
            await asyncio.shield(dns_waiter)
            url = self._url_cache.get(parsed_url.hostname)

        elif has_optimized_url:
            url = request_url.optimized

        command = b'scp -f ' if connection_type == "SOURCE" else b'scp -t '

        connection_error: Exception | None = None

        if url.address is None:
            for address, ip_info in url:
                try:
                    await scp_connection.make_connection(
                        command,
                        ip_info,
                        must_be_dir=must_be_dir,
                        preserve=preserve,
                        recurse=recurse,
                        **kwargs,
                    )

                    url.address = address
                    url.socket_config = ip_info
                    connection_error = None
                    break

                except Exception as err:
                    # Close this attempt's socket before trying the next address.
                    connection_error = err
                    scp_connection.reset()

        else:
            try:
                await scp_connection.make_connection(
                    command,
                    url.socket_config,
                    must_be_dir=must_be_dir,
                    preserve=preserve,
                    recurse=recurse,
                    **kwargs,
                )


            except Exception as err:
                connection_error = err

        return (
            connection_error,
            parsed_url,
        )

    def _get_or_create_attributes(
        self,
        attributes: FileAttributes | None,
        encoded_data: bytes | None,
    ):
        if attributes is None:

            # Whole seconds since the epoch plus a nanosecond fraction.
            created, created_ns = divmod(time.time_ns(), 1_000_000_000)

            attributes = FileAttributes(
                type=TransferResult.to_file_type_int("FILE"),
                size=len(encoded_data) if encoded_data else 0,
                permissions=0o644,
                crtime=created,
                crtime_ns=created_ns,
                atime=created,
                atime_ns=created_ns,
                ctime=created,
                ctime_ns=created_ns,
                mtime=created,
                mtime_ns=created_ns,
                mime_type="application/octet-stream",
            )

        return attributes

    def close(self):
        for connection in [*self._source_connections, *self._destination_connections]:
            connection.close()
