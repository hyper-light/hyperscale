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
        
    async def copy(
        self,
        source_url: str | URL,
        destination_url: str | URL,
        source_path: str | pathlib.Path,
        destination_path: str | pathlib.Path,
        connection_options: ConnectionOptions | None = None,
        username: str | None = None,
        password: str | None = None,
        disable_host_check: bool = False,
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
                        disable_host_check=disable_host_check,
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
        disable_host_check: bool = False,
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
                        disable_host_check=disable_host_check,
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
        disable_host_check: bool = False,
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
                        disable_host_check=disable_host_check,
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
        disable_host_check: bool = False,
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
        dest: SCPConnection | None= None

        try:
            
            timings["connect_start"] = time.monotonic()

            connections = await self._create_connections(
                source_url,
                destination_url,
                connection_options=connection_options,
                username=username,
                password=password,
                disable_host_check=disable_host_check,
            )

            (
                src_err,
                dest_err,
                source,
                dest,
                source_url_parsed,
                destination_url_parsed,
            ) = connections

            if src_err:
                timings["connect_end"] = time.monotonic()

                self._source_connections.append(SCPConnection(source.connection_type))
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
                    error=src_err,
                    timings=timings,
                )
            
            elif dest_err:
                timings["connect_end"] = time.monotonic()

                self._source_connections.append(source)
                self._destination_connections.append(SCPConnection(dest.connection_type))

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
                    error=dest_err,
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
            
            handlers = await asyncio.gather(*[
                source.create_session(
                    local_path,
                ),
                dest.create_session(
                    dest_path,
                )
            ])

            handlers: dict[ConnectionType, SCPHandler] = {
                session_type: handler for handler, session_type in handlers
            }

            timings["initialization_end"] = time.monotonic()
            timings["transfer_start"] = time.monotonic()

            command = SCPCommand(
                handlers["SOURCE"],
                handlers["DEST"],
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
            
        except Exception as err:
            timings["request_end"] = time.monotonic()

            if source:
                self._source_connections.append(
                    SCPConnection("SOURCE")
                )

            if dest:
                self._destination_connections.append(
                    SCPConnection("DEST")
                )

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
        SCPConnection,
        SCPConnection,
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
        
        connections = await asyncio.gather(*[
            self._connect(
                source_url,
                connection_type='SOURCE',
                must_be_dir=must_be_dir,
                preserve=preserve,
                recurse=recurse,
                **connection_options,
            ),
            self._connect(
                destination_url,
                connection_type='DEST',
                must_be_dir=must_be_dir,
                preserve=preserve,
                recurse=recurse,
                **connection_options,
            ),
        ])

        source_err: Exception | None = None
        connected_source: SCPConnection | None = None
        connected_source_url: SFTPUrl | None = None
        
        dest_err: Exception | None = None
        connected_dest: SCPConnection | None = None
        connected_dest_url: SFTPUrl | None = None

        for connection_set in connections:
            (
                err,
                connection,
                url,
                connection_type,
            ) = connection_set

            match connection_type:
                case 'SOURCE':
                    source_err = err
                    connected_source = connection
                    connected_source_url = url


                case 'DEST':
                    dest_err = err
                    connected_dest = connection
                    connected_dest_url = url

        return (
            source_err,
            dest_err,
            connected_source,
            connected_dest,
            connected_source_url,
            connected_dest_url,

        )

    async def _connect(
        self,
        request_url: str | URL,
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        connection_type: ConnectionType = "SOURCE",
        **kwargs: dict[str, Any],

    ) -> tuple[
        Exception | None,
        SCPConnection,
        SFTPUrl | None,
        ConnectionType,
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
            await dns_lock.acquire()
            url = parsed_url
            await url.lookup_ssh()

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


        if connection_type == "SOURCE":
            scp_connection = self._source_connections.pop()
            command = b'scp -f '

        else:
            scp_connection = self._destination_connections.pop()
            command = b'scp -t '

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
                    break

                except Exception as err:
                    connection_error = err

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
            scp_connection,
            parsed_url,
            connection_type,
        )

    def _get_or_create_attributes(
        self,
        attributes: FileAttributes | None,
        encoded_data: bytes | None,
    ):
        if attributes is None:

            created = time.monotonic()
            created_ns = time.monotonic_ns()

            attributes = FileAttributes(
                type=TransferResult.to_file_type_int("FILE"),
                size=len(encoded_data) if encoded_data else 0,
                uid=1000,
                gid=1000,
                permissions=644,
                crtime=created,
                crtime_ns=created_ns,
                atime=created,
                atime_ns=created_ns,
                ctime=created,
                ctime_ns=created_ns,
                created=created,
                mtime_ns=created_ns,
                mime_type="application/octet-stream",
            )

        return attributes