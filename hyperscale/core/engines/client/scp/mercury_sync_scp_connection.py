import asyncio
import pathlib
import time
from typing import Any, Literal
from collections import defaultdict
from hyperscale.core.engines.client.shared.models import (
    URL as SFTPUrl,
    RequestType,
)
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
)
from hyperscale.core.engines.client.shared.protocols import (
    ProtocolMap,
)

from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.ssh.protocol.ssh.connection import SSHClientConnectionOptions
from hyperscale.core.engines.client.sftp.models import TransferResult
from hyperscale.core.engines.client.sftp.protocols.file import File
from .models.scp import SCPOptions, SCPResponse, SCPTimings
from .scp_command import SCPCommand
from .protocols import (
    SCPConnection,
    SCPHandler,
    ConnectionType
)


CommandType = Literal["COPY", "SEND", "RECEIVE"]
DataType = str | list[str] | File | list[File]


class MercurySyncSCPConnction:

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
        self._connection_waiters: list[asyncio.Future] = []

        self._url_cache: dict[str, SFTPUrl] = {}
        self._optimized: dict[str, URL | Auth | Data ] = {}
        self._options: SSHClientConnectionOptions | None = None

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
        username: str | None = None,
        password: str | None = None,
        connection_options: SCPOptions | None = None,
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
                        data=None,
                        local_path=source_path,
                        dest_path=destination_path,
                        username=username,
                        password=password,
                        options=connection_options,
                        disable_host_check=disable_host_check,
                        must_be_dir=enforce_path_as_directory,
                        preserve=preserve_file_attributes,
                        recurse=recurse,
                    ),
                    timeout=timeout,
                )
            
            except asyncio.TimeoutError as err:

                if not isinstance(source_url, str):
                    source_url = source_url.data

                if not isinstance(destination_url, str):
                    destination_url = destination_url.data

                if isinstance(source_path, pathlib.Path):
                    source_path = str(source_path)

                if isinstance(destination_path, pathlib.Path):
                    destination_path = str(destination_path)

                return SCPResponse(
                    source_url=source_url,
                    source_path=source_path,
                    destination_url=destination_url,
                    destination_path=destination_path,
                    operation="COPY",
                    error=err,
                    timings={},
                )
    
    async def send(
        self,
        url: str | URL,
        path: str | pathlib.Path,
        data: DataType,
        username: str | None = None,
        password: str | None = None,
        connection_options: SCPOptions | None = None,
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
                        data=data,
                        local_path=path,
                        dest_path=path,
                        username=username,
                        password=password,
                        options=connection_options,
                        disable_host_check=disable_host_check,
                        must_be_dir=enforce_path_as_directory,
                        preserve=preserve_file_attributes,
                        recurse=recurse,
                    ),
                    timeout=timeout,
                )
            
            except asyncio.TimeoutError as err:

                if isinstance(url, URL):
                    url = url.data

                if isinstance(path, pathlib.Path):
                    path = str(path)

                return SCPResponse(
                    source_url=url,
                    source_path=path,
                    destination_url=url,
                    destination_path=path,
                    operation="SEND",
                    error=err,
                    timings={},
                )

    async def receive(
        self,
        url: str | URL,
        path: str | pathlib.Path,
        username: str | None = None,
        password: str | None = None,
        connection_options: SCPOptions | None = None,
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
                        data=None,
                        local_path=path,
                        dest_path=path,
                        username=username,
                        password=password,
                        options=connection_options,
                        disable_host_check=disable_host_check,
                        must_be_dir=enforce_path_as_directory,
                        preserve=preserve_file_attributes,
                        recurse=recurse,
                    ),
                    timeout=timeout,
                )
            
            except asyncio.TimeoutError as err:

                if isinstance(url, URL):
                    url = url.data

                if isinstance(path, pathlib.Path):
                    path = str(path)

                return SCPResponse(
                    source_url=url,
                    source_path=path,
                    destination_url=url,
                    destination_path=path,
                    operation="RECEIVE",
                    error=err,
                    timings={},
                )
            
    async def _execute(
        self,
        command_type: CommandType,
        source_url: str | URL,
        destination_url: str | URL,
        data: DataType,
        local_path: str | None = None,
        dest_path: str | None = None,
        username: str | None = None,
        password: str | None = None,
        options: SCPOptions | None = None,
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
                username=username,
                password=password,
                options=options,
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
                    source_url=source_url if isinstance(source_url, str) else source_url_parsed.full,
                    source_path=local_path,
                    destination_url=destination_url if isinstance(destination_url, str) else destination_url_parsed.full,
                    destination_path=dest_path,
                    operation=command_type,
                    error=src_err,
                    timings=timings,
                )
            
            elif dest_err:
                timings["connect_end"] = time.monotonic()

                self._source_connections.append(source)
                self._destination_connections.append(SCPConnection(dest.connection_type))

                return SCPResponse(
                    source_url=source_url if isinstance(source_url, str) else source_url_parsed.full,
                    source_path=local_path,
                    destination_url=destination_url if isinstance(destination_url, str) else destination_url_parsed.full,
                    destination_path=dest_path,
                    operation=command_type,
                    error=dest_err,
                    timings=timings,
                )
            
            timings["connect_end"] = time.monotonic()
            timings["initialization_start"] = time.monotonic()

            
            handlers = await asyncio.gather(*[
                source.create_session(
                    local_path.encode(),
                ),
                dest.create_session(
                    dest_path.encode(),
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

            filesystem: dict[str, TransferResult] = {}

            match command_type:
                case "COPY":
                    filesystem = await command.copy()

                case "RECEIVE":
                    filesystem = await command.receive(dest_path)

                case "SEND":
                    
                    if isinstance(data, list):
                        prepared_data =[
                            File(
                                local_path.encode(),
                                item.encode(),
                            )
                            if isinstance(item, str)
                            else item
                            for item in data
                        ]

                    elif isinstance(data, str):
                        prepared_data = File(
                            local_path.encode(),
                            data.encode(),
                        )

                    else:
                        prepared_data = data


                    filesystem = await command.send(prepared_data)

            timings["transfer_end"] = time.monotonic()
            self._source_connections.append(source)
            self._destination_connections.append(dest)

            timings["request_end"] = time.monotonic()

            return SCPResponse(
                source_url=source_url if isinstance(source_url, str) else source_url_parsed.full,
                source_path=local_path,
                destination_url=destination_url if isinstance(destination_url, str) else destination_url_parsed.full,
                destination_path=dest_path,
                operation=command_type,
                data=filesystem,
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

            return SCPResponse(
                source_url=source_url if isinstance(source_url, str) else source_url.data,
                source_path=local_path,
                destination_url=destination_url if isinstance(destination_url, str) else destination_url.data,
                destination_path=dest_path,
                operation=command_type,
                error=err,
                timings=timings,
            )
        
    async def _create_connections(
        self,
        source_url: str | URL,
        destination_url: str | URL,
        username: str | None = None,
        password: str | None = None,
        options: SCPOptions | None = None,
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
        
        connection_options: dict[str, Any] = {}
        if username:
            connection_options["username"] = username

        if password:
            connection_options["password"] = password


        if options:
            connection_options.update(options.model_dump(exclude_none=True))

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
            ssh_connection = self._source_connections.pop()
            command = b'scp -f '

        else:
            ssh_connection = self._destination_connections.pop()
            command = b'scp -t '

        connection_error: Exception | None = None

        if url.address is None:
            for address, ip_info in url:
                try:
                    await ssh_connection.make_connection(
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
                await ssh_connection.make_connection(
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
            ssh_connection,
            parsed_url,
            connection_type,
        )
