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
from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnectionOptions, SSHClientConnection
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPGlob, LocalFS
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPConnectionLost
from .models.scp import SCPOptions, SCPResponse, FileResult
from .scp_command import SCPCommand
from .protocols import (
    SCPConnection,
    SCPHandler,
    scp_error,
    ConnectionType
)
from .file import File


CommandType = Literal["COPY", "SEND", "RECEIVE"]
DataType = str | list[str] | File | list[File]


class MercurySyncSCPConnction:
    """SCP handler for remote-to-remote copies"""

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
        self._local_fs = LocalFS()

        
    async def copy(self) -> None:
        """Start SCP remote-to-remote transfer"""

        cancelled = False

        try:
            await self._copy_files()
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError) as exc:
            self._handle_error(exc)
        finally:
            if self._source:
                await self._source.close(cancelled)

            if self._sink:
                await self._sink.close(cancelled)
    
    async def send(self, srcpath: str | pathlib.PurePath) -> None:
        """Start SCP transfer"""

        cancelled = False

        try:

            if isinstance(srcpath, str):
                srcpath = srcpath.encode('utf-8')

            elif isinstance(srcpath, pathlib.PurePath):
                srcpath = str(srcpath).encode('utf-8')

            exc = await self._sink.await_response()

            if exc:
                raise exc
            
            await asyncio.gather(*[
                self._send_files(
                    name.filename,
                    b'',
                    name.attrs,
                ) for name in await SFTPGlob(self._fs).match(srcpath)
            ])

        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError) as exc:
            self._handle_error(exc)
        finally:
            if self._sink:
                await self._sink.close(cancelled)

    async def receive(self, dstpath: str | pathlib.PurePath) -> None:
        """Start SCP file receive"""

        cancelled = False
        try:
            if isinstance(dstpath, pathlib.PurePath):
                dstpath = str(dstpath)

            if isinstance(dstpath, str):
                dstpath = dstpath.encode('utf-8')

            if self._must_be_dir and not await self._fs.isdir(dstpath):
                self._handle_error(scp_error(SFTPFailure, 'Not a directory',
                                             dstpath))
            else:
                await self._recv_files(b'', dstpath)
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError, ValueError) as exc:
            self._handle_error(exc)
        finally:
            await self._source.close(cancelled)
    
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
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "initialization_start",
                "initialization_end",
                "transfer_start",
                "transfer_end",
                "request_end",
            ],
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

        source_connection: SCPConnection | None = None
        destination_connection: SCPConnection | None= None

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
                source_url,
                destination_url,
            ) = connections

            if src_err:
                timings["connect_end"] = time.monotonic()

                self._source_connections.append(SCPConnection(source.connection_type))
                self._destination_connections.append(dest)

                return SCPResponse(

                )
            
            elif dest_err:
                timings["connect_end"] = time.monotonic()

                self._source_connections.append(source)
                self._destination_connections.append(SCPConnection(dest.connection_type))

                return SCPResponse(

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
                self._local_fs,
                recurse=recurse,
                preserve=preserve,
                must_be_dir=must_be_dir,
            )

            filesystem: dict[str, FileResult] = {}

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

            

        except Exception as err:
            timings["request_end"] = time.monotonic()

            if source_connection:
                self._source_connections.append(
                    SCPConnection("SOURCE")
                )

            if destination_connection:
                self._destination_connections.append(
                    SCPConnection("DEST")
                )

            return SCPResponse()

        
    def _handle_error(self, exc: Exception) -> None:
        """Handle an SCP error"""

        if isinstance(exc, BrokenPipeError):
            exc = scp_error(SFTPConnectionLost, 'Connection lost',
                             fatal=True, suppress_send=True)

        raise exc
        
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
