import asyncio
import time
from collections import defaultdict
from typing import Any, Literal
from hyperscale.core.engines.client.shared.models import (
    URL as SFTPUrl,
    RequestType,
)
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.protocols import (
    ProtocolMap,
)

from .models import (
    CommandType,
    SFTPConnectionOptions,
    SFTPOptions,
    TransferResult,
)
from .protocols import SFTPConnection
from .protocols.sftp import MIN_SFTP_VERSION
from .sftp_command import SFTPCommand



class MercurySyncSFTPConnction:

    def __init__(
        self,
        pool_size: int | None = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
        connection_options: SFTPConnectionOptions | None = None,
    ):
        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections
        self._loop = asyncio.get_event_loop()

        self._connection_options = connection_options

        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: list[SFTPConnection] = []

        self._hosts: dict[str, tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: list[asyncio.Future] = []

        self._url_cache: dict[str, SFTPUrl] = {}
        self._optimized: dict[str, URL | Auth | Data ] = {}


        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.SFTP]

        self.address_family = address_family
        self.address_protocol = protocol

    async def _execute(
        self,
        command_type: CommandType,
        request_url: str | URL,
        command_args: tuple[Any, ...],
        options: SFTPOptions,
        username: str | None = None,
        password: str | None = None,
    ):
        timings: dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "initialization_start",
                "initialization_end",
                "command_start",
                "command_end",
                "request_end",
            ],
            float | None,
        ] = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "initialization_start": None,
            "initialization_end": None,
            "command_start": None,
            "command_end": None,
            "request_end": None,
        }

        if command_args is None:
            command_args = ()

        if command_options is None:
            command_options = {}

        timings["request_start"] = time.monotonic()

        connection: SFTPConnection | None = None
        
        try:
            timings["connect_start"] = time.monotonic()

            (
                err,
                connection,
                url,
            ) = await self._connect(
                request_url,
                username=username,
                password=password,
                **self._connection_options.options,
            )

            if err:
                timings["connect_end"] = time.monotonic()
                self._connections.append(SFTPConnection())

            timings["connect_end"] = time.monotonic()
            timings["initialization_start"] = time.monotonic()

            handler = await connection.create_session(
                env=self._connection_options.env or (),
                send_env=self._connection_options.remote_env or (),
                sftp_version=self._connection_options.sftp_version,
            )

            timings["initialization_end"] = time.monotonic()

            command = SFTPCommand(
                handler,
                self._loop,
                path_encoding=self._connection_options.path_encoding,
            )

            timings["command_start"] = time.monotonic()

            result: tuple[
                float,
                dict[bytes | Any, TransferResult]
            ] = (0, {})

            match command_type:
                case "chdir":
                    result = await command.chdir(*command_args, options)

                case "chmod":
                    result = await command.chmod(*command_args, options)

                case "chown":
                    result = await command.chown(*command_args, options)

                case "copy":
                    result = await command.copy(*command_args, options)

                case "exists":
                    result = await command.exists(*command_args, options)

                case "get":
                    result = await command.get(*command_args, options)

                case "getatime":
                    result = await command.getatime(*command_args, options)

                case "getatime_ns":
                    result = await command.getatime_ns(*command_args, options)

                case "getcrtime":
                    result = await command.getcrtime(*command_args, options)

                case "getcrtime_ns":
                    result = await command.getcrtime_ns(*command_args, options)

                case "getcwd":
                    result = await command.getcwd(options)

                case "getmtime":
                    result = await command.getmtime(*command_args, options)

                case "getmtime_ns":
                    result = await command.getmtime_ns(*command_args, options)

                case "getsize":
                    result = await command.getsize(*command_args, options)

                case "glob":
                    result = await command.glob(*command_args)

                case "glob_sftpname":
                    result = await command.glob_sftpname(*command_args)

                case "isdir":
                    result = await command.isdir(*command_args, options)

                case "isfile":
                    result = await command.isfile(*command_args, options)

                case "islink":
                    result = await command.islink(*command_args, options)

                case "lexists":
                    result = await command.lexists(*command_args, options)

                case "link":
                    result = await command.link(*command_args, options)

                case "listdir":
                    result = await command.scandir(path=command_args[0])

                case "lstat":
                    result = await command.lstat(*command_args, options)
                
                case "makedirs":
                    result = await command.makedirs(*command_args, options)

                case "mcopy":
                    result = await command.mcopy(*command_args, options)

                case "mget":
                    result = await command.mget(*command_args, options)

                case "mkdir":
                    result = await command.mkdir(*command_args)

                case "mput":
                    result = await command.mput(*command_args, options)

                case "posix_rename":
                    result = await command.posix_rename(*command_args)

                case "put":
                    result = await command.put(*command_args, options)

                case "readdir":
                    result = await command.scandir(path=command_args[0])

                case "readlink":
                    result = await command.readlink(*command_args)

                case "realpath":
                    result = await command.realpath(*command_args, options)

                case "remove":
                    result = await command.remove(*command_args)

                case "rename":
                    result = await command.rename(*command_args, options)

                case "rmdir":
                    result = await command.rmdir(*command_args)

                case "rmtree":
                    result = await command.rmtree(*command_args)

                case "scandir":
                    result = await command.scandir(path=command_args[0])

                case "setstat":
                    result = await command.setstat(*command_args, options)

                case "stat":
                    result = await command.stat(*command_args, options)

                case "statvfs":
                    result = await command.statvfs(*command_args)

                case "symlink":
                    result = await command.symlink(*command_args)

                case "truncate":
                    result = await command.truncate(*command_args)

                case "unlink":
                    result = await command.unlink(*command_args)

                case "utime":
                    result = await command.utime(*command_args, options)

            elapsed, operations = result
            
            timings["command_end"] = elapsed
            self._connections.append(connection)

            timings["request_end"] = time.monotonic()

        except Exception as err:
            timings["request_end"] = time.monotonic()

            self._connections.append(
                SFTPConnection()
            )


    async def _connect(
        self,
        request_url: str | URL,
        **kwargs: dict[str, Any],

    ) -> tuple[
        Exception | None,
        SFTPConnection,
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


        sftp_connection = self._connections.pop()

        connection_error: Exception | None = None

        if url.address is None:
            for address, ip_info in url:
                try:
                    await sftp_connection.make_connection(
                        ip_info,
                        **kwargs,
                    )

                    url.address = address
                    url.socket_config = ip_info
                    break

                except Exception as err:
                    connection_error = err

        else:
            try:
                await sftp_connection.make_connection(
                    url.socket_config,
                    **kwargs,
                )


            except Exception as err:
                connection_error = err

        return (
            connection_error,
            sftp_connection,
            parsed_url,
        )
