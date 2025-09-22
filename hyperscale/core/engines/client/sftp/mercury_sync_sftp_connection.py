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

from .models import CommandType
from .protocols import SFTPConnection
from .protocols.sftp import MIN_SFTP_VERSION, LocalFS
from .sftp_command import SFTPCommand



class MercurySyncSFTPConnction:

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
        path: str,
        sftp_version: int = MIN_SFTP_VERSION,
        path_encoding: str = 'utf-8',
        username: str | None = None,
        password: str | None = None,
        env: dict[str, str] | None = None,
        remote_env: dict[str, str] | None = None,
        command_args: tuple[Any, ...] | None = None,
        connection_options: dict[str, Any] | None = None,
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

        if connection_options is None:
            connection_options = {}

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
                **connection_options,
            )

            if err:
                timings["connect_end"] = time.monotonic()
                self._connections.append(SFTPConnection())

            timings["connect_end"] = time.monotonic()
            timings["initialization_start"] = time.monotonic()

            handler = await connection.create_session(
                env=env or (),
                send_env=remote_env or (),
                sftp_version=sftp_version,
            )

            timings["initialization_end"] = time.monotonic()

            command = SFTPCommand(
                handler,
                LocalFS(),
                path_encoding=path_encoding,
            )

            timings["command_start"] = time.monotonic()

            match command_type:
                case "chdir":
                    await command.chdir(path)

                case "chmod":
                    await command.chmod(path, *command_args, **command_options)

                case "chown":
                    await command.chown(path, **command_options)

                case "copy":
                    await command.copy(path, *command_args, **command_options)

                case "exists":
                    await command.exists(path)

                case "get":
                    await command.get(path, *command_args, **command_options)

                case "getatime":
                    await command.getatime(path)

                case "getatime_ns":
                    await command.getatime_ns(path)

                case "getcrtime":
                    await command.getcrtime(path)

                case "getcrtime_ns":
                    await command.getcrtime_ns(path)

                case "getcwd":
                    await command.getcwd()

                case "getmtime":
                    await command.getmtime(path)

                case "getmtime_ns":
                    await command.getmtime_ns(path)

                case "getsize":
                    await command.getsize(path)

                case "glob":
                    await command.glob(path)

                case "glob_sftpname":
                    await command.glob_sftpname(path)

                case "isdir":
                    await command.isdir(path)

                case "isfile":
                    await command.isfile(path)

                case "islink":
                    await command.islink(path)

                case "lexists":
                    await command.lexists(path)

                case "link":
                    await command.link(path, *command_args)

                case "listdir":
                    await command.scandir(path)

                case "lstat":
                    await command.lstat(path, **command_options)
                
                case "makedirs":
                    await command.makedirs(path, **command_options)

                case "mcopy":
                    await command.mcopy(path, *command_args, **command_options)

                case "mget":
                    await command.mget(path, *command_args, **command_options)

                case "mkdir":
                    await command.mkdir(path, **command_options)

                case "mput":
                    await command.mput(path, *command_args, **command_options)

                case "posix_rename":
                    await command.posix_rename(path, *command_args)

                case "put":
                    await command.put(path, *command_args, **command_options)

                case "readdir":
                    await command.scandir(path)

                case "readlink":
                    await command.readlink(path)

                case "realpath":
                    await command.realpath(path)

                case "remove":
                    await command.remove(path)

                case "rename":
                    await command.rename(path, *command_args, **command_options)

                case "rmdir":
                    await command.rmdir(path)

                case "rmtree":
                    await command.rmtree(path)

                case "scandir":
                    await command.scandir(path)

                case "setstat":
                    await command.setstat(path, *command_args, **command_options)

                case "stat":
                    await command.stat(path, *command_args, **command_options)

                case "statvfs":
                    await command.statvfs(path)

                case "symlink":
                    await command.symlink(path, *command_args)

                case "truncate":
                    await command.truncate(path, *command_args)

                case "unlink":
                    await command.unlink(path)

                case "utime":
                    await command.utime(path, *command_args, **command_options)

            
            timings["command_end"] = time.monotonic()
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
