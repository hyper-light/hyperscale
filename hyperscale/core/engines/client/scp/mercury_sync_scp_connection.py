import asyncio
import pathlib
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
    NEW_LINE,
    ProtocolMap,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnectionOptions
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, SFTPGlob
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from .protocols import (
    SCPConnection,
    SCPHandler,
    parse_cd_args,
    parse_time_args,
    SCP_BLOCK_SIZE,
    scp_error,
)



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
    

    def _handle_error(self, exc: Exception) -> None:
        """Handle an SCP error"""

        if isinstance(exc, BrokenPipeError):
            exc = scp_error(SFTPConnectionLost, 'Connection lost',
                             fatal=True, suppress_send=True)

        if self._error_handler and not getattr(exc, 'fatal', False):
            self._error_handler(exc)
        else:
            raise exc


    async def _connect(
        self,
        request_url: str | URL,
        path: str | pathlib.Path,
        must_be_dir: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        connection_type: Literal["SOURCE", "DEST"] = "SOURCE",
        **kwargs: dict[str, Any],

    ) -> tuple[SCPHandler, SCPHandler]:
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


        if connection_type == "SOURCE":
            ssh_connection = self._source_connections.pop()
            command = b'scp -f '

        else:
            ssh_connection = self._destination_connections.pop()
            command = b'scp -t '

        connection_error: Exception | None = None

        if must_be_dir:
            command += b'-d '

        if preserve:
            command += b'-p '

        if recurse:
            command += b'-r '

        
        command += path.encode()

        connection: SCPHandler | None = None

        if url.address is None:
            for address, ip_info in url:
                try:
                    connection = await ssh_connection.make_connection(
                        command,
                        ip_info,
                        **kwargs,
                    )

                    url.address = address
                    url.socket_config = ip_info
                    break

                except Exception as err:
                    if "server_hostname is only meaningful with ssl" in str(err):
                        return (
                            None,
                            None,
                            None,
                            parsed_url,
                            True,
                        )

        else:
            try:
                connection = await ssh_connection.make_connection(
                    command,
                    ip_info,
                    **kwargs,
                )


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

        return (
            connection_error,
            connection,
            parsed_url,
            False,
        )
