import asyncio
import pathlib
import time
from collections import defaultdict
from urllib.parse import urlparse, ParseResult
from typing import Any, Literal
from hyperscale.core.engines.client.shared.models import (
    URL as SFTPUrl,
    RequestType,
)
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
    Directory,
    File,
    FileGlob,
)

from hyperscale.core.engines.client.sftp.protocols.sftp import MIN_SFTP_VERSION
from hyperscale.core.testing.models.file.file_attributes import FileAttributes
from hyperscale.core.engines.client.shared.models import URLMetadata
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.shared.protocols import (
    ProtocolMap,
)

from .models import (
    CommandType,
    SFTPOptions,
    TransferResult,
    SFTPResponse,
    AttributeFlags,
    CheckType,
    DesiredAccess,
    SFTPTimings,
)

from hyperscale.core.engines.client.ssh.models import ConnectionOptions
from .protocols import SFTPConnection
from .sftp_command import SFTPCommand


SFTPVersion = Literal["v3", "v4", "v5", "v6"]


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

        self._url_cache: dict[str, SFTPUrl] = {}
        self._optimized: dict[str, URL | Auth | Data ] = {}

        protocols = ProtocolMap()
        address_family, protocol = protocols[RequestType.SFTP]

        self._connection_options: ConnectionOptions = None

        self.address_family = address_family
        self.address_protocol = protocol

        self.sftp_version: Literal[3, 4, 5, 6] = MIN_SFTP_VERSION

        self.path_encoding: str = 'utf-8'
        self.env: dict[str, str] = {}

    async def get(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        desired_access: DesiredAccess | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "get",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            desired_access=desired_access,
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                            preserve=preserve,
                            recurse=recurse,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="get",
                    error=err,
                    timings={},
                )

    async def put(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        data: bytes | str | File | Directory,
        attriutes: FileAttributes | None = None,
        connection: ConnectionOptions | None = None,
        desired_access: DesiredAccess | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "put",
                        url,
                        command_args=(
                            path,
                            attriutes,
                            data,
                        ),
                        options=SFTPOptions(
                            desired_access=desired_access,
                            flags=flags,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="put",
                    error=err,
                    timings={},
                )
            
    async def copy(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        follow_symlinks: bool = False,
        connection: ConnectionOptions | None = None,
        desired_access: DesiredAccess | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        preserve: bool = False,
        recurse: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "copy",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            desired_access=desired_access,
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                            preserve=preserve,
                            recurse=recurse,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="copy",
                    error=err,
                    timings={},
                )
            
    async def mget(
        self,
        url: str | URL,
        pattern: str,
        connection: ConnectionOptions | None = None,
        desired_access: DesiredAccess | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "mget",
                        url,
                        command_args=(
                            pattern,
                        ),
                        options=SFTPOptions(
                            desired_access=desired_access,
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                            preserve=preserve,
                            recurse=recurse,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="mget",
                    error=err,
                    timings={},
                )
            
    async def mput(
        self,
        url: str | URL,
        pattern: str,
        data: bytes | str | FileGlob | Directory,
        connection: ConnectionOptions | None = None,
        attriutes: FileAttributes | None = None,
        disable_host_check: bool = False,
        desired_access: DesiredAccess | None = None,
        flags: list[AttributeFlags] | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "mput",
                        url,
                        command_args=(
                            pattern,
                            attriutes,
                            data,
                        ),
                        options=SFTPOptions(
                            desired_access=desired_access,
                            flags=flags,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="mput",
                    error=err,
                    timings={},
                )
            
    async def mcopy(
        self,
        url: str | URL,
        pattern: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        desired_access: DesiredAccess | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        preserve: bool = False,
        recurse: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "mcopy",
                        url,
                        command_args=(
                            pattern,
                        ),
                        options=SFTPOptions(
                            desired_access=desired_access,
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                            preserve=preserve,
                            recurse=recurse,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="mcopy",
                    error=err,
                    timings={},
                )
            
    async def glob(
        self,
        url: str | URL,
        pattern: str,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "glob",
                        url,
                        command_args=(
                            pattern,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="glob",
                    error=err,
                    timings={},
                )
            
    async def glob_sftpname(
        self,
        url: str | URL,
        pattern: str,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "glob_sftpname",
                        url,
                        command_args=(
                            pattern,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="glob_sftpname",
                    error=err,
                    timings={},
                )
            
    async def makedirs(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        attributes: FileAttributes | None = None,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        exist_ok: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "makedirs",
                        url,
                        command_args=(
                            path,
                            attributes,
                        ),
                        options=SFTPOptions(
                            exist_ok=exist_ok,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="makedirs",
                    error=err,
                    timings={},
                )
            
    async def rmtree(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "rmtree",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="rmtree",
                    error=err,
                    timings={},
                )
            
    async def stat(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "stat",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="stat",
                    error=err,
                    timings={},
                )
    
    async def lstat(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "lstat",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="lstat",
                    error=err,
                    timings={},
                )
            
    async def setstat(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        attributes: FileAttributes | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "setstat",
                        url,
                        command_args=(
                            path,
                            attributes,
                        ),
                        options=SFTPOptions(
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="setstat",
                    error=err,
                    timings={},
                )
            
    async def truncate(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        size: int,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "truncate",
                        url,
                        command_args=(
                            path,
                            size,
                        ),
                        options=SFTPOptions(
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="truncate",
                    error=err,
                    timings={},
                )
            
    async def chown(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        uid: int | None = None,
        gid: int | None = None,
        owner: str | None = None,
        group: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "chown",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            uid=uid,
                            gid=gid,
                            owner=owner,
                            group=group,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="chown",
                    error=err,
                    timings={},
                )
            
    async def statvfs(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "statvfs",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="statvfs",
                    error=err,
                    timings={},
                )
            
    async def utime(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        nanoseconds: tuple[int, int]  | None = None,
        times: tuple[float, float] | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "utime",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            nanoseconds=nanoseconds,
                            times=times,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="utime",
                    error=err,
                    timings={},
                )
            
    async def exists(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "exists",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="exists",
                    error=err,
                    timings={},
                )
            
    async def lexists(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "lexists",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="lexists",
                    error=err,
                    timings={},
                )
            
    async def getatime(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getatime",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getatime",
                    error=err,
                    timings={},
                )
            
    async def getatime_ns(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getatime_ns",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getatime_ns",
                    error=err,
                    timings={},
                )
            
    async def getcrtime(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getcrtime",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getcrtime",
                    error=err,
                    timings={},
                )
            
    async def getcrtime_ns(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getcrtime_ns",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getcrtime_ns",
                    error=err,
                    timings={},
                )
            
    async def getmtime(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getmtime",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getmtime",
                    error=err,
                    timings={},
                )
            
    async def getmtime_ns(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getmtime_ns",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getmtime_ns",
                    error=err,
                    timings={},
                )
            
    async def getsize(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getsize",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getsize",
                    error=err,
                    timings={},
                )
            
    async def isdir(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "isdir",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="isdir",
                    error=err,
                    timings={},
                )
            
    async def isfile(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "isfile",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="isfile",
                    error=err,
                    timings={},
                )
            
    async def islink(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        follow_symlinks: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "islink",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                            follow_symlinks=follow_symlinks,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="islink",
                    error=err,
                    timings={},
                )
            
    async def remove(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "remove",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="remove",
                    error=err,
                    timings={},
                )
            
    async def unlink(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "unlink",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="unlink",
                    error=err,
                    timings={},
                )
            
    async def rename(
        self,
        url: str | URL,
        from_path: str | pathlib.PurePath,
        to_path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "rename",
                        url,
                        command_args=(
                            from_path,
                            to_path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="rename",
                    error=err,
                    timings={},
                )
            
    async def posix_rename(
        self,
        url: str | URL,
        from_path: str | pathlib.PurePath,
        to_path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        flags: list[AttributeFlags] | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "posix_rename",
                        url,
                        command_args=(
                            from_path,
                            to_path,
                        ),
                        options=SFTPOptions(
                            flags=flags,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="posix_rename",
                    error=err,
                    timings={},
                )
            
    async def scandir(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "scandir",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="scandir",
                    error=err,
                    timings={},
                )
            
    async def mkdir(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        attributes: FileAttributes | None = None,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "mkdir",
                        url,
                        command_args=(
                            path,
                            attributes,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="mkdir",
                    error=err,
                    timings={},
                )
            
    async def rmdir(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "rmdir",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="rmdir",
                    error=err,
                    timings={},
                )
            
    async def realpath(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        check: CheckType = "none",
        join_paths: list[str | pathlib.PurePath] | None = None,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "realpath",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                          check=check,
                          compose_paths=join_paths,  
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="realpath",
                    error=err,
                    timings={},
                )
            
    async def getcwd(
        self,
        url: str | URL,
        check: CheckType = "none",
        join_paths: list[str | pathlib.PurePath] | None = None,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "getcwd",
                        url,
                        options=SFTPOptions(
                          check=check,
                          compose_paths=join_paths,  
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="getcwd",
                    error=err,
                    timings={},
                )
            
    async def readlink(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "readlink",
                        url,
                        command_args=(
                            path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="readlink",
                    error=err,
                    timings={},
                )
            
    async def symlink(
        self,
        url: str | URL,
        from_path: str | pathlib.Path,
        to_path: str | pathlib.Path,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "symlink",
                        url,
                        command_args=(
                            from_path,
                            to_path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="symlink",
                    error=err,
                    timings={},
                )
            
    async def link(
        self,
        url: str | URL,
        from_path: str | pathlib.Path,
        to_path: str | pathlib.Path,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "link",
                        url,
                        command_args=(
                            from_path,
                            to_path,
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="link",
                    error=err,
                    timings={},
                )
            
    async def chdir(
        self,
        url: str | URL,
        path: str | pathlib.PurePath,
        check: CheckType = "none",
        join_paths: list[str | pathlib.PurePath] | None = None,
        connection: ConnectionOptions | None = None,
        disable_host_check: bool = False,
        username: str | None = None,
        password: str | None = None,
        timeout: int | float | None = None,
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._execute(
                        "chdir",
                        url,
                        command_args=(
                            path,
                        ),
                        options=SFTPOptions(
                          check=check,
                          compose_paths=join_paths,  
                        ),
                        connection_options=connection,
                        disable_host_check=disable_host_check,
                        username=username,
                        password=password,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError as err:
                if isinstance(url, str):
                    url_data = urlparse(url)

                else:
                    url_data = url.optimized.parsed

                return SFTPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    action="chdir",
                    error=err,
                    timings={},
                )
            
    async def _execute(
        self,
        command_type: CommandType,
        request_url: str | URL,
        connection_options: ConnectionOptions | None = None,
        command_args: tuple[Any, ...]  | None = None,
        options: SFTPOptions | None = None,
        username: str | None = None,
        password: str | None = None,
        disable_host_check: bool = False,
    ):
        timings: dict[
            SFTPTimings,
            float | None,
        ] = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "initialization_start": None,
            "initialization_end": None,
            "execution_start": None,
            "execution_end": None,
            "close_start": None,
            "close_end": None,
            "request_end": None,
        }

        if command_args is None:
            command_args = ()

        timings["request_start"] = time.monotonic()

        connection: SFTPConnection | None = None
        
        try:
            timings["connect_start"] = time.monotonic()

            default_connection_options = self._connection_options.to_dict()
            if connection_options:
                default_connection_options.update(
                    connection_options.to_dict(),
                )

            if username:
                default_connection_options["username"] = username

            if password:
                default_connection_options["password"] = password

            if disable_host_check:
                default_connection_options['known_hosts'] = None

            (
                err,
                connection,
                url,
            ) = await self._connect(
                request_url,
                **default_connection_options,
            )

            if err:
                timings["connect_end"] = time.monotonic()
                self._connections.append(SFTPConnection())
                
                return SFTPResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                        params=url.params,
                        query=url.query,
                    ),
                    operation=command_type,
                    timings=timings,
                    error=err,
                )

            timings["connect_end"] = time.monotonic()
            timings["initialization_start"] = time.monotonic()

            handler = await connection.create_session(
                env=self.env,
                sftp_version=self.sftp_version,
            )

            timings["initialization_end"] = time.monotonic()

            command = SFTPCommand(
                handler,
                path_encoding=self.path_encoding,
            )

            timings["execution_start"] = time.monotonic()

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

            elapsed, transferred = result
            
            timings["exectution_end"] = elapsed
            timings["close_start"] = time.monotonic()

            command.exit()
            await command.wait_closed()

            timings["close_end"] = time.monotonic()
            
            self._connections.append(connection)

            timings["request_end"] = time.monotonic()

            return SFTPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path,
                    params=url.params,
                    query=url.query,
                ),
                operation=command_type,
                transferred=transferred,
                timings=timings,

            )

        except Exception as err:
            timings["request_end"] = time.monotonic()

            self._connections.append(
                SFTPConnection()
            )


            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            elif isinstance(request_url, URL) and request_url.optimized:
                request_url: ParseResult = request_url.optimized.parsed


            return SFTPResponse(
                url=URLMetadata(
                    host=request_url.hostname,
                    path=request_url.path,
                    params=request_url.params,
                    query=request_url.query,
                ),
                operation=command_type,
                timings=timings,
                error=err,
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
    
    def close(self):
        for connection in self._connections:
            connection.close()
