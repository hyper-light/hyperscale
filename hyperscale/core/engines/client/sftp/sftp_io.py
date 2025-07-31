import asyncio
from typing import Generic, Set, TypeVar, AsyncIterator, TYPE_CHECKING
from .error import SFTPError, SFTPEOFError, SFTPFailure
from .sftp_file_protocol import SFTPFileProtocol
from .sftpfs_protocol import SFTPFSProtocol
from .types import SFTPProgressHandler


if TYPE_CHECKING:
    from .sftp_client_handler import SFTPClientHandler


T = TypeVar('T')


async def request_nonsparse_range(offset: int, length: int) -> \
        AsyncIterator[tuple[int, int]]:
    """Return the entire file as the range to copy"""

    yield offset, length


class SFTPParallelIO(Generic[T]):
    """Parallelize I/O requests on files

       This class issues parallel read and write requests on files.

    """

    def __init__(self, block_size: int, max_requests: int,
                 offset: int, size: int):
        self._block_size = block_size
        self._max_requests = max_requests
        self._offset = offset
        self._bytes_left = size
        self._pending: Set[asyncio.Task[tuple[int, int, int, T]]] = set()

    async def _start_task(self, offset: int, size: int) -> \
                tuple[int, int, int, T]:
        """Start a task to perform file I/O on a particular byte range"""

        count, result = await self.run_task(offset, size)
        return offset, size, count, result

    def _start_tasks(self) -> None:
        """Create parallel file I/O tasks"""

        while self._bytes_left and len(self._pending) < self._max_requests:
            size = min(self._bytes_left, self._block_size)

            task = asyncio.ensure_future(self._start_task(self._offset, size))
            self._pending.add(task)

            self._offset += size
            self._bytes_left -= size

    async def run_task(self, offset: int, size: int) -> tuple[int, T]:
        """Perform file I/O on a particular byte range"""

        raise NotImplementedError

    async def iter(self) -> AsyncIterator[tuple[int, T]]:
        """Perform file I/O and return async iterator of results"""

        self._start_tasks()

        while self._pending:
            done, self._pending = await asyncio.wait(
                self._pending, return_when=asyncio.FIRST_COMPLETED)

            exceptions = []

            for task in done:
                try:
                    offset, size, count, result = task.result()
                    yield offset, result

                    if count and count < size:
                        self._pending.add(asyncio.ensure_future(
                            self._start_task(offset+count, size-count)))
                except SFTPEOFError:
                    self._bytes_left = 0
                except (OSError, SFTPError) as exc:
                    exceptions.append(exc)

            if exceptions:
                for task in self._pending:
                    task.cancel()

                raise exceptions[0]

            self._start_tasks()


class SFTPFileReader(SFTPParallelIO[bytes]):
    """Parallelized SFTP file reader"""

    def __init__(self, block_size: int, max_requests: int,
                 handler: SFTPClientHandler, handle: bytes,
                 offset: int, size: int):
        super().__init__(block_size, max_requests, offset, size)

        self._handler = handler
        self._handle = handle
        self._start = offset

    async def run_task(self, offset: int, size: int) -> tuple[int, bytes]:
        """Read a block of the file"""

        data, _ = await self._handler.read(self._handle, offset, size)

        return len(data), data

    async def run(self) -> bytes:
        """Reassemble and return data from parallel reads"""

        result = bytearray()

        async for offset, data in self.iter():
            pos = offset - self._start
            pad = pos - len(result)

            if pad > 0:
                result += pad * b'\0'

            result[pos:pos+len(data)] = data

        return bytes(result)


class SFTPFileWriter(SFTPParallelIO[int]):
    """Parallelized SFTP file writer"""

    def __init__(self, block_size: int, max_requests: int,
                 handler: SFTPClientHandler, handle: bytes,
                 offset: int, data: bytes):
        super().__init__(block_size, max_requests, offset, len(data))

        self._handler = handler
        self._handle = handle
        self._start = offset
        self._data = data

    async def run_task(self, offset: int, size: int) -> tuple[int, int]:
        """Write a block to the file"""

        pos = offset - self._start
        await self._handler.write(self._handle, offset,
                                  self._data[pos:pos+size])
        return size, size

    async def run(self):
        """Perform parallel writes"""

        async for _ in self.iter():
            pass


class SFTPFileCopier(SFTPParallelIO[int]):
    """SFTP file copier

       This class parforms an SFTP file copy, initiating multiple
       read and write requests to copy chunks of the file in parallel.

    """

    def __init__(self, block_size: int, max_requests: int, total_bytes: int,
                 sparse: bool, srcfs: SFTPFSProtocol, dstfs: SFTPFSProtocol,
                 srcpath: bytes, dstpath: bytes,
                 progress_handler: SFTPProgressHandler):
        super().__init__(block_size, max_requests, 0, 0)

        self._sparse = sparse

        self._srcfs = srcfs
        self._dstfs = dstfs

        self._srcpath = srcpath
        self._dstpath = dstpath

        self._src: SFTPFileProtocol | None = None
        self._dst: SFTPFileProtocol | None = None

        self._bytes_copied = 0
        self._total_bytes = total_bytes
        self._progress_handler = progress_handler

    async def run_task(self, offset: int, size: int) -> tuple[int, int]:
        """Copy a block of the source file"""

        assert self._src is not None
        assert self._dst is not None

        data = await self._src.read(size, offset)
        await self._dst.write(data, offset)
        datalen = len(data)

        return datalen, datalen

    async def run(self) -> None:
        """Perform parallel file copy"""


        try:
            self._src = await self._srcfs.open(self._srcpath, 'rb',
                                               block_size=0)
            self._dst = await self._dstfs.open(self._dstpath, 'wb',
                                               block_size=0)

            if self._progress_handler and self._total_bytes == 0:
                self._progress_handler(self._srcpath, self._dstpath, 0, 0)
                return

            if self._sparse:
                ranges = self._src.request_ranges(0, self._total_bytes)
            else:
                ranges = request_nonsparse_range(0, self._total_bytes)

            if (
                self._srcfs == self._dstfs
                and self._srcfs.__class__.__name__ == 'SFTPClient'
                and hasattr(self._srcfs, 'supports_remote_copy')
                and self._srcfs.supports_remote_copy
            ):
                async for offset, length in ranges:
                    await self._srcfs.remote_copy(
                        self._src,
                        self._dst,
                        offset, length, offset)

                    self._bytes_copied += length

                    if self._progress_handler:
                        self._progress_handler(self._srcpath, self._dstpath,
                                               self._bytes_copied,
                                               self._total_bytes)
            else:
                async for self._offset, self._bytes_left in ranges:
                    async for _, datalen in self.iter():
                        self._bytes_copied += datalen

                        if self._progress_handler and datalen != 0:
                            self._progress_handler(self._srcpath, self._dstpath,
                                                   self._bytes_copied,
                                                   self._total_bytes)

                if self._bytes_copied != self._total_bytes and not self._sparse:
                    exc = SFTPFailure('Unexpected EOF during file copy')

                    setattr(exc, 'filename', self._srcpath)
                    setattr(exc, 'offset', self._bytes_copied)

                    raise exc
        finally:
            if self._src: # pragma: no branch
                await self._src.close()

            if self._dst: # pragma: no branch
                await self._dst.close()
