
from os import SEEK_SET, SEEK_CUR, SEEK_END
from typing import Self, Type, AsyncIterator, overload, TYPE_CHECKING
from .constants import (
    MAX_SFTP_READ_LEN,
    FILEXFER_ATTR_DEFINED_V4,
)
from .error import SFTPEOFError
from .sftp_attrs import SFTPAttrs, utime_to_attrs
from .sftp_io import SFTPFileReader, SFTPFileWriter
from .sftpvfs_attrs import SFTPVFSAttrs
from .types import TracebackType


if TYPE_CHECKING:
    from .sftp_client_handler import SFTPClientHandler


class SFTPClientFile:
    """SFTP client remote file object

       This class represents an open file on a remote SFTP server. It
       is opened with the :meth:`open() <SFTPClient.open>` method on the
       :class:`SFTPClient` class and provides methods to read and write
       data and get and set attributes on the open file.

    """

    def __init__(self, handler: SFTPClientHandler, handle: bytes,
                 appending: bool, encoding: str | None, errors: str,
                 block_size: int, max_requests: int):
        self._handler = handler
        self._handle: bytes | None = handle
        self._appending = appending
        self._encoding = encoding
        self._errors = errors
        self._offset = None if appending else 0

        self.read_len = \
            handler.limits.max_read_len if block_size == -1 else block_size
        self.write_len = \
            handler.limits.max_write_len if block_size == -1 else block_size

        if max_requests <= 0:
            if self.read_len:
                max_requests = max(16, min(MAX_SFTP_READ_LEN //
                                           self.read_len, 128))
            else:
                max_requests = 1

        self._max_requests = max_requests

    async def __aenter__(self) -> Self:
        """Allow SFTPClientFile to be used as an async context manager"""

        return self

    async def __aexit__(self, _exc_type: Type[BaseException] | None,
                        _exc_value: BaseException | None,
                        _traceback: TracebackType | None) -> bool:
        """Wait for file close when used as an async context manager"""

        await self.close()
        return False

    @property
    def handle(self) -> bytes:
        """Return handle or raise an error if clsoed"""

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        return self._handle

    async def _end(self) -> int:
        """Return the offset of the end of the file"""

        attrs = await self.stat()
        return attrs.size or 0

    async def request_ranges(self, offset: int, length: int) -> \
            AsyncIterator[tuple[int, int]]:
        """Return file ranges containing data in a remote file"""

        next_offset = offset
        next_length = length
        end = offset + length
        at_end = False

        try:
            while not at_end:
                result = await self._handler.request_ranges(
                    self.handle, next_offset, next_length)

                if result.ranges:
                    # pylint: disable=undefined-loop-variable

                    for range_offset, range_length in result.ranges:
                        yield range_offset, range_length

                    next_offset = range_offset + range_length
                    next_length = end - next_offset
                else: # pragma: no cover
                    break

                at_end = result.at_end
        except SFTPEOFError:
            pass

    async def read(self, size: int = -1,
                   offset: int = None) -> str | bytes:
        """Read data from the remote file

           This method reads and returns up to `size` bytes of data
           from the remote file. If size is negative, all data up to
           the end of the file is returned.

           If offset is specified, the read will be performed starting
           at that offset rather than the current file position. This
           argument should be provided if you want to issue parallel
           reads on the same file, since the file position is not
           predictable in that case.

           Data will be returned as a string if an encoding was set when
           the file was opened. Otherwise, data is returned as bytes.

           An empty `str` or `bytes` object is returned when at EOF.

           :param size:
               The number of bytes to read
           :param offset: (optional)
               The offset from the beginning of the file to begin reading
           :type size: `int`
           :type offset: `int`

           :returns: data read from the file, as a `str` or `bytes`

           :raises: | :exc:`ValueError` if the file has been closed
                    | :exc:`UnicodeDecodeError` if the data can't be
                      decoded using the requested encoding
                    | :exc:`SFTPError` if the server returns an error

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        if offset is None:
            offset = self._offset

        # If self._offset is None, we're appending and haven't sought
        # backward in the file since the last write, so there's no
        # data to return

        data = b''

        if offset is not None:
            if size is None or size < 0:
                size = (await self._end()) - offset

            try:
                if self.read_len and size > \
                        min(self.read_len, self._handler.limits.max_read_len):
                    data = await SFTPFileReader(
                        self.read_len, self._max_requests, self._handler,
                        self._handle, offset, size).run()
                else:
                    data, _ = await self._handler.read(self._handle,
                                                       offset, size)

                self._offset = offset + len(data)
            except SFTPEOFError:
                pass

        if self._encoding:
            return data.decode(self._encoding, self._errors)
        else:
            return data

    async def read_parallel(self, size: int = -1,
                            offset: int | None = None) -> \
            AsyncIterator[tuple[int, bytes]]:
        """Read parallel blocks of data from the remote file

           This method reads and returns up to `size` bytes of data
           from the remote file. If size is negative, all data up to
           the end of the file is returned.

           If offset is specified, the read will be performed starting
           at that offset rather than the current file position.

           Data is returned as a series of tuples delivered by an
           async iterator, where each tuple contains an offset and
           data bytes. Encoding is ignored here, since multi-byte
           characters may be split across block boundaries.

           To maximize performance, multiple reads are issued in
           parallel, and data blocks may be returned out of order.
           The size of the blocks and the maximum number of
           outstanding read requests can be controlled using
           the `block_size` and `max_requests` arguments passed
           in the call to the :meth:`open() <SFTPClient.open>`
           method on the :class:`SFTPClient` class.

           :param size:
               The number of bytes to read
           :param offset: (optional)
               The offset from the beginning of the file to begin reading
           :type size: `int`
           :type offset: `int`

           :returns: an async iterator of tuples of offset and data bytes

           :raises: | :exc:`ValueError` if the file has been closed
                    | :exc:`SFTPError` if the server returns an error

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        if offset is None:
            offset = self._offset

        # If self._offset is None, we're appending and haven't sought
        # backward in the file since the last write, so there's no
        # data to return

        if offset is not None:
            if size is None or size < 0:
                size = (await self._end()) - offset
        else:
            offset = 0
            size = 0

        return SFTPFileReader(self.read_len, self._max_requests,
                               self._handler, self._handle, offset,
                               size).iter()

    async def write(self, data: str | bytes, offset: int | None = None) -> int:
        """Write data to the remote file

           This method writes the specified data at the current
           position in the remote file.

           :param data:
               The data to write to the file
           :param offset: (optional)
               The offset from the beginning of the file to begin writing
           :type data: `str` or `bytes`
           :type offset: `int`

           If offset is specified, the write will be performed starting
           at that offset rather than the current file position. This
           argument should be provided if you want to issue parallel
           writes on the same file, since the file position is not
           predictable in that case.

           :returns: number of bytes written

           :raises: | :exc:`ValueError` if the file has been closed
                    | :exc:`UnicodeEncodeError` if the data can't be
                      encoded using the requested encoding
                    | :exc:`SFTPError` if the server returns an error

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        if offset is None:
            # Offset is ignored when appending, so fill in an offset of 0
            # if we don't have a current file position
            offset = self._offset or 0

        if self._encoding:
            data_bytes = data.encode(self._encoding, self._errors)
        else:
            data_bytes = data

        datalen = len(data_bytes)

        if self.write_len and datalen > self.write_len:
            await SFTPFileWriter(
                self.write_len, self._max_requests, self._handler,
                self._handle, offset, data_bytes).run()
        else:
            await self._handler.write(self._handle, offset, data_bytes)

        self._offset = None if self._appending else offset + datalen
        return datalen

    async def seek(self, offset: int, from_what: int = SEEK_SET) -> int:
        """Seek to a new position in the remote file

           This method changes the position in the remote file. The
           `offset` passed in is treated as relative to the beginning
           of the file if `from_what` is set to `SEEK_SET` (the
           default), relative to the current file position if it is
           set to `SEEK_CUR`, or relative to the end of the file
           if it is set to `SEEK_END`.

           :param offset:
               The amount to seek
           :param from_what: (optional)
               The reference point to use
           :type offset: `int`
           :type from_what: `SEEK_SET`, `SEEK_CUR`, or `SEEK_END`

           :returns: The new byte offset from the beginning of the file

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        if from_what == SEEK_SET:
            self._offset = offset
        elif from_what == SEEK_CUR:
            if self._offset is None:
                self._offset = (await self._end()) + offset
            else:
                self._offset += offset
        elif from_what == SEEK_END:
            self._offset = (await self._end()) + offset
        else:
            raise ValueError('Invalid reference point')

        return self._offset

    async def tell(self) -> int:
        """Return the current position in the remote file

           This method returns the current position in the remote file.

           :returns: The current byte offset from the beginning of the file

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        if self._offset is None:
            self._offset = await self._end()

        return self._offset

    async def stat(self, flags = FILEXFER_ATTR_DEFINED_V4) -> SFTPAttrs:
        """Return file attributes of the remote file

           This method queries file attributes of the currently open file.

           :param flags: (optional)
               Flags indicating attributes of interest (SFTPv4 or later)
           :type flags: `int`

           :returns: An :class:`SFTPAttrs` containing the file attributes

           :raises: :exc:`SFTPError` if the server returns an error

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        return await self._handler.fstat(self._handle, flags)

    async def setstat(self, attrs: SFTPAttrs) -> None:
        """Set attributes of the remote file

           This method sets file attributes of the currently open file.

           :param attrs:
               File attributes to set on the file
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        await self._handler.fsetstat(self._handle, attrs)

    async def statvfs(self) -> SFTPVFSAttrs:
        """Return file system attributes of the remote file

           This method queries attributes of the file system containing
           the currently open file.

           :returns: An :class:`SFTPVFSAttrs` containing the file system
                     attributes

           :raises: :exc:`SFTPError` if the server doesn't support this
                    extension or returns an error

        """

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        return await self._handler.fstatvfs(self._handle)

    async def truncate(self, size: int | None = None) -> None:
        """Truncate the remote file to the specified size

           This method changes the remote file's size to the specified
           value. If a size is not provided, the current file position
           is used.

           :param size: (optional)
               The desired size of the file, in bytes
           :type size: `int`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        if size is None:
            size = self._offset

        await self.setstat(SFTPAttrs(size=size))

    @overload
    async def chown(self, uid: int, gid: int) -> None: ... # pragma: no cover

    @overload
    async def chown(self, owner: str,
                    group: str) -> None: ... # pragma: no cover

    async def chown(self, uid_or_owner = None, gid_or_group = None,
                    uid = None, gid = None, owner = None, group = None):
        """Change the owner user and group of the remote file

           This method changes the user and group of the currently open file.

           :param uid:
               The new user id to assign to the file
           :param gid:
               The new group id to assign to the file
           :param owner:
               The new owner to assign to the file (SFTPv4 only)
           :param group:
               The new group to assign to the file (SFTPv4 only)
           :type uid: `int`
           :type gid: `int`
           :type owner: `str`
           :type group: `str`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        if isinstance(uid_or_owner, int):
            uid = uid_or_owner
        elif isinstance(uid_or_owner, str):
            owner = uid_or_owner

        if isinstance(gid_or_group, int):
            gid = gid_or_group
        elif isinstance(gid_or_group, str):
            group = gid_or_group

        await self.setstat(SFTPAttrs(uid=uid, gid=gid,
                                     owner=owner, group=group))

    async def chmod(self, mode: int) -> None:
        """Change the file permissions of the remote file

           This method changes the permissions of the currently
           open file.

           :param mode:
               The new file permissions, expressed as an int
           :type mode: `int`

           :raises: :exc:`SFTPError` if the server returns an error

        """

        await self.setstat(SFTPAttrs(permissions=mode))

    async def utime(self, times: tuple[float, float] | None = None,
                    ns: tuple[int, int] | None = None) -> None:
        """Change the access and modify times of the remote file

           This method changes the access and modify times of the
           currently open file. If `times` is not provided,
           the times will be changed to the current time.

           :param times: (optional)
               The new access and modify times, as seconds relative to
               the UNIX epoch
           :param ns: (optional)
               The new access and modify times, as nanoseconds relative to
               the UNIX epoch
           :type times: tuple of two `int` or `float` values
           :type ns: tuple of two `int` values

           :raises: :exc:`SFTPError` if the server returns an error

        """

        await self.setstat(utime_to_attrs(times, ns))

    async def lock(self, offset: int, length: int, flags: int) -> None:
        """Acquire a byte range lock on the remote file"""

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        await self._handler.lock(self._handle, offset, length, flags)

    async def unlock(self, offset: int, length: int) -> None:
        """Release a byte range lock on the remote file"""

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        await self._handler.unlock(self._handle, offset, length)

    async def fsync(self) -> None:
        """Force the remote file data to be written to disk"""

        if self._handle is None:
            raise ValueError('I/O operation on closed file')

        await self._handler.fsync(self._handle)

    async def close(self) -> None:
        """Close the remote file"""

        if self._handle:
            await self._handler.close(self._handle)
            self._handle = None
