import errno
import os
import sys
from typing import AsyncIterator
from .types import SFTPFileObj

if sys.platform == 'win32': # pragma: no cover
    async def request_ranges(file_obj: SFTPFileObj, offset: int,
                              length: int) -> AsyncIterator[Tuple[int, int]]:
        """Return file ranges containing data on Windows"""

        handle = msvcrt.get_osfhandle(file_obj.fileno())
        bufsize = _MAX_SPARSE_RANGES * 16

        while True:
            try:
                query_range = offset.to_bytes(8, 'little') + \
                              length.to_bytes(8, 'little')

                ranges = win32file.DeviceIoControl(
                    handle, winioctlcon.FSCTL_QUERY_ALLOCATED_RANGES,
                    query_range, bufsize, None)
            except pywintypes.error as exc:
                if exc.args[0] == winerror.ERROR_MORE_DATA:
                    bufsize *= 2
                else:
                    raise
            else:
                break

        for pos in range(0, len(ranges), 16):
            offset = int.from_bytes(ranges[pos:pos+8], 'little')
            length = int.from_bytes(ranges[pos+8:pos+16], 'little')
            yield offset, length

elif hasattr(os, 'SEEK_DATA'):
    async def request_ranges(file_obj: SFTPFileObj, offset: int,
                              length: int) -> AsyncIterator[tuple[int, int]]:
        """Return file ranges containing data"""

        end = offset
        limit = offset + length

        try:
            while end < limit:
                start = file_obj.seek(end, os.SEEK_DATA)
                end = min(file_obj.seek(start, os.SEEK_HOLE), limit)
                yield start, end - start
        except OSError as exc: # pragma: no cover
            if exc.errno != errno.ENXIO:
                raise
else: # pragma: no cover
    async def request_ranges(file_obj: SFTPFileObj, offset: int,
                              length: int) -> AsyncIterator[tuple[int, int]]:
        """Sparse files aren't supported - return the full input range"""

        # pylint: disable=unused-argument

        if length:
            yield offset, length
