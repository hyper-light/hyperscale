import datetime
import errno
import io
import os
import pathlib
from types import TracebackType
from typing import Literal

from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, SFTPName
from hyperscale.core.engines.client.ssh.protocol.ssh.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY



FileType = Literal[
    "REGULAR",
    "DIRECTORY",
]



class SCPFile:

    def __init__(
        self,
        path: bytes,
        data: bytes | io.BytesIO,
        file_type: FileType = "REGULAR",
        user_id: int = 1000,
        group_id: int = 1000,
        owner: str = "test",
        group: str = "test",
        permissions: int = 644,
        file_time: int = 0,
        links_count: int = 1,
    ):
        
        self.path = path

        if isinstance(data, io.BytesIO):
            self._data = data

        else:
            self._data = io.BytesIO(data)

        file_type_value = FILEXFER_TYPE_REGULAR
        if file_type == FILEXFER_TYPE_DIRECTORY:
            file_type_value = FILEXFER_TYPE_DIRECTORY

        self.file_type_name: FileType = file_type
        self.file_type = file_type_value
        self.user_id = user_id
        self.group_id = group_id
        self.owner = owner
        self.group = group
        self.permissions = permissions
        self.size = len(data)

        if file_time is None:  
            file_time = datetime.datetime.now(
                datetime.timezone.utc,
            ).timestamp()

        file_time_ns = file_time * 10**9

        self.atime = file_time
        self.atime_ns = file_time_ns

        self.crtime = file_time
        self.crtime_ns = file_time_ns

        self.mtime = file_time
        self.mtime_ns = file_time_ns
        self.ctime = file_time
        self.ctime_ns = file_time_ns
        self.nlink = links_count

        self.name = pathlib.Path(str(path)).name.encode()

    
    
    @property
    def data(self):
        return self._data.read()

    async def __aenter__(self):
        return self

    async def __aexit__(
        self, 
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ): 
        return False
    
    def to_name(self):
        return SFTPName(
            self.name,
            self.path,
            self.to_attrs(),
        )
    
    def to_attrs(self) -> SFTPAttrs:
        return SFTPAttrs(
            type=self.file_type,
            size=self.size,
            alloc_size=self.size,
            uid=self.user_id,
            gid=self.group_id,
            owner=self.owner,
            group=self.group,
            permissions=self.permissions,
            atime=self.atime,
            atime_ns=self.atime_ns,
            crtime=self.crtime,
            crtime_ns=self.crtime_ns,
            mtime=self.mtime,
            mtime_ns=self.mtime_ns,
            ctime=self.ctime,
            ctime_ns=self.ctime_ns,
            nlink=self.nlink,
        )
    
    def request_ranges(self, offset: int, length: int):
        """Return data ranges containing data in a local file"""

        if not hasattr(os, 'SEEK_DATA'):
            return offset, length if length else None

        
        end = offset
        limit = offset + length

        try:
            while end < limit:
                start = self._data.seek(end, os.SEEK_DATA)
                end = min(self._data.seek(start, os.SEEK_HOLE), limit)
                yield start, end - start

        except OSError as exc: # pragma: no cover
            if exc.errno != errno.ENXIO:
                raise exc

    async def read(self, size: int, offset: int) -> bytes:
        """Read data from the local file"""

        self._data.seek(offset)
        return self._data.read(size)

    async def write(self, data: bytes, offset: int) -> int:
        """Write data to the local file"""

        self._data.seek(offset)
        return self._data.write(data)