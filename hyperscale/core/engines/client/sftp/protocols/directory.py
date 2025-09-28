import datetime
import pathlib
from types import TracebackType

from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, SFTPName
from hyperscale.core.engines.client.ssh.protocol.ssh.constants import FILEXFER_TYPE_DIRECTORY
from .file import SCPFile


class Directory:

    def __init__(
        self,
        path: bytes,
        user_id: int = 1000,
        group_id: int = 1000,
        owner: str = "test",
        group: str = "test",
        permissions: int = 644,
        file_time: int = 0,
        links_count: int = 1,
    ):
        
        self.path = path
        self.files: dict[str, SCPFile | Directory] = {}

        self.file_type_name = "DIRECTORY"
        self.file_type = FILEXFER_TYPE_DIRECTORY
        self.user_id = user_id
        self.group_id = group_id
        self.owner = owner
        self.group = group
        self.permissions = permissions

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
    def named_file(self):
        return SFTPName(
            self.name,
            self.path,
            self.to_attrs(),
        )
    

    async def __aenter__(self):
        return self

    async def __aexit__(
        self, 
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ): 
        return False
    
    def to_attrs(self) -> SFTPAttrs:
        return SFTPAttrs(
            type=self.file_type,
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