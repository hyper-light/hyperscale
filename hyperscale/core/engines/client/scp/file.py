import datetime
import pathlib
from typing import Literal

from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, SFTPName
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_SYMLINK, FILEXFER_TYPE_SPECIAL
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_UNKNOWN, FILEXFER_TYPE_SOCKET
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_CHAR_DEVICE, FILEXFER_TYPE_BLOCK_DEVICE
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_FIFO



FileType = Literal[
    "REGULAR",
    "DIRECTORY",
]



class File:

    def __init__(
        self,
        path: bytes,
        data: bytes,
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
        self.data = data

        file_type_value = FILEXFER_TYPE_REGULAR
        if file_type == FILEXFER_TYPE_DIRECTORY:
            file_type_value = FILEXFER_TYPE_DIRECTORY

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

        self.attrs = SFTPAttrs(
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

        self.name = pathlib.Path(str(path)).name.encode()

        self.sftp_named_file = SFTPName(
            self.name,
            self.path,
            self.attrs,
        )