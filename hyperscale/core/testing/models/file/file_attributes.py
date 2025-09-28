import msgspec
import mimetypes
import pathlib
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs


class FileAttributes(msgspec.Struct):
    type: int | None = None
    size: int | None = None
    alloc_size: int | None = None
    uid: int | None = None
    gid: int | None = None
    owner: str | None = None
    group: str | None = None
    permissions: int | None = None
    atime: int | float | None = None
    atime_ns: int | float | None = None
    crtime: int | float | None = None
    crtime_ns: int | float | None = None
    mtime: int | float | None = None
    mtime_ns: int | float | None = None
    ctime: int | float | None = None
    ctime_ns: int | float | None = None
    mime_type: str | None = None
    nlink: int | None = None


    @classmethod
    def from_sftp_attrs(cls, attrs: SFTPAttrs):
        return FileAttributes(
            type=attrs.type,
            size=attrs.size,
            alloc_size=attrs.alloc_size,
            uid=attrs.uid,
            gid=attrs.gid,
            owner=attrs.owner,
            group=attrs.group,
            permissions=attrs.permissions,
            atime=attrs.atime,
            atime_ns=attrs.atime_ns,
            crtime=attrs.crtime,
            crtime_ns=attrs.crtime_ns,
            mime_type=attrs.mime_type,
            mtime=attrs.mtime,
            mtime_ns=attrs.mtime_ns,
            nlink=attrs.nlink,
        )
    
    @classmethod
    def from_stat(
        self,
        path: pathlib.Path
    ):
        
        result = path.stat()
        mime_type, _ = mimetypes.guess_file_type(path)

        path_type = 1

        if path.is_dir():
            path_type = 2

        elif path.is_symlink():
            path_type = 3


        elif path.is_socket():
            path_type = 6

        elif path.is_char_device():
            path_type = 7

        elif path.is_block_device():
            path_type = 8

        elif path.is_fifo():
            path_type = 9

        return FileAttributes(
            type=path_type,
            uid=result.st_uid,
            gid=result.st_gid,
            permissions=result.st_mode,
            atime=result.st_atime,
            atime_ns=result.st_atime_ns,
            ctime=result.st_ctime,
            crtime=result.st_ctime,
            crtime_ns=result.st_ctime_ns,
            mtime=result.st_mtime,
            mtime_ns=result.st_mtime_ns,
            mime_type=mime_type,
            nlink=result.st_nlink,
        )
        
    
    def to_sftp_attrs(self) -> SFTPAttrs:
        return SFTPAttrs(
            type=self.type,
            size=self.size,
            alloc_size=self.size,
            uid=self.uid,
            gid=self.gid,
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
    