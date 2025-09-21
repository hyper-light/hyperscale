from pydantic import BaseModel, StrictInt, StrictFloat, StrictStr
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs


class FileAttributes(BaseModel):
    type: StrictInt | None = None
    size: StrictInt | None = None
    alloc_size: StrictInt | None = None
    uid: StrictInt | None = None
    gid: StrictInt | None = None
    owner: StrictStr | None = None
    group: StrictStr | None = None
    permissions: StrictInt | None = None
    atime: StrictInt | None = None
    atime_ns: StrictInt | StrictFloat | None = None
    crtime: StrictInt | None = None
    crtime_ns: StrictInt | StrictFloat | None = None
    mtime: StrictInt | None = None
    mtime_ns: StrictInt | StrictFloat | None = None
    ctime: StrictInt | None = None
    ctime_ns: StrictInt | StrictFloat | None = None
    mime_type: StrictStr | None = None
    nlink: StrictInt | None = None


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
    