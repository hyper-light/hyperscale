from pydantic import BaseModel, StrictInt
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPVFSAttrs


class FilesystemAttributes(BaseModel):
    bsize: StrictInt = 0
    frsize: StrictInt = 0
    blocks: StrictInt = 0
    bfree: StrictInt = 0
    bavail: StrictInt = 0
    files: StrictInt = 0
    ffree: StrictInt = 0
    favail: StrictInt = 0
    fsid: StrictInt = 0
    flags: StrictInt = 0
    namemax: StrictInt = 0


    @classmethod
    def from_sftpvfs_attrs(cls, attrs: SFTPVFSAttrs):
        return FilesystemAttributes(
            bavail=attrs.bavail,
            bfree=attrs.bfree,
            blocks=attrs.blocks,
            bsize=attrs.bsize,
            favail=attrs.favail,
            ffree=attrs.ffree,
            files=attrs.files,
            flags=attrs.flags,
            frsize=attrs.frsize,
            fsid=attrs.fsid,
            namemax=attrs.namemax,
        )
    
    def to_sftpvfs_attrs(self) -> SFTPVFSAttrs:
        return SFTPVFSAttrs(
            bavail=self.bavail,
            bfree=self.bfree,
            blocks=self.blocks,
            bsize=self.bsize,
            favail=self.favail,
            ffree=self.ffree,
            files=self.files,
            flags=self.flags,
            frsize=self.frsize,
            fsid=self.fsid,
            namemax=self.namemax,
        )
    