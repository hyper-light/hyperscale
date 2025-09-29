import msgspec
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPVFSAttrs


class FilesystemAttributes(msgspec.Struct):
    bsize: int = 0
    frsize: int = 0
    blocks: int = 0
    bfree: int = 0
    bavail: int = 0
    files: int = 0
    ffree: int = 0
    favail: int = 0
    fsid: int = 0
    flags: int = 0
    namemax: int = 0


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
    