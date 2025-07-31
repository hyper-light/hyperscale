import os
from .packet import UInt64, SSHPacket
from .record import Record


class SFTPVFSAttrs(Record):
    """SFTP file system attributes

       SFTPVFSAttrs is a simple record class with the following fields:

         ============ =========================================== ======
         Field        Description                                 Type
         ============ =========================================== ======
         bsize        File system block size (I/O size)           uint64
         frsize       Fundamental block size (allocation size)    uint64
         blocks       Total data blocks (in frsize units)         uint64
         bfree        Free data blocks                            uint64
         bavail       Available data blocks (for non-root)        uint64
         files        Total file inodes                           uint64
         ffree        Free file inodes                            uint64
         favail       Available file inodes (for non-root)        uint64
         fsid         File system id                              uint64
         flags        File system flags (read-only, no-setuid)    uint64
         namemax      Maximum filename length                     uint64
         ============ =========================================== ======

    """

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

    def encode(self, sftp_version: int) -> bytes:
        """Encode SFTP statvfs attributes as bytes in an SSH packet"""

        # pylint: disable=unused-argument

        return b''.join((UInt64(self.bsize), UInt64(self.frsize),
                         UInt64(self.blocks), UInt64(self.bfree),
                         UInt64(self.bavail), UInt64(self.files),
                         UInt64(self.ffree), UInt64(self.favail),
                         UInt64(self.fsid), UInt64(self.flags),
                         UInt64(self.namemax)))

    @classmethod
    def decode(cls, packet: SSHPacket, sftp_version: int) -> 'SFTPVFSAttrs':
        """Decode bytes in an SSH packet as SFTP statvfs attributes"""

        # pylint: disable=unused-argument

        vfsattrs = cls()

        vfsattrs.bsize = packet.get_uint64()
        vfsattrs.frsize = packet.get_uint64()
        vfsattrs.blocks = packet.get_uint64()
        vfsattrs.bfree = packet.get_uint64()
        vfsattrs.bavail = packet.get_uint64()
        vfsattrs.files = packet.get_uint64()
        vfsattrs.ffree = packet.get_uint64()
        vfsattrs.favail = packet.get_uint64()
        vfsattrs.fsid = packet.get_uint64()
        vfsattrs.flags = packet.get_uint64()
        vfsattrs.namemax = packet.get_uint64()

        return vfsattrs

    @classmethod
    def from_local(cls, result: os.statvfs_result) -> 'SFTPVFSAttrs':
        """Convert from local statvfs attributes"""

        return cls(result.f_bsize, result.f_frsize, result.f_blocks,
                   result.f_bfree, result.f_bavail, result.f_files,
                   result.f_ffree, result.f_favail, 0, result.f_flag,
                   result.f_namemax)
