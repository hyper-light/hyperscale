from typing import Self
from .sftp_attrs import SFTPAttrs
from .record import Record
from .packet import UInt64, SSHPacket, UInt32, Boolean, String


class SFTPLimits(Record):
    """SFTP server limits

       SFTPLimits is a simple record class with the following fields:

         ================= ========================================= ======
         Field             Description                               Type
         ================= ========================================= ======
         max_packet_len    Max allowed size of an SFTP packet        uint64
         max_read_len      Max allowed size of an SFTP read request  uint64
         max_write_len     Max allowed size of an SFTP write request uint64
         max_open_handles  Max allowed number of open file handles   uint64
         ================= ========================================= ======

    """

    max_packet_len: int
    max_read_len: int
    max_write_len: int
    max_open_handles: int

    def encode(self) -> bytes:
        """Encode SFTP server limits in an SSH packet"""

        return (UInt64(self.max_packet_len) + UInt64(self.max_read_len) +
                UInt64(self.max_write_len) + UInt64(self.max_open_handles))
    

    @classmethod
    def decode(cls, packet: SSHPacket) -> Self:
        """Decode bytes in an SSH packet as SFTP server limits"""

        max_packet_len = packet.get_uint64()
        max_read_len = packet.get_uint64()
        max_write_len = packet.get_uint64()
        max_open_handles = packet.get_uint64()

        return cls(max_packet_len, max_read_len,
                   max_write_len, max_open_handles)

class SFTPName(Record):
    """SFTP file name and attributes

       SFTPName is a simple record class with the following fields:

         ========= ================================== ==================
         Field     Description                        Type
         ========= ================================== ==================
         filename  Filename                           `str` or `bytes`
         longname  Expanded form of filename & attrs  `str` or `bytes`
         attrs     File attributes                    :class:`SFTPAttrs`
         ========= ================================== ==================

       A list of these is returned by :meth:`readdir() <SFTPClient.readdir>`
       in :class:`SFTPClient` when retrieving the contents of a directory.

    """

    filename: bytes | str = ''
    longname: bytes | str = ''
    attrs: SFTPAttrs = SFTPAttrs()

    def _format(self, k: str, v: object) -> str | None:
        """Convert name fields to more readable values"""

        if k == 'longname' and not v:
            return None

        if isinstance(v, bytes):
            v = v.decode('utf-8', 'backslashreplace')

        return str(v) or None


    def encode(self, sftp_version: int) -> bytes:
        """Encode an SFTP name as bytes in an SSH packet"""

        longname = String(self.longname) if sftp_version == 3 else b''

        return (String(self.filename) + longname +
                self.attrs.encode(sftp_version))

    @classmethod
    def decode(cls, packet: SSHPacket, sftp_version: int) -> 'SFTPName':
        """Decode bytes in an SSH packet as an SFTP name"""

        filename = packet.get_string()
        longname = packet.get_string() if sftp_version == 3 else None
        attrs = SFTPAttrs.decode(packet, sftp_version)

        return cls(filename, longname, attrs)


class SFTPRanges(Record):
    """SFTP sparse file ranges"""

    ranges: list[tuple[int, int]]
    at_end: bool

    def encode(self) -> bytes:
        """Encode sparse file ranges in an SSH packet"""

        return (UInt32(len(self.ranges)) +
                b''.join((UInt64(offset) + UInt64(length)
                          for offset, length in self.ranges)) +
                Boolean(self.at_end))

    @classmethod
    def decode(cls, packet: SSHPacket) -> Self:
        """Decode bytes in an SSH packet as sparse file ranges"""

        count = packet.get_uint32()
        ranges = [(packet.get_uint64(), packet.get_uint64())
                  for _ in range(count)]
        at_end = packet.get_boolean()

        return cls(ranges, at_end)