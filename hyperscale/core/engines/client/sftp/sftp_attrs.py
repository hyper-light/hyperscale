import grp
import os
import pwd
import stat
import sys
import time
from typing import Sequence, cast
from .constants import (
    FILEXFER_ATTR_ACCESSTIME,
    FILEXFER_ATTR_ACL,
    FILEXFER_ATTR_ACMODTIME,
    FILEXFER_ATTR_ALLOCATION_SIZE,
    FILEXFER_ATTR_BITS,
    FILEXFER_ATTR_CREATETIME,
    FILEXFER_ATTR_CTIME,
    FILEXFER_ATTR_EXTENDED,
    FILEXFER_ATTR_LINK_COUNT,
    FILEXFER_ATTR_MIME_TYPE,
    FILEXFER_ATTR_MODIFYTIME,
    FILEXFER_ATTR_OWNERGROUP,
    FILEXFER_ATTR_PERMISSIONS,
    FILEXFER_ATTR_SIZE,
    FILEXFER_ATTR_SUBSECOND_TIMES,
    FILEXFER_ATTR_TEXT_HINT,
    FILEXFER_ATTR_UIDGID,
    FILEXFER_ATTR_UNTRANSLATED_NAME,
    FILEXFER_TYPE_SOCKET,
    FILEXFER_TYPE_SPECIAL,
    FILEXFER_TYPE_UNKNOWN,
    FILEXFER_TYPE_REGULAR,
    FILEXFER_TYPE_DIRECTORY,
    FILEXFER_TYPE_CHAR_DEVICE,
    FILEXFER_TYPE_BLOCK_DEVICE,
    FILEXFER_TYPE_FIFO,
    FILEXFER_TYPE_SYMLINK,
    FILEXFER_ATTR_DEFINED_V3,
    FILEXFER_ATTR_DEFINED_V4,
    FILEXFER_ATTR_DEFINED_V5,
    FILEXFER_ATTR_DEFINED_V6,
    NSECS_IN_SEC,

)
from .error import (
    SFTPBadMessage,
    SFTPGroupInvalid,
    SFTPOwnerInvalid,
)
from .packet import (
    String,
    UInt32,
    UInt64,
    SSHPacket,
    Byte,
)
from .record import Record



valid_attr_flags = {
    3: FILEXFER_ATTR_DEFINED_V3,
    4: FILEXFER_ATTR_DEFINED_V4,
    5: FILEXFER_ATTR_DEFINED_V5,
    6: FILEXFER_ATTR_DEFINED_V6
}

# _const_dict: dict[str, int] = constants.__dict__
# def get_symbol_names(symbols: dict[str, int], prefix: str,
#                      strip_leading: int = 0) -> dict[int, str]:
#     """Return a mapping from values to symbol names for logging"""

#     return {value: name[strip_leading:] for name, value in symbols.items()
#             if name.startswith(prefix)}

# _file_types = {k: v.lower() for k, v in
#                get_symbol_names(_const_dict, 'FILEXFER_TYPE_', 14).items()}


_file_types: dict[str, int] = {
    1: "REGULAR",
    2: "DIRECTORY",
    3: "SYMLINK",
    4: "SPECIAL",
    5: "UNKNOWN",
    6: "SOCKET",
    7: "CHAR_DEVICE",
    8: "BLOCK_DEVICE",
    9: "FIFO"
}


def _nsec_to_tuple(nsec: int) -> tuple[int, int]:
    """Convert nanoseconds since epoch to seconds & remainder"""

    return divmod(nsec, NSECS_IN_SEC)


def _float_sec_to_tuple(sec: float) -> tuple[int, int]:
    """Convert float seconds since epoch to seconds & remainder"""

    return (int(sec), int((sec % 1) * NSECS_IN_SEC))

def _lookup_user(uid: int | None) -> str:
    """Return the user name associated with a uid"""

    if uid is not None:
        try:
            # pylint: disable=import-outside-toplevel
            user = pwd.getpwuid(uid).pw_name
        except (ImportError, KeyError):
            user = str(uid)
    else:
        user = ''

    return user


def _lookup_group(gid: int | None) -> str:
    """Return the group name associated with a gid"""

    if gid is not None:
        try:
            # pylint: disable=import-outside-toplevel
            group = grp.getgrgid(gid).gr_name
        except (ImportError, KeyError):
            group = str(gid)
    else:
        group = ''

    return group


def _stat_mode_to_filetype(mode: int) -> int:
    """Convert stat mode/permissions to file type"""

    if stat.S_ISREG(mode):
        filetype = FILEXFER_TYPE_REGULAR
    elif stat.S_ISDIR(mode):
        filetype = FILEXFER_TYPE_DIRECTORY
    elif stat.S_ISLNK(mode):
        filetype = FILEXFER_TYPE_SYMLINK
    elif stat.S_ISSOCK(mode):
        filetype = FILEXFER_TYPE_SOCKET
    elif stat.S_ISCHR(mode):
        filetype = FILEXFER_TYPE_CHAR_DEVICE
    elif stat.S_ISBLK(mode):
        filetype = FILEXFER_TYPE_BLOCK_DEVICE
    elif stat.S_ISFIFO(mode):
        filetype = FILEXFER_TYPE_FIFO
    elif stat.S_IFMT(mode) != 0:
        filetype = FILEXFER_TYPE_SPECIAL
    else:
        filetype = FILEXFER_TYPE_UNKNOWN

    return filetype


def utime_to_attrs(
    times: tuple[float, float] | None = None,
    ns: tuple[int, int] | None = None,
) -> 'SFTPAttrs':
    """Convert utime arguments to SFTPAttrs"""

    if ns:
        atime, atime_ns = _nsec_to_tuple(ns[0])
        mtime, mtime_ns = _nsec_to_tuple(ns[1])
    elif times:
        atime, atime_ns = _float_sec_to_tuple(times[0])
        mtime, mtime_ns = _float_sec_to_tuple(times[1])
    else:
        if hasattr(time, 'time_ns'):
            atime, atime_ns = _nsec_to_tuple(time.time_ns())
        else: # pragma: no cover
            atime, atime_ns = _float_sec_to_tuple(time.time())

        mtime, mtime_ns = atime, atime_ns

    return SFTPAttrs(atime=atime, atime_ns=atime_ns,
                     mtime=mtime, mtime_ns=mtime_ns)


class SFTPAttrs(Record):
    """SFTP file attributes

       SFTPAttrs is a simple record class with the following fields:

         ============ ================================================= ======
         Field        Description                                       Type
         ============ ================================================= ======
         type         File type (SFTPv4+)                               byte
         size         File size in bytes                                uint64
         alloc_size   Allocation file size in bytes (SFTPv6+)           uint64
         uid          User id of file owner                             uint32
         gid          Group id of file owner                            uint32
         owner        User name of file owner (SFTPv4+)                 string
         group        Group name of file owner (SFTPv4+)                string
         permissions  Bit mask of POSIX file permissions                uint32
         atime        Last access time, UNIX epoch seconds              uint64
         atime_ns     Last access time, nanoseconds (SFTPv4+)           uint32
         crtime       Creation time, UNIX epoch seconds (SFTPv4+)       uint64
         crtime_ns    Creation time, nanoseconds (SFTPv4+)              uint32
         mtime        Last modify time, UNIX epoch seconds              uint64
         mtime_ns     Last modify time, nanoseconds (SFTPv4+)           uint32
         ctime        Last change time, UNIX epoch seconds (SFTPv6+)    uint64
         ctime_ns     Last change time, nanoseconds (SFTPv6+)           uint32
         acl          Access control list for file (SFTPv4+)            bytes
         attrib_bits  Attribute bits set for file (SFTPv5+)             uint32
         attrib_valid Valid attribute bits for file (SFTPv5+)           uint32
         text_hint    Text/binary hint for file (SFTPv6+)               byte
         mime_type    MIME type for file (SFTPv6+)                      string
         nlink        Link count for file (SFTPv6+)                     uint32
         untrans_name Untranslated name for file (SFTPv6+)              bytes
         ============ ================================================= ======

       Extended attributes can also be added via a field named
       `extended` which is a list of bytes name/value pairs.

       When setting attributes using an :class:`SFTPAttrs`, only fields
       which have been initialized will be changed on the selected file.

    """

    type: int = FILEXFER_TYPE_UNKNOWN
    size: int | None
    alloc_size: int | None
    uid: int | None
    gid: int | None
    owner: str | None
    group: str | None
    permissions: int | None
    atime: int | None
    atime_ns: int | None
    crtime: int | None
    crtime_ns: int | None
    mtime: int | None
    mtime_ns: int | None
    ctime: int | None
    ctime_ns: int | None
    acl: bytes | None
    attrib_bits: int | None
    attrib_valid: int | None
    text_hint: int | None
    mime_type: str | None
    nlink: int | None
    untrans_name: bytes | None
    extended: Sequence[tuple[bytes, bytes]] = ()

    def _format_ns(self, k: str):
        """Convert epoch seconds & nanoseconds to a string date & time"""

        result = time.ctime(getattr(self, k))
        nsec = getattr(self, k + '_ns')

        if result and nsec:
            result = result[:19] + f'.{nsec:09d}' + result[19:]

        return result

    def _format(self, k: str, v: object) -> str | None:
        """Convert attributes to more readable values"""

        if v is None or k == 'extended' and not v:
            return None

        if k == 'type':
            return _file_types.get(cast(int, v), str(v)) \
                if v != FILEXFER_TYPE_UNKNOWN else None
        elif k == 'permissions':
            return f'{cast(int, v):04o}'
        elif k in ('atime', 'crtime', 'mtime', 'ctime'):
            return self._format_ns(k)
        elif k in ('atime_ns', 'crtime_ns', 'mtime_ns', 'ctime_ns'):
            return None
        else:
            return str(v) or None

    def encode(self, sftp_version: int) -> bytes:
        """Encode SFTP attributes as bytes in an SSH packet"""

        flags = 0
        attrs = []

        if sftp_version >= 4:
            if sftp_version < 5 and self.type >= FILEXFER_TYPE_SOCKET:
                filetype = FILEXFER_TYPE_SPECIAL
            else:
                filetype = self.type

            attrs.append(Byte(filetype))

        if self.size is not None:
            flags |= FILEXFER_ATTR_SIZE
            attrs.append(UInt64(self.size))

        if self.alloc_size is not None:
            flags |= FILEXFER_ATTR_ALLOCATION_SIZE
            attrs.append(UInt64(self.alloc_size))

        if sftp_version == 3:
            if self.uid is not None and self.gid is not None:
                flags |= FILEXFER_ATTR_UIDGID
                attrs.append(UInt32(self.uid) + UInt32(self.gid))
            elif self.owner is not None and self.group is not None:
                raise ValueError('Setting owner and group requires SFTPv4 '
                                 'or later')
        else:
            if self.owner is not None and self.group is not None:
                flags |= FILEXFER_ATTR_OWNERGROUP
                attrs.append(String(self.owner) + String(self.group))
            elif self.uid is not None and self.gid is not None:
                flags |= FILEXFER_ATTR_OWNERGROUP
                attrs.append(String(str(self.uid)) + String(str(self.gid)))

        if self.permissions is not None:
            flags |= FILEXFER_ATTR_PERMISSIONS
            attrs.append(UInt32(self.permissions))

        if sftp_version == 3:
            if self.atime is not None and self.mtime is not None:
                flags |= FILEXFER_ATTR_ACMODTIME
                attrs.append(UInt32(int(self.atime)) + UInt32(int(self.mtime)))
        else:
            subsecond = (self.atime_ns is not None or
                         self.crtime_ns is not None or
                         self.mtime_ns is not None or
                         self.ctime_ns is not None)

            if subsecond:
                flags |= FILEXFER_ATTR_SUBSECOND_TIMES

            if self.atime is not None:
                flags |= FILEXFER_ATTR_ACCESSTIME
                attrs.append(UInt64(int(self.atime)))

                if subsecond:
                    attrs.append(UInt32(self.atime_ns or 0))

            if self.crtime is not None:
                flags |= FILEXFER_ATTR_CREATETIME
                attrs.append(UInt64(int(self.crtime)))

                if subsecond:
                    attrs.append(UInt32(self.crtime_ns or 0))

            if self.mtime is not None:
                flags |= FILEXFER_ATTR_MODIFYTIME
                attrs.append(UInt64(int(self.mtime)))

                if subsecond:
                    attrs.append(UInt32(self.mtime_ns or 0))

            if sftp_version >= 6 and self.ctime is not None:
                flags |= FILEXFER_ATTR_CTIME
                attrs.append(UInt64(int(self.ctime)))

                if subsecond:
                    attrs.append(UInt32(self.ctime_ns or 0))

        if sftp_version >= 4 and self.acl is not None:
            flags |= FILEXFER_ATTR_ACL
            attrs.append(String(self.acl))

        if sftp_version >= 5 and \
                self.attrib_bits is not None and \
                self.attrib_valid is not None:
            flags |= FILEXFER_ATTR_BITS
            attrs.append(UInt32(self.attrib_bits) + UInt32(self.attrib_valid))

        if sftp_version >= 6:
            if self.text_hint is not None:
                flags |= FILEXFER_ATTR_TEXT_HINT
                attrs.append(Byte(self.text_hint))

            if self.mime_type is not None:
                flags |= FILEXFER_ATTR_MIME_TYPE
                attrs.append(String(self.mime_type))

            if self.nlink is not None:
                flags |= FILEXFER_ATTR_LINK_COUNT
                attrs.append(UInt32(self.nlink))

            if self.untrans_name is not None:
                flags |= FILEXFER_ATTR_UNTRANSLATED_NAME
                attrs.append(String(self.untrans_name))

        if self.extended:
            flags |= FILEXFER_ATTR_EXTENDED
            attrs.append(UInt32(len(self.extended)))
            attrs.extend(String(type) + String(data)
                         for type, data in self.extended)

        return UInt32(flags) + b''.join(attrs)

    @classmethod
    def decode(cls, packet: SSHPacket, sftp_version: int) -> 'SFTPAttrs':
        """Decode bytes in an SSH packet as SFTP attributes"""

        flags = packet.get_uint32()
        attrs = cls()

        # Work around a bug seen in a Huawei SFTP server where
        # FILEXFER_ATTR_MODIFYTIME is included in flags, even though
        # the SFTP version is set to 3. That flag is only defined for
        # SFTPv4 and later.
        if sftp_version == 3 and flags & (FILEXFER_ATTR_ACMODTIME |
                                          FILEXFER_ATTR_MODIFYTIME):
            flags &= ~FILEXFER_ATTR_MODIFYTIME

        unsupported_attrs = flags & ~valid_attr_flags[sftp_version]

        if unsupported_attrs:
            raise SFTPBadMessage(
                f'Unsupported attribute flags: 0x{unsupported_attrs:08x}')

        if sftp_version >= 4:
            attrs.type = packet.get_byte()

        if flags & FILEXFER_ATTR_SIZE:
            attrs.size = packet.get_uint64()

        if flags & FILEXFER_ATTR_ALLOCATION_SIZE:
            attrs.alloc_size = packet.get_uint64()

        if sftp_version == 3:
            if flags & FILEXFER_ATTR_UIDGID:
                attrs.uid = packet.get_uint32()
                attrs.gid = packet.get_uint32()
        else:
            if flags & FILEXFER_ATTR_OWNERGROUP:
                owner = packet.get_string()

                try:
                    attrs.owner = owner.decode('utf-8')
                except UnicodeDecodeError:
                    raise SFTPOwnerInvalid('Invalid owner name: ' +
                        owner.decode('utf-8', 'backslashreplace')) from None

                group = packet.get_string()

                try:
                    attrs.group = group.decode('utf-8')
                except UnicodeDecodeError:
                    raise SFTPGroupInvalid('Invalid group name: ' +
                        group.decode('utf-8', 'backslashreplace')) from None

        if flags & FILEXFER_ATTR_PERMISSIONS:
            mode = packet.get_uint32()

            if sftp_version == 3:
                attrs.type = _stat_mode_to_filetype(mode)
                attrs.permissions = mode & 0xffff
            else:
                attrs.permissions = mode & 0xfff

        if sftp_version == 3:
            if flags & FILEXFER_ATTR_ACMODTIME:
                attrs.atime = packet.get_uint32()
                attrs.mtime = packet.get_uint32()
        else:
            if flags & FILEXFER_ATTR_ACCESSTIME:
                attrs.atime = packet.get_uint64()

                if flags & FILEXFER_ATTR_SUBSECOND_TIMES:
                    attrs.atime_ns = packet.get_uint32()

            if flags & FILEXFER_ATTR_CREATETIME:
                attrs.crtime = packet.get_uint64()

                if flags & FILEXFER_ATTR_SUBSECOND_TIMES:
                    attrs.crtime_ns = packet.get_uint32()

            if flags & FILEXFER_ATTR_MODIFYTIME:
                attrs.mtime = packet.get_uint64()

                if flags & FILEXFER_ATTR_SUBSECOND_TIMES:
                    attrs.mtime_ns = packet.get_uint32()

            if flags & FILEXFER_ATTR_CTIME:
                attrs.ctime = packet.get_uint64()

                if flags & FILEXFER_ATTR_SUBSECOND_TIMES:
                    attrs.ctime_ns = packet.get_uint32()

        if flags & FILEXFER_ATTR_ACL:
            attrs.acl = packet.get_string()

        if flags & FILEXFER_ATTR_BITS:
            attrs.attrib_bits = packet.get_uint32()
            attrs.attrib_valid = packet.get_uint32()

        if flags & FILEXFER_ATTR_TEXT_HINT:
            attrs.text_hint = packet.get_byte()

        if flags & FILEXFER_ATTR_MIME_TYPE:
            try:
                attrs.mime_type = packet.get_string().decode('utf-8')
            except UnicodeDecodeError:
                raise SFTPBadMessage('Invalid MIME type') from None

        if flags & FILEXFER_ATTR_LINK_COUNT:
            attrs.nlink = packet.get_uint32()

        if flags & FILEXFER_ATTR_UNTRANSLATED_NAME:
            attrs.untrans_name = packet.get_string()

        if flags & FILEXFER_ATTR_EXTENDED:
            count = packet.get_uint32()
            attrs.extended = []

            for _ in range(count):
                attr = packet.get_string()
                data = packet.get_string()
                attrs.extended.append((attr, data))

        return attrs

    @classmethod
    def from_local(cls, result: os.stat_result) -> 'SFTPAttrs':
        """Convert from local stat attributes"""

        mode = result.st_mode
        filetype = _stat_mode_to_filetype(mode)

        if sys.platform == 'win32': # pragma: no cover
            uid = 0
            gid = 0
            owner = ''
            group = ''
        else:
            uid = result.st_uid
            gid = result.st_gid
            owner = _lookup_user(uid)
            group = _lookup_group(gid)

        atime, atime_ns = _nsec_to_tuple(result.st_atime_ns)
        mtime, mtime_ns = _nsec_to_tuple(result.st_mtime_ns)
        ctime, ctime_ns = _nsec_to_tuple(result.st_ctime_ns)

        if sys.platform == 'win32': # pragma: no cover
            crtime, crtime_ns = ctime, ctime_ns
        elif hasattr(result, 'st_birthtime'): # pragma: no cover
            crtime, crtime_ns = _float_sec_to_tuple(result.st_birthtime)
        else: # pragma: no cover
            crtime, crtime_ns = mtime, mtime_ns

        return cls(filetype, result.st_size, None, uid, gid, owner, group,
                   mode, atime, atime_ns, crtime, crtime_ns, mtime, mtime_ns,
                   ctime, ctime_ns, None, None, None, None, None,
                   result.st_nlink, None)