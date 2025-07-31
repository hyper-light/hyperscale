import asyncio
from typing import Sequence, TYPE_CHECKING
from .config import SFTPName, SFTPLimits, SFTPRanges
from .constants import (
    FXP_HANDLE,
    FXP_FSTAT,
    FXP_ATTRS,
    FXP_BLOCK,
    FXP_CLOSE,
    FXP_DATA,
    FXP_EXTENDED,
    FXP_EXTENDED_REPLY,
    FXP_FSETSTAT,
    FXP_INIT,
    FXP_LINK,
    FXP_LSTAT,
    FXP_MKDIR,
    FXP_NAME,
    FXP_OPEN,
    FXP_OPENDIR,
    FXP_READ,
    FXP_READDIR,
    FXP_READLINK,
    FXP_REALPATH,
    FXP_REMOVE,
    FXP_RENAME,
    FXP_RMDIR,
    FXP_SETSTAT,
    FXP_STAT,
    FXP_STATUS,
    FXP_SYMLINK,
    FXP_UNBLOCK,
    FXP_VERSION,
    FXP_WRITE,
    FXRP_NO_CHECK,
    FXR_OVERWRITE,
    MAX_SFTP_VERSION,
    MIN_SFTP_VERSION,
)
from .error import (
    SFTPConnectionLost,
    SFTPError,
    SFTPOpUnsupported,
    PacketDecodeError,
    Error,
    ConnectionLost,
)
from .packet import (
    SSHPacket,
    UInt32,
    UInt64,
    Byte,
    String,
    Boolean,
)
from .pflags import pflags_to_flags
from .sftp_attrs import SFTPAttrs
from .sftp_handler import SFTPHandler, SFTPBadMessage
from .sftpvfs_attrs import SFTPVFSAttrs

if TYPE_CHECKING:
    from .ssh_reader import SSHReader
    from .ssh_writer import SSHWriter


SFTPNames = tuple[Sequence[SFTPName], bool]
RequestWaiter = asyncio.Future[tuple[int, SSHPacket]]


class SFTPClientHandler(SFTPHandler):
    """An SFTP client session handler"""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        reader: SSHReader[bytes],
        writer: SSHWriter[bytes],
        sftp_version: int,
    ):
        super().__init__(reader, writer)

        self._loop = loop
        self._version = sftp_version
        self._next_pktid = 0
        self._requests: dict[int, RequestWaiter] = {}
        self._nonstandard_symlink = False
        self._supports_posix_rename = False
        self._supports_statvfs = False
        self._supports_fstatvfs = False
        self._supports_hardlink = False
        self._supports_fsync = False
        self._supports_lsetstat = False
        self._supports_limits = False
        self._supports_copy_data = False
        self._supports_ranges = False

    @property
    def version(self) -> int:
        """SFTP version associated with this SFTP session"""

        return self._version

    @property
    def supports_copy_data(self) -> bool:
        """Return whether or not SFTP remote copy is supported"""

        return self._supports_copy_data

    async def _cleanup(self, exc: Exception| None) -> None:
        """Clean up this SFTP client session"""

        req_exc = exc or SFTPConnectionLost('Connection closed')

        for waiter in list(self._requests.values()):
            if not waiter.cancelled(): # pragma: no branch
                waiter.set_exception(req_exc)

        self._requests = {}

        await super()._cleanup(exc)

    async def _process_packet(self, pkttype: int, pktid: int,
                              packet: SSHPacket) -> None:
        """Process incoming SFTP responses"""

        try:
            waiter = self._requests.pop(pktid)
        except KeyError:
            await self._cleanup(SFTPBadMessage('Invalid response id'))
        else:
            if not waiter.cancelled(): # pragma: no branch
                waiter.set_result((pkttype, packet))

    def _send_request(self, pkttype: int | bytes, args: Sequence[bytes],
                      waiter: RequestWaiter) -> None:
        """Send an SFTP request"""

        pktid = self._next_pktid
        self._next_pktid = (self._next_pktid + 1) & 0xffffffff

        self._requests[pktid] = waiter

        if isinstance(pkttype, bytes):
            hdr = UInt32(pktid) + String(pkttype)
            pkttype = FXP_EXTENDED
        else:
            hdr = UInt32(pktid)

        self.send_packet(pkttype, pktid, hdr, *args)

    async def _make_request(self, pkttype: int | bytes,
                            *args: bytes) -> object:
        """Make an SFTP request and wait for a response"""

        waiter: RequestWaiter = self._loop.create_future()
        self._send_request(pkttype, args, waiter)
        resptype, resp = await waiter

        return_type = self._return_types.get(pkttype)

        if resptype not in (FXP_STATUS, return_type):
            raise SFTPBadMessage(f'Unexpected response type: {resptype}')

        result = self._packet_handlers[resptype](self, resp)

        if result is not None or return_type is None:
            return result
        else:
            raise SFTPBadMessage('Unexpected FX_OK response')

    def _process_status(self, packet: SSHPacket) -> None:
        """Process an incoming SFTP status response"""

        exc = SFTPError.construct(packet)

        if self._version < 6:
            packet.check_end()

        if exc:
            raise exc

    def _process_handle(self, packet: SSHPacket) -> bytes:
        """Process an incoming SFTP handle response"""

        handle = packet.get_string()

        if self._version < 6:
            packet.check_end()

        return handle

    def _process_data(self, packet: SSHPacket) -> tuple[bytes, bool]:
        """Process an incoming SFTP data response"""

        data = packet.get_string()
        at_end = packet.get_boolean() if packet and self._version >= 6 \
            else False

        if self._version < 6:
            packet.check_end()

        return data, at_end

    def _process_name(self, packet: SSHPacket) -> SFTPNames:
        """Process an incoming SFTP name response"""

        count = packet.get_uint32()
        names = [SFTPName.decode(packet, self._version) for _ in range(count)]
        at_end = packet.get_boolean() if packet and self._version >= 6 \
            else False

        if self._version < 6:
            packet.check_end()

        return names, at_end

    def _process_attrs(self, packet: SSHPacket) -> SFTPAttrs:
        """Process an incoming SFTP attributes response"""

        attrs = SFTPAttrs().decode(packet, self._version)

        if self._version < 6:
            packet.check_end()

        return attrs

    def _process_extended_reply(self, packet: SSHPacket) -> SSHPacket:
        """Process an incoming SFTP extended reply response"""

        # pylint: disable=no-self-use

        # Let the caller do the decoding for extended replies
        return packet

    _packet_handlers = {
        FXP_STATUS:         _process_status,
        FXP_HANDLE:         _process_handle,
        FXP_DATA:           _process_data,
        FXP_NAME:           _process_name,
        FXP_ATTRS:          _process_attrs,
        FXP_EXTENDED_REPLY: _process_extended_reply
    }

    async def start(self) -> None:
        """Start an SFTP client"""

        assert self._reader is not None

        self.send_packet(FXP_INIT, None, UInt32(self._version))

        try:
            resp = await self.recv_packet()

            resptype = resp.get_byte()

            if resptype != FXP_VERSION:
                raise SFTPBadMessage('Expected version message')

            version = resp.get_uint32()

            if not MIN_SFTP_VERSION <= version <= MAX_SFTP_VERSION:
                raise SFTPBadMessage(f'Unsupported version: {version}')

            rcvd_extensions: list[tuple[bytes, bytes]] = []

            while resp:
                name = resp.get_string()
                data = resp.get_string()
                rcvd_extensions.append((name, data))
        except PacketDecodeError as exc:
            raise SFTPBadMessage(str(exc)) from None
        except SFTPError:
            raise
        except ConnectionLost as exc:
            raise SFTPConnectionLost(str(exc)) from None
        except (asyncio.IncompleteReadError, Error) as exc:
            raise SFTPConnectionLost(str(exc)) from None

        self._version = version

        for name, data in rcvd_extensions:
            if name == b'posix-rename@openssh.com' and data == b'1':
                self._supports_posix_rename = True
            elif name == b'statvfs@openssh.com' and data == b'2':
                self._supports_statvfs = True
            elif name == b'fstatvfs@openssh.com' and data == b'2':
                self._supports_fstatvfs = True
            elif name == b'hardlink@openssh.com' and data == b'1':
                self._supports_hardlink = True
            elif name == b'fsync@openssh.com' and data == b'1':
                self._supports_fsync = True
            elif name == b'lsetstat@openssh.com' and data == b'1':
                self._supports_lsetstat = True
            elif name == b'limits@openssh.com' and data == b'1':
                self._supports_limits = True
            elif name == b'copy-data' and data == b'1':
                self._supports_copy_data = True
            elif name == b'ranges@asyncssh.com' and data == b'1':
                self._supports_ranges = True

        if version == 3:
            # Check if the server has a buggy SYMLINK implementation

            server_version: str = self._reader.get_extra_info('server_version', '')

            if any(
                name in server_version
                for name in self._nonstandard_symlink_impls
            ):
                self._nonstandard_symlink = True

    async def request_limits(self) -> None:
        """Request SFTP server limits"""

        if self._supports_limits:
            packet: SSHPacket = await self._make_request(
                b'limits@openssh.com')

            limits = SFTPLimits.decode(packet)
            packet.check_end()

            if limits.max_read_len:
                self.limits.max_read_len = limits.max_read_len

            if limits.max_write_len:
                self.limits.max_write_len = limits.max_write_len

    async def open(self, filename: bytes, pflags: int,
                   attrs: SFTPAttrs) -> bytes:
        """Make an SFTP open request"""

        if self._version >= 5:
            desired_access, flags = pflags_to_flags(pflags)

            return await self._make_request(
                FXP_OPEN, String(filename), UInt32(desired_access),
                UInt32(flags), attrs.encode(self._version))
        else:

            return await self._make_request(
                FXP_OPEN, String(filename), UInt32(pflags),
                attrs.encode(self._version))

    async def open56(self, filename: bytes, desired_access: int,
                     flags: int, attrs: SFTPAttrs) -> bytes:
        """Make an SFTPv5/v6 open request"""

        if self._version >= 5:
            return await self._make_request(
                FXP_OPEN, String(filename), UInt32(desired_access),
                UInt32(flags), attrs.encode(self._version))
        else:
            raise SFTPOpUnsupported('SFTPv5/v6 open not supported by server')

    async def close(self, handle: bytes) -> None:
        """Make an SFTP close request"""

        if self._writer:
            await self._make_request(FXP_CLOSE, String(handle))

    async def read(self, handle: bytes, offset: int,
                   length: int) -> tuple[bytes, bool]:
        """Make an SFTP read request"""

        return await self._make_request(
            FXP_READ, String(handle), UInt64(offset), UInt32(length))

    async def write(self, handle: bytes, offset: int, data: bytes) -> int:
        """Make an SFTP write request"""


        return await self._make_request(
            FXP_WRITE, String(handle), UInt64(offset), String(data))

    async def stat(self, path: bytes, flags: int, *,
                   follow_symlinks: bool = True) -> SFTPAttrs:
        """Make an SFTP stat or lstat request"""

        if self._version >= 4:
            flag_bytes = UInt32(flags)
            flag_text = f', flags 0x{flags:08x}'
        else:
            flag_bytes = b''
            flag_text = ''

        if follow_symlinks:

            return await self._make_request(
                FXP_STAT, String(path), flag_bytes)
        else:

            return await self._make_request(
                FXP_LSTAT, String(path), flag_bytes)

    async def lstat(self, path: bytes, flags: int) -> SFTPAttrs:
        """Make an SFTP lstat request"""

        if self._version >= 4:
            flag_bytes = UInt32(flags)

        else:
            flag_bytes = b''

        return await self._make_request(
            FXP_LSTAT, String(path), flag_bytes)

    async def fstat(self, handle: bytes, flags: int) -> SFTPAttrs:
        """Make an SFTP fstat request"""

        if self._version >= 4:
            flag_bytes = UInt32(flags)
            flag_text = f', flags 0x{flags:08x}'
        else:
            flag_bytes = b''
            flag_text = ''


        return await self._make_request(
            FXP_FSTAT, String(handle), flag_bytes)

    async def setstat(self, path: bytes, attrs: SFTPAttrs, *,
                      follow_symlinks: bool = True) -> None:
        """Make an SFTP setstat or lsetstat request"""

        if follow_symlinks:
            await self._make_request(FXP_SETSTAT, String(path),
                                     attrs.encode(self._version))
        elif self._supports_lsetstat:
            await self._make_request(b'lsetstat@openssh.com', String(path),
                                     attrs.encode(self._version))
        else:
            raise SFTPOpUnsupported('lsetstat not supported by server')

    async def fsetstat(self, handle: bytes, attrs: SFTPAttrs) -> None:
        """Make an SFTP fsetstat request"""

        await self._make_request(FXP_FSETSTAT, String(handle),
                                 attrs.encode(self._version))

    async def statvfs(self, path: bytes) -> SFTPVFSAttrs:
        """Make an SFTP statvfs request"""

        if self._supports_statvfs:

            packet: SSHPacket = await self._make_request(
                b'statvfs@openssh.com', String(path))

            vfsattrs = SFTPVFSAttrs.decode(packet, self._version)
            packet.check_end()

            return vfsattrs
        else:
            raise SFTPOpUnsupported('statvfs not supported')

    async def fstatvfs(self, handle: bytes) -> SFTPVFSAttrs:
        """Make an SFTP fstatvfs request"""

        if self._supports_fstatvfs:

            packet: SSHPacket = await self._make_request(
                b'fstatvfs@openssh.com', String(handle))

            vfsattrs = SFTPVFSAttrs.decode(packet, self._version)
            packet.check_end()

            return vfsattrs
        else:
            raise SFTPOpUnsupported('fstatvfs not supported')

    async def remove(self, path: bytes) -> None:
        """Make an SFTP remove request"""
        await self._make_request(FXP_REMOVE, String(path))

    async def rename(self, oldpath: bytes, newpath: bytes, flags: int) -> None:
        """Make an SFTP rename request"""

        if self._version >= 5:
            await self._make_request(FXP_RENAME, String(oldpath),
                                     String(newpath), UInt32(flags))
        elif flags and self._supports_posix_rename:
            await self._make_request(b'posix-rename@openssh.com',
                                     String(oldpath), String(newpath))
        elif not flags:
            await self._make_request(FXP_RENAME, String(oldpath),
                                     String(newpath))
        else:
            raise SFTPOpUnsupported('Rename with overwrite not supported')

    async def posix_rename(self, oldpath: bytes, newpath: bytes) -> None:
        """Make an SFTP POSIX rename request"""

        if self._supports_posix_rename:
            await self._make_request(b'posix-rename@openssh.com',
                                     String(oldpath), String(newpath))
        elif self._version >= 5:
            await self._make_request(FXP_RENAME, String(oldpath),
                                     String(newpath), UInt32(FXR_OVERWRITE))
        else:
            raise SFTPOpUnsupported('POSIX rename not supported')

    async def opendir(self, path: bytes) -> bytes:
        """Make an SFTP opendir request"""
        return await self._make_request(
            FXP_OPENDIR, String(path))

    async def readdir(self, handle: bytes) -> SFTPNames:
        """Make an SFTP readdir request"""
        return  await self._make_request(
            FXP_READDIR, String(handle))

    async def mkdir(self, path: bytes, attrs: SFTPAttrs) -> None:
        """Make an SFTP mkdir request"""
        await self._make_request(FXP_MKDIR, String(path),
                                 attrs.encode(self._version))

    async def rmdir(self, path: bytes) -> None:
        """Make an SFTP rmdir request"""
        await self._make_request(FXP_RMDIR, String(path))

    async def realpath(self, path: bytes, *compose_paths: bytes,
                       check: int = FXRP_NO_CHECK) -> SFTPNames:
        """Make an SFTP realpath request"""

        if self._version >= 6:
            return await self._make_request(
                FXP_REALPATH, String(path), Byte(check),
                *map(String, compose_paths))
        else:
            return await self._make_request(
                FXP_REALPATH, String(path))

    async def readlink(self, path: bytes) -> SFTPNames:
        """Make an SFTP readlink request"""
        return await self._make_request(
            FXP_READLINK, String(path))

    async def symlink(self, oldpath: bytes, newpath: bytes) -> None:
        """Make an SFTP symlink request"""
        if self._version >= 6:
            await self._make_request(FXP_LINK, String(newpath),
                                     String(oldpath), Boolean(True))
        else:
            if self._nonstandard_symlink:
                args = String(oldpath) + String(newpath)
            else:
                args = String(newpath) + String(oldpath)

            await self._make_request(FXP_SYMLINK, args)

    async def link(self, oldpath: bytes, newpath: bytes) -> None:
        """Make an SFTP hard link request"""

        if self._version >= 6 or self._supports_hardlink:
            if self._version >= 6:
                await self._make_request(FXP_LINK, String(newpath),
                                         String(oldpath), Boolean(False))
            else:
                await self._make_request(b'hardlink@openssh.com',
                                         String(oldpath), String(newpath))
        else:
            raise SFTPOpUnsupported('link not supported')

    async def lock(self, handle: bytes, offset: int, length: int,
                   flags: int) -> None:
        """Make an SFTP byte range lock request"""

        if self._version >= 6:
            await self._make_request(FXP_BLOCK, String(handle),
                                     UInt64(offset), UInt64(length),
                                     UInt32(flags))
        else:
            raise SFTPOpUnsupported('Byte range locks not supported')

    async def unlock(self, handle: bytes, offset: int, length: int) -> None:
        """Make an SFTP byte range unlock request"""

        if self._version >= 6:
            await self._make_request(FXP_UNBLOCK, String(handle),
                                     UInt64(offset), UInt64(length))
        else:
            raise SFTPOpUnsupported('Byte range locks not supported')

    async def fsync(self, handle: bytes) -> None:
        """Make an SFTP fsync request"""

        if self._supports_fsync:
            await self._make_request(b'fsync@openssh.com', String(handle))
        else:
            raise SFTPOpUnsupported('fsync not supported')

    async def copy_data(self, read_from_handle: bytes, read_from_offset: int,
                        read_from_length: int, write_to_handle: bytes,
                        write_to_offset: int) -> None:
        """Make an SFTP copy data request"""

        if self._supports_copy_data:
            await self._make_request(b'copy-data', String(read_from_handle),
                                     UInt64(read_from_offset),
                                     UInt64(read_from_length),
                                     String(write_to_handle),
                                     UInt64(write_to_offset))
        else:
            raise SFTPOpUnsupported('copy-data not supported')

    async def request_ranges(self, handle: bytes, offset: int,
                             length: int) -> SFTPRanges:
        """Request file ranges containing data in a remote file"""

        if self._supports_ranges:
            packet: SSHPacket = await self._make_request(
                b'ranges@asyncssh.com', String(handle),
                UInt64(offset), UInt64(length))

            result = SFTPRanges.decode(packet)
            packet.check_end()

            return result
        else:
            return SFTPRanges([(offset, length)], True)

    def exit(self) -> None:
        """Handle a request to close the SFTP session"""

        if self._writer:
            self._writer.write_eof()

    async def wait_closed(self) -> None:
        """Wait for this SFTP session to close"""

        if self._writer:
            await self._writer.channel.wait_closed()

