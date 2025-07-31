from typing import TYPE_CHECKING
from .config import SFTPLimits
from .constants import (
    FXP_ATTRS,
    FXP_WRITE,
    FXP_DATA,
    FXP_OPEN,
    FXP_READ,
    FXP_HANDLE,
    FXP_LSTAT,
    FXP_FSTAT,
    FXP_OPENDIR,
    FXP_READDIR,
    FXP_REALPATH,
    FXP_NAME,
    FXP_STAT,
    FXP_READLINK,
    FXP_EXTENDED_REPLY,
    SAFE_SFTP_READ_LEN,
    SAFE_SFTP_WRITE_LEN,
)
from .error import (
    SFTPNoConnection,
    SFTPConnectionLost,
    SFTPBadMessage,
    Error,
    PacketDecodeError,
)
from .packet import SSHPacket, Byte, UInt32

if TYPE_CHECKING:
    from .ssh_reader import SSHReader
    from .ssh_writer import SSHWriter


class SFTPHandler:
    """SFTP session handler"""

    _data_pkttypes = {FXP_WRITE, FXP_DATA}

    _handler_names = {
        1: "FXP_INIT",
        2: "FXP_VERSION",
        3: "FXP_OPEN",
        4: "FXP_CLOSE",
        5: "FXP_READ",
        6: "FXP_WRITE",
        7: "FXP_LSTAT",
        8: "FXP_FSTAT",
        9: "FXP_SETSTAT",
        10: "FXP_FSETSTAT",
        11: "FXP_OPENDIR",
        12: "FXP_READDIR",
        13: "FXP_REMOVE",
        14: "FXP_MKDIR",
        15: "FXP_RMDIR",
        16: "FXP_REALPATH",
        17: "FXP_STAT",
        18: "FXP_RENAME",
        19: "FXP_READLINK",
        20: "FXP_SYMLINK",
        21: "FXP_LINK",
        22: "FXP_BLOCK",
        23: "FXP_UNBLOCK",
        101: "FXP_STATUS",
        102: "FXP_HANDLE",
        103: "FXP_DATA",
        104: "FXP_NAME",
        105: "FXP_ATTRS",
        200: "FXP_EXTENDED",
        201: "FXP_EXTENDED_REPLY"
    }
    _realpath_check_names = {
        1: "NO_CHECK",
        2: "STAT_IF_EXISTS",
        3: "STAT_ALWAYS",
    }

    # SFTP implementations with broken order for SYMLINK arguments
    _nonstandard_symlink_impls = ['OpenSSH', 'paramiko']

    # Return types by message -- unlisted entries always return FXP_STATUS,
    #                            those below return FXP_STATUS on error
    _return_types = {
        FXP_OPEN:                 FXP_HANDLE,
        FXP_READ:                 FXP_DATA,
        FXP_LSTAT:                FXP_ATTRS,
        FXP_FSTAT:                FXP_ATTRS,
        FXP_OPENDIR:              FXP_HANDLE,
        FXP_READDIR:              FXP_NAME,
        FXP_REALPATH:             FXP_NAME,
        FXP_STAT:                 FXP_ATTRS,
        FXP_READLINK:             FXP_NAME,
        b'statvfs@openssh.com':   FXP_EXTENDED_REPLY,
        b'fstatvfs@openssh.com':  FXP_EXTENDED_REPLY,
        b'limits@openssh.com':    FXP_EXTENDED_REPLY,
        b'ranges@asyncssh.com':   FXP_EXTENDED_REPLY
    }

    def __init__(
        self,
        reader: SSHReader[bytes],
        writer: SSHWriter[bytes],
    ):
        self._reader: SSHReader[bytes] | None = reader
        self._writer: SSHWriter[bytes] | None = writer

        self.limits = SFTPLimits(0, SAFE_SFTP_READ_LEN, SAFE_SFTP_WRITE_LEN, 0)

    async def _cleanup(self, exc: Exception | None) -> None:
        """Clean up this SFTP session"""

        # pylint: disable=unused-argument

        if self._writer: # pragma: no branch
            self._writer.close()
            self._reader = None
            self._writer = None

    async def _process_packet(self, pkttype: int, pktid: int,
                              packet: SSHPacket) -> None:
        """Abstract method for processing SFTP packets"""

        raise NotImplementedError

    def send_packet(self, pkttype: int, pktid: int | None,
                    *args: bytes) -> None:
        """Send an SFTP packet"""

        if not self._writer:
            raise SFTPNoConnection('Connection not open')

        payload = Byte(pkttype) + b''.join(args)

        try:
            self._writer.write(UInt32(len(payload)) + payload)
        except ConnectionError as exc:
            raise SFTPConnectionLost(str(exc)) from None

    async def recv_packet(self) -> SSHPacket:
        """Receive an SFTP packet"""

        assert self._reader is not None

        pktlen = await self._reader.readexactly(4)
        pktlen = int.from_bytes(pktlen, 'big')

        packet = await self._reader.readexactly(pktlen)
        return SSHPacket(packet)

    async def recv_packets(self) -> None:
        """Receive and process SFTP packets"""

        try:
            while self._reader: # pragma: no branch
                packet = await self.recv_packet()

                pkttype = packet.get_byte()
                pktid = packet.get_uint32()

                await self._process_packet(pkttype, pktid, packet)
        except PacketDecodeError as exc:
            await self._cleanup(SFTPBadMessage(str(exc)))
        except EOFError:
            await self._cleanup(None)
        except (OSError, Error) as exc:
            await self._cleanup(exc)