import asyncio
from typing import TYPE_CHECKING, Callable
from .packet import UInt32
from .constants import (
    SSH_TUN_MODE_POINTTOPOINT,
    SSH_TUN_AF_INET,
    SSH_TUN_AF_INET6,
    SSH_TUN_UNIT_ANY,
)
from .ssh_forward_channel import SSHForwardChannel
from .types import DataType


if TYPE_CHECKING:
    from .ssh_connection import SSHConnection
    from .ssh_tun_tap_session import SSHTunTapSession



SSHTunTapSessionFactory = Callable[[], SSHTunTapSession]


class SSHTunTapChannel(SSHForwardChannel[bytes]):
    """SSH TunTap channel"""

    def __init__(self, conn: SSHConnection,
                 loop: asyncio.AbstractEventLoop, encoding: str | None,
                 errors: str, window: int, max_pktsize: int):
        super().__init__(conn, loop, encoding, errors, window, max_pktsize)

        self._mode: int | None = None

    def _accept_data(self, data: bytes, datatype: DataType = None) -> None:
        """Strip off address family on incoming packets in TUN mode"""

        if self._mode == SSH_TUN_MODE_POINTTOPOINT:
            data = data[4:]

        super()._accept_data(data, datatype)

    def write(self, data: bytes, datatype: DataType = None) -> None:
        """Add address family in outbound packets in TUN mode"""

        if self._mode == SSH_TUN_MODE_POINTTOPOINT:
            version = data[0] >> 4
            family = SSH_TUN_AF_INET if version == 4 else SSH_TUN_AF_INET6
            data = UInt32(family) + data

        super().write(data, datatype)

    async def open(self, session_factory: SSHTunTapSessionFactory,
                   mode: int, unit: int | None) -> SSHTunTapSession:
        """Open a TUN/TAP channel"""

        self._mode = mode

        if unit is None:
            unit = SSH_TUN_UNIT_ANY

        return  await self._open_forward(session_factory,
                                             b'tun@openssh.com',
                                             UInt32(mode), UInt32(unit))

    def set_mode(self, mode: int) -> None:
        """Set mode for inbound connections"""

        self._mode = mode

