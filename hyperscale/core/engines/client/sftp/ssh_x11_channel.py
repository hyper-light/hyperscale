from typing import TYPE_CHECKING
from .packet import String, UInt32
from .ssh_forward_channel import SSHForwardChannel


if TYPE_CHECKING:
    from .ssh_tcp_channel import SSHTCPSession, SSHTCPSessionFactory


class SSHX11Channel(SSHForwardChannel[bytes]):
    """SSH X11 channel"""

    async def open(self, session_factory: SSHTCPSessionFactory[bytes],
                   orig_host: str, orig_port: int) -> SSHTCPSession[bytes]:
        """Open an SSH X11 channel"""

        self.set_extra_info(local_peername=(orig_host, orig_port),
                            remote_peername=('', 0))

        return await self._open_forward(session_factory, b'x11',
                                             String(orig_host),
                                             UInt32(orig_port))

    def set_inbound_peer_names(self, orig_host: str, orig_port: int) -> None:
        """Set local and remote peer name for inbound connections"""

        self.set_extra_info(local_peername=('', 0),
                            remote_peername=(orig_host, orig_port))

