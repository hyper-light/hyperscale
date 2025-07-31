from typing import Generic, AnyStr, TYPE_CHECKING, Callable
from .packet import String, UInt32
from .ssh_forward_channel import SSHForwardChannel


if TYPE_CHECKING:
    from .ssh_unix_session import SSHUNIXSession


SSHUNIXSessionFactory = Callable[[], SSHUNIXSession[AnyStr]]

class SSHUNIXChannel(SSHForwardChannel, Generic[AnyStr]):
    """SSH UNIX channel"""

    async def _open_unix(self, session_factory: SSHUNIXSessionFactory[AnyStr],
                         chantype: bytes, path: str,
                          *args: bytes) -> SSHUNIXSession[AnyStr]:
        """Open a UNIX channel"""

        self.set_extra_info(local_peername='', remote_peername=path)

        return await self._open_forward(session_factory, chantype,
                                             String(path), *args)

    async def connect(self, session_factory: SSHUNIXSessionFactory[AnyStr],
                      path: str) -> SSHUNIXSession[AnyStr]:
        """Create a new outbound UNIX session"""

        # OpenSSH appears to have a bug which requires an originator
        # host and port to be sent after the path name to connect to
        # when opening a direct streamlocal channel.
        return await self._open_unix(session_factory,
                                     b'direct-streamlocal@openssh.com',
                                     path, String(''), UInt32(0))

    async def accept(self, session_factory: SSHUNIXSessionFactory[AnyStr],
                     path: str) -> SSHUNIXSession[AnyStr]:
        """Create a new forwarded UNIX session"""

        return await self._open_unix(session_factory,
                                     b'forwarded-streamlocal@openssh.com',
                                     path, String(''))

    def set_inbound_peer_names(self, dest_path: str) -> None:
        """Set local and remote peer names for inbound connections"""

        self.set_extra_info(local_peername=dest_path, remote_peername='')