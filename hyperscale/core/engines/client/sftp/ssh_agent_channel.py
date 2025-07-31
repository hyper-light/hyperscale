from typing import TYPE_CHECKING
from .ssh_forward_channel import SSHForwardChannel

if TYPE_CHECKING:
    from .ssh_unix_channel import SSHUNIXSession, SSHUNIXSessionFactory



class SSHAgentChannel(SSHForwardChannel[bytes]):
    """SSH agent channel"""

    async def open(self, session_factory: SSHUNIXSessionFactory[bytes]) -> \
            SSHUNIXSession[bytes]:
        """Open an SSH agent channel"""

        return await self._open_forward(session_factory,
                                             b'auth-agent@openssh.com')
