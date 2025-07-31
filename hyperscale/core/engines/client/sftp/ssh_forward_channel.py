from typing import Generic, AnyStr, TYPE_CHECKING, Callable
from .ssh_channel import SSHChannel
from .types import MaybeAwait


if TYPE_CHECKING:
    from .ssh_session import SSHSession


SSHSessionFactory = Callable[[], SSHSession[AnyStr]]


class SSHForwardChannel(SSHChannel, Generic[AnyStr]):
    """SSH channel for forwarding TCP and UNIX domain connections"""

    async def _finish_open_request(
            self, result: MaybeAwait[SSHSession[AnyStr]]) -> None:
        """Finish processing a forward channel open request"""

        await super()._finish_open_request(result)

        if self._session is not None:
            self._session.session_started()
            self.resume_reading()

    async def _open_forward(self, session_factory: SSHSessionFactory[AnyStr],
                            chantype: bytes, *args: bytes) -> \
            SSHSession[AnyStr]:
        """Open a forward channel"""

        packet = await super()._open(chantype, *args)

        # Forward channels should have no extra data in the open confirmation
        packet.check_end()

        self._session = session_factory()
        self._session.connection_made(self)
        self._session.session_started()

        assert self._conn is not None
        self._conn.create_task(self._start_reading())

        return self._session