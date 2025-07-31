from typing import Generic, AnyStr, Any, Iterable
from .ssh_channel import SSHChannel
from .ssh_stream_session import SSHStreamSession
from .types import DataType


class SSHWriter(Generic[AnyStr]):
    """SSH write stream handler"""

    def __init__(
        self,
        session: SSHStreamSession[AnyStr],
        chan: SSHChannel[AnyStr],
        datatype: DataType = None):
        self._session: SSHStreamSession[AnyStr] = session
        self._chan: SSHChannel[AnyStr] = chan
        self._datatype = datatype

    @property
    def channel(self) -> SSHChannel[AnyStr]:
        """The SSH channel associated with this stream"""

        return self._chan


    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Return additional information about this stream

           This method returns extra information about the channel
           associated with this stream. See :meth:`get_extra_info()
           <SSHClientChannel.get_extra_info>` on :class:`SSHClientChannel`
           for additional information.

        """

        return self._chan.get_extra_info(name, default)

    def can_write_eof(self) -> bool:
        """Return whether the stream supports :meth:`write_eof`"""

        return self._chan.can_write_eof()

    def close(self) -> None:
        """Close the channel

           .. note:: After this is called, no data can be read or written
                     from any of the streams associated with this channel.

        """

        return self._chan.close()

    def is_closing(self) -> bool:
        """Return if the stream is closing or is closed"""

        return self._chan.is_closing()

    async def wait_closed(self) -> None:
        """Wait until the stream is closed

           This should be called after :meth:`close` to wait until
           the underlying connection is closed.

        """

        await self._chan.wait_closed()

    async def drain(self) -> None:
        """Wait until the write buffer on the channel is flushed

           This method is a coroutine which blocks the caller if the
           stream is currently paused for writing, returning when
           enough data has been sent on the channel to allow writing
           to resume. This can be used to avoid buffering an excessive
           amount of data in the channel's send buffer.

        """

        await self._session.drain(self._datatype)

    def write(self, data: AnyStr) -> None:
        """Write data to the stream

           This method writes bytes or characters to the stream.

           .. note:: Unlike traditional `asyncio` stream writers,
                     the data must be supplied as either `bytes` or
                     a `str` depending on whether an encoding was
                     specified when the underlying channel was opened.

        """

        return self._chan.write(data, self._datatype)

    def writelines(self, list_of_data: Iterable[AnyStr]) -> None:
        """Write a collection of data to the stream"""

        return self._chan.writelines(list_of_data, self._datatype)

    def write_eof(self) -> None:
        """Write EOF on the channel

           This method sends an end-of-file indication on the channel,
           after which no more data can be written.

           .. note:: On an :class:`SSHServerChannel` where multiple
                     output streams are created, writing EOF on one
                     stream signals EOF for all of them, since it
                     applies to the channel as a whole.

        """

        return self._chan.write_eof()

    def get_redirect_info(self) -> tuple[SSHStreamSession[AnyStr], DataType]:
        """Get information needed to redirect to this SSHWriter"""

        return self._session, self._datatype
