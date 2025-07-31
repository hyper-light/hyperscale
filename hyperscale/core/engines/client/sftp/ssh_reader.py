from typing import Generic, AnyStr, AsyncIterator, Any
from .ssh_channel import SSHChannel
from .ssh_stream_session import SSHStreamSession
from .types import DataType


class SSHReader(Generic[AnyStr]):
    """SSH read stream handler"""

    def __init__(
        self,
        session: SSHStreamSession[AnyStr],
        chan: SSHChannel[AnyStr],
        datatype: DataType = None,
    ):
        self._session: SSHStreamSession[AnyStr] = session
        self._chan: SSHChannel[AnyStr] = chan
        self._datatype = datatype

    async def __aiter__(self) -> AsyncIterator[AnyStr]:
        """Allow SSHReader to be an async iterator"""

        async for result in self._session.aiter(self._datatype):
            yield result

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

    def feed_data(self, data: AnyStr) -> None:
        """Feed data to the associated session

           This method feeds data to the SSH session associated with
           this stream, providing compatibility with the
           :meth:`feed_data() <asyncio.StreamReader.feed_data>` method
           on :class:`asyncio.StreamReader`. This is mostly useful
           for testing.

        """

        self._session.data_received(data, self._datatype)

    def feed_eof(self) -> None:
        """Feed EOF to the associated session

           This method feeds an end-of-file indication to the SSH session
           associated with this stream, providing compatibility with the
           :meth:`feed_eof() <asyncio.StreamReader.feed_data>` method
           on :class:`asyncio.StreamReader`. This is mostly useful
           for testing.

        """

        self._session.eof_received()

    async def read(self, n: int = -1) -> AnyStr:
        """Read data from the stream

           This method is a coroutine which reads up to `n` bytes
           or characters from the stream. If `n` is not provided or
           set to `-1`, it reads until EOF or a signal is received.

           If EOF is received and the receive buffer is empty, an
           empty `bytes` or `str` object is returned.

           If the next data in the stream is a signal, the signal is
           delivered as a raised exception.

           .. note:: Unlike traditional `asyncio` stream readers,
                     the data will be delivered as either `bytes` or
                     a `str` depending on whether an encoding was
                     specified when the underlying channel was opened.

        """

        return await self._session.read(self._datatype, n, exact=False)

    async def readline(self) -> AnyStr:
        """Read one line from the stream

           This method is a coroutine which reads one line, ending in
           `'\\n'`.

           If EOF is received before `'\\n'` is found, the partial
           line is returned. If EOF is received and the receive buffer
           is empty, an empty `bytes` or `str` object is returned.

           If the next data in the stream is a signal, the signal is
           delivered as a raised exception.

           .. note:: In Python 3.5 and later, :class:`SSHReader` objects
                     can also be used as async iterators, returning input
                     data one line at a time.

        """

        return await self._session.readline(self._datatype)

    async def readuntil(self, separator: object,
                        max_separator_len = 0) -> AnyStr:
        """Read data from the stream until `separator` is seen

           This method is a coroutine which reads from the stream until
           the requested separator is seen. If a match is found, the
           returned data will include the separator at the end.

           The `separator` argument can be a single `bytes` or `str`
           value, a sequence of multiple `bytes` or `str` values,
           or a compiled regex (`re.Pattern`) to match against,
           returning data as soon as a matching separator is found
           in the stream.

           When passing a regex pattern as the separator, the
           `max_separator_len` argument should be set to the
           maximum length of an expected separator match. This
           can greatly improve performance, by minimizing how far
           back into the stream must be searched for a match.
           When passing literal separators to match against, the
           max separator length will be set automatically.

           .. note:: For best results, a separator regex should
                     both begin and end with data which is as
                     unique as possible, and should not start or
                     end with optional or repeated elements.
                     Otherwise, you run the risk of failing to
                     match parts of a separator when it is split
                     across multiple reads.

           If EOF or a signal is received before a match occurs, an
           :exc:`IncompleteReadError <asyncio.IncompleteReadError>`
           is raised and its `partial` attribute will contain the
           data in the stream prior to the EOF or signal.

           If the next data in the stream is a signal, the signal is
           delivered as a raised exception.

        """

        return await self._session.readuntil(separator, self._datatype,
                                             max_separator_len)

    async def readexactly(self, n: int) -> AnyStr:
        """Read an exact amount of data from the stream

           This method is a coroutine which reads exactly n bytes or
           characters from the stream.

           If EOF or a signal is received in the stream before `n`
           bytes are read, an :exc:`IncompleteReadError
           <asyncio.IncompleteReadError>` is raised and its `partial`
           attribute will contain the data before the EOF or signal.

           If the next data in the stream is a signal, the signal is
           delivered as a raised exception.

        """

        return await self._session.read(self._datatype, n, exact=True)

    def at_eof(self) -> bool:
        """Return whether the stream is at EOF

           This method returns `True` when EOF has been received and
           all data in the stream has been read.

        """

        return self._session.at_eof(self._datatype)

    def get_redirect_info(self) -> tuple[SSHStreamSession[AnyStr], DataType]:
        """Get information needed to redirect from this SSHReader"""

        return self._session, self._datatype

