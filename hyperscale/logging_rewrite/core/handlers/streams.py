import asyncio
import sys
from asyncio import StreamWriter
from typing import Union, Optional

from hyperscale.logging_rewrite.core.utils import get_running_loop
from hyperscale.logging_rewrite.core.filters import Filter
from hyperscale.logging_rewrite.core.formatters.base import Formatter
from hyperscale.logging_rewrite.core.handlers.base import Handler
from hyperscale.logging_rewrite.core.levels import LogLevel
from hyperscale.logging_rewrite.core.protocols import AiologgerProtocol
from hyperscale.logging_rewrite.core.records import LogRecord


class AsyncStreamHandler(Handler):
    terminator = "\n"

    def __init__(
        self,
        stream=None,
        level: Union[str, int, LogLevel] = LogLevel.NOTSET,
        formatter: Formatter = None,
        filter: Filter = None,
    ) -> None:
        super().__init__()
        if stream is None:
            stream = sys.stderr
        self.stream = stream
        self.level = level
        if formatter is None:
            formatter = Formatter()
        self.formatter: Formatter = formatter
        if filter:
            self.add_filter(filter)
        self.protocol_class = AiologgerProtocol
        self._initialization_lock = asyncio.Lock()
        self.writer: Optional[StreamWriter] = None

    @property
    def initialized(self):
        return self.writer is not None

    async def _init_writer(self) -> StreamWriter:
        async with self._initialization_lock:
            if self.writer is not None:
                return self.writer

            loop = get_running_loop()
            transport, protocol = await loop.connect_write_pipe(
                self.protocol_class, self.stream
            )

            self.writer = StreamWriter(  # type: ignore # https://github.com/python/typeshed/pull/2719
                transport=transport, protocol=protocol, reader=None, loop=loop
            )
            return self.writer

    async def handle(self, record: LogRecord) -> bool:
        """
        Conditionally emit the specified logging record.
        Emission depends on filters which may have been added to the handler.
        """
        rv = self.filter(record)
        if rv:
            await self.emit(record)
        return rv

    async def flush(self):
        await self.writer.drain()

    async def emit(self, record: LogRecord):
        """
        Actually log the specified logging record to the stream.
        """
        if self.writer is None:
            self.writer = await self._init_writer()

        try:
            msg = self.formatter.format(record) + self.terminator

            self.writer.write(msg.encode())
            await self.writer.drain()
        except Exception as exc:
            await self.handle_error(record, exc)

    async def close(self):
        """
        Tidy up any resources used by the handler.

        This version removes the handler from an internal map of handlers,
        should ensure that this gets called from overridden close()
        methods.
        """
        if self.writer is None:
            return
        await self.flush()
        self.writer.close()
