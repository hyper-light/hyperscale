import sys
from collections.abc import Callable
from typing import TypeVar

import msgspec

from hyperscale.core.runtime.real_filesystem import RealFilesystem

from .models import PingResult
from .ping_result_serializer import PingResultSerializer

ResponseT = TypeVar("ResponseT")


class PingResultOutput:
    """Writes a ``hyperscale ping`` request's result to ``--filepath``.

    The result is written once, as one JSON object, the moment the request
    completes: atomically (temp file in the destination's directory, fsync,
    rename, directory fsync), so the path holds either the whole result or
    nothing. A request that raised is written as an error-only result.

    A failed write is held, not raised, so the terminal UI can stop first;
    ``raise_on_write_failure`` then reports it on stderr and exits the
    command non-zero. With no ``--filepath`` nothing is built or written.
    """

    __slots__ = ("_output_file", "_serializer", "_recorded", "_write_error")

    def __init__(self, output_file: str | None, serializer: PingResultSerializer) -> None:
        self._output_file = output_file
        self._serializer = serializer
        self._recorded = False
        self._write_error: Exception | None = None

    async def record(self, build_result: Callable[[ResponseT], PingResult], response: ResponseT) -> None:
        """Build the result from ``response`` and write it to the output
        file, holding any failure for ``raise_on_write_failure``."""
        if self._output_file is None:
            return
        self._recorded = True
        filesystem = RealFilesystem(max_workers=1)
        try:
            await filesystem.atomic_write(self._output_file, msgspec.json.encode(build_result(response)))
        except Exception as write_error:
            self._write_error = write_error
        finally:
            filesystem.shutdown()

    async def record_failure(self, error: Exception) -> None:
        """Write the error-only result of a request that raised, unless the
        request's own result was already recorded."""
        if self._output_file is None or self._recorded:
            return
        await self.record(self._serializer.from_error, error)

    def raise_on_write_failure(self) -> None:
        """Report a failed write on stderr and exit non-zero. Call once the
        terminal UI has stopped."""
        if self._write_error is None:
            return
        print(
            f"hyperscale ping: could not write the result to {self._output_file}: {self._write_error}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1) from self._write_error
