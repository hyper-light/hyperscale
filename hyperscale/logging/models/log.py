import datetime
import os
from typing import Generic, TypeVar

import msgspec

from .entry import Entry


T = TypeVar("T")


class Log(msgspec.Struct, Generic[T], kw_only=True):
    entry: Entry
    filename: str
    function_name: str
    line_number: int
    # The writing process: hyperscale runs one asyncio thread per process,
    # so the process is what tells one writer from another (and on Linux
    # the main thread's native id is the pid).
    thread_id: int = msgspec.field(
        default_factory=os.getpid,
    )
    timestamp: str = msgspec.field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )
    lsn: int | None = None
