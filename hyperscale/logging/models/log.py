import msgspec
import threading
import datetime
from typing import Generic, TypeVar
from .entry import Entry


T = TypeVar('T')


class Log(msgspec.Struct, Generic[T], kw_only=True):
    entry: Entry
    filename: str
    function_name: str
    line_number: int
    thread_id: int = msgspec.field(
        default_factory=threading.get_native_id,
    )
    timestamp: str = msgspec.field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )