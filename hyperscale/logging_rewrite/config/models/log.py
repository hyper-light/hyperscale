import msgspec
import threading
import datetime


class Log(msgspec.Struct, kw_only=True):
    entry: msgspec.Struct
    filename: str
    function_name: str
    line_number: int
    thread_id: int = msgspec.field(
        default_factory=threading.get_native_id,
    )
    timestamp: str = msgspec.field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )