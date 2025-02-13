from typing import Generic, TypeVar

try:

    from google.protobuf.message import Message

except Exception:
    class Message:
        pass

T = TypeVar("T")


class Protobuf(Generic[T], Message):
    pass
