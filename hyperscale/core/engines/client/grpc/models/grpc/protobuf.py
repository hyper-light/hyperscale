from typing import Generic, TypeVar

from google.protobuf.message import Message

T = TypeVar("T")


class Protobuf(Generic[T], Message):
    pass
