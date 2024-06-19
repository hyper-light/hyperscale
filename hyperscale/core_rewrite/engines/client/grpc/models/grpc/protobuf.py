from typing import TypeVar, Generic
from google.protobuf.message import Message


T = TypeVar('T')


class Protobuf(Generic[T], Message):
    pass