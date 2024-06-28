from typing import Generic, TypeVar

T = TypeVar("T")


class Push(Generic[T]):
    IS_PUSH = True
