from typing import TypeVar

T = TypeVar("T")


class Response(tuple[int, T]):
    pass
