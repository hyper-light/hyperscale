from typing import AsyncIterable, TypeVar

from hyperscale.distributed.models.base.message import Message

from .call import Call

T = TypeVar("T", bound=Message)


Stream = AsyncIterable[Call[T]]
