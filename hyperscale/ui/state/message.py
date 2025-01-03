from typing import TypeVar, Generic
from .state_types import ActionData


T = TypeVar('T', bound=ActionData)


class Message(Generic[T]):
    __slots__ = (
        'channel',
        'data'
    )

    def __init__(
        self,
        channel: str,
        data: T
    ):
        super().__init__()

        self.channel = channel
        self.data: T = data