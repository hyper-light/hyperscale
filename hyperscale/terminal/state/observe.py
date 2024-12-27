import asyncio
from typing import TypeVar, Callable, Awaitable, TypeVar
from pydantic import StrictStr, StrictInt, StrictFloat
from typing import List, Callable, Awaitable, TypeVar, Tuple, Dict, Any


BaseDataType = StrictInt | StrictFloat | StrictStr


ActionData = (
    BaseDataType |
    List[BaseDataType] |
    Tuple[BaseDataType, ...] |
    List[Tuple[BaseDataType, ...]] | 
    Dict[StrictStr, BaseDataType]
)

K = TypeVar('K')
T = TypeVar('T', bound=ActionData)


Action = Callable[[K], Awaitable[T]]


ComponentUpdate = Callable[[T], Awaitable[None]]


def observe(
    trigger: Action[K, T], 
    updates: List[ComponentUpdate[T]],
):
    async def wrap(*args: tuple[Any, ...], **kwargs: Dict[str, Any]) -> Awaitable[T]:

        result = await trigger(*args, **kwargs)

        await asyncio.gather(*[update(result) for update in updates])

        return result
            
    return wrap
