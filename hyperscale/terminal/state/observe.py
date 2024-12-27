import asyncio
import functools
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



class SubscriptionSet:

    def __init__(self):
        self.updates: Dict[str, Callable[[ActionData], None]] = {}



def observe(
    trigger: Action[K, T], 
    subscriptions: SubscriptionSet
) -> Action[K, T]:
    
    topic = trigger.__name__

    async def wrap(*args, **kwargs):

        result = await trigger(*args, **kwargs)
        await asyncio.gather(*[update(result) for update in subscriptions.updates[topic]])

        return result
    
            
    return wrap
