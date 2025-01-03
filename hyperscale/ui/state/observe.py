import asyncio
from typing import TypeVar
from .message import Message
from .state_types import ActionData, Action
from .subscription_set import SubscriptionSet


K = TypeVar('K')
T = TypeVar('T', bound=ActionData)


def observe(
    trigger: Action[K, T], 
    subscriptions: SubscriptionSet,
    alias: str | None = None,
) -> Action[K, T]:
    
    if alias is None:
        alias = trigger.__name__

    async def wrap(*args, **kwargs):

        result: Message[T] | T = await trigger(*args, **kwargs)

        if isinstance(result, Message) and (
            updates := subscriptions.updates.get(result.channel)
        ):
            await asyncio.gather(*[
                update(result.data) for update in updates
            ])
            
        else:
            await asyncio.gather(*[update(result) for update in subscriptions.updates[alias]])

        return result
    
            
    return wrap
