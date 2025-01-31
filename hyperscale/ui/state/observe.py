import asyncio
from collections import defaultdict
from typing import TypeVar, Any
from .state_types import ActionData, Action
from .subscription_set import (
    SubscriptionSet, 
)


K = TypeVar("K")
T = TypeVar("T", bound=ActionData)




def observe(
    trigger: Action[K, T],
    subscriptions: SubscriptionSet,
    default_channel: str | None = None,
) -> Action[K, T]:
    if default_channel is None:
        default_channel = trigger.__name__

    subscriptions.default_channels[trigger.__name__] = default_channel
    subscriptions.triggers[trigger.__name__] = trigger


    async def wrap(*args, **kwargs):

        if len(args) > 0:
            subscriptions.last_args.data[trigger.__name__] = args

        if len(kwargs) > 0:
            subscriptions.last_kwargs.data[trigger.__name__] = kwargs
        
        result = await trigger(*args, **kwargs)

        channel = default_channel
        data: ActionData | None = None

        if isinstance(result, tuple) and len(result) == 2:
            channel, data = result
            updates = subscriptions.updates.get(channel)

        else:
            updates = subscriptions.updates.get(channel)
            data = result

        if updates is not None and data is not None:
            await asyncio.gather(
                *[update(data) for update in updates], return_exceptions=True
            )

        return result

    return wrap
