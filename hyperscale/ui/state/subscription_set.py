import asyncio
import signal
from collections import defaultdict
from typing import Callable, TypeVar
from typing import List, Callable, Dict, Any
from .state_types import ActionData, Action



class LastArgs:
    
    def __init__(self):
        self.data: dict[str, list[Any]] = defaultdict(list)

class LastKwargs:
    
    def __init__(self):
        self.data: dict[str, dict[str, Any]] = defaultdict(dict)



K = TypeVar("K")
T = TypeVar("T", bound=ActionData)


class SubscriptionSet:
    def __init__(self):
        self.updates: Dict[str, List[Callable[[ActionData], None]]] = defaultdict(list)
        self.last_args = LastArgs()
        self.last_kwargs = LastKwargs()
        self.default_channels: dict[str, str] = {}
        self.triggers: dict[str, Action[K, T]] = {}


    def add_topic(self, topic: str, update_funcs: List[Callable[[ActionData], None]]):
        self.updates[topic].extend(update_funcs)

    
    async def rerender_last(
        self,
        trigger: Action[K, T],
    ):
        try:

            trigger_name = trigger.__name__

            result = await trigger(
                *self.last_args.data[trigger_name],
                **self.last_kwargs.data[trigger_name],
            )

            channel = self.default_channels[trigger_name]
            data: ActionData | None = None

            if isinstance(result, tuple) and len(result) == 2:
                channel, data = result
                updates = self.updates.get(channel)

            else:
                updates = self.updates.get(channel)
                data = result

            if updates is not None and data is not None:
                await asyncio.gather(
                    *[update(data) for update in updates], 
                    return_exceptions=True,
                )

        except Exception:
            pass
