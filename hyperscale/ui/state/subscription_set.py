import asyncio
import itertools
import signal
from collections import defaultdict
from typing import Callable, TypeVar
from typing import List, Callable, Dict, Any
from .last_args import LastArgs as LastArgs
from .last_kwargs import LastKwargs as LastKwargs
from .state_types import ActionData, Action


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

    def remove_updates(self, update_funcs: List[Callable[[ActionData], None]]):
        """Unsubscribe ``update_funcs`` from every topic: a stopped
        terminal's components must neither receive nor hold updates."""
        removed = set(update_funcs)
        for topic, topic_updates in list(self.updates.items()):
            self.updates[topic] = list(itertools.filterfalse(removed.__contains__, topic_updates))

    
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
