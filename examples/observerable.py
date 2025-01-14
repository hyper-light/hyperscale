import asyncio
import inspect
from typing import Dict, Generic, TypeVar, Callable, Awaitable
from hyperscale.ui.state import Action, ActionData

K = TypeVar("K", bound=dict)
S = TypeVar("S", bound=dict)
T = TypeVar("T", bound=ActionData)


ComponentUpdate = Callable[[T], Awaitable[None]]
ObservedAction = Callable[[T], Awaitable[T] | T]


def observe(trigger: Action[T], update: ComponentUpdate[T], store: asyncio.Queue[T]):
    if inspect.isawaitable(trigger):

        async def wrap(*args: tuple[T, ...], **kwargs: dict[str, T]):
            previous = await store.get()

            result = await trigger(previous, *args, **kwargs)
            store.put_nowait(result)

            await update(result)

            return result

    else:

        async def wrap(*args: tuple[T, ...], **kwargs: dict[str, T]):
            previous = await store.get()

            result = trigger(previous, *args, **kwargs)
            store.put_nowait(result)

            await update(result)

            return result

    return wrap


class Store(Generic[S, K]):
    def __init__(self, state: S, observables: K):
        self._store = {key: asyncio.Queue() for key in state}

        for key, value in state.items():
            self._store[key].put_nowait(value)

        self._observables = observables
        self._state_locks: dict[str, asyncio.Lock] = {
            key: asyncio.Lock() for key in observables
        }

    def wrap_action(
        self, topic: K, component_update_call: ComponentUpdate[T]
    ) -> ObservedAction[T]:
        if observable := self._observables.get(topic):
            return observe(
                observable,
                component_update_call,
                self._store[topic],
            )


def add(prev: int, next: int):
    return prev + next


async def update(data: int):
    print("GOT", data)


store = Store(
    {
        "add": 0,
    },
    {"add": add},
)

addCount = store.wrap_action("add", update)


async def run():
    await addCount(1)
    await addCount(1)
    await addCount(1)


asyncio.run(run())
