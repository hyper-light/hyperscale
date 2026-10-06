import asyncio
from inspect import isawaitable, signature
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    get_args,
    get_type_hints,
)

from hyperscale.core.engines.client.shared.timeouts import Timeouts

from .state_action import StateAction

T = TypeVar("T")
K = TypeVar("K")


class ContextHook(Generic[T, K]):
    def __init__(
        self,
        workflows: List[str],
        call: Callable[..., Awaitable[Any]],
        timeouts: Optional[Timeouts] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        if timeouts is None:
            timeouts = Timeouts()

        self._call = call
        self.timeouts = timeouts
        self.tags = tags
        self.full_name = call.__qualname__
        self.name = call.__name__

        type_hints = get_type_hints(call)
        return_type = type_hints.get("return")

        return_types = list(get_args(return_type))
        if len(return_types) < 1:
            raise Exception(
                "Err. - must specify Use[T] or Provide[T] return type annotation."
            )

        self.state_type = return_types.pop()

        if len(workflows) < 1:
            workflows = [self.full_name.split(".").pop(0)]

        self.workflows = workflows

        self.action_type = (
            StateAction.PROVIDE
            if hasattr(return_type, "ACTION")
            and getattr(return_type, "ACTION") == StateAction.PROVIDE
            else StateAction.USE
        )

        self.result: T | Exception = None
        self.context_args: Dict[str, Any] = {}
        # Every parameter the hook declares can be filled from the context;
        # the documented hooks take them positional-or-keyword.
        self._hook_args = [arg.name for arg in signature(call).parameters.values()]

    async def call(self, *args, **kwargs):
        try:
            context_args = {
                name: value for name, value in kwargs.items() if name in self._hook_args
            }

            # A hook is a plain function -- the documented form, ``def``
            # returning ``Provide[T]``/``Use[T]`` -- or a coroutine function:
            # only an awaitable result is awaited, under the hook's timeout.
            # Awaiting a plain function's value raised TypeError, which was
            # stored as the provided value.
            result = self._call(*args, **context_args)
            if isawaitable(result):
                result = await asyncio.wait_for(
                    result,
                    timeout=self.timeouts.request_timeout,
                )

            return (self.name, result)

        except Exception as err:
            return (self.name, err)
