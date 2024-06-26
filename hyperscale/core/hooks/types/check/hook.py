from typing import Any, Awaitable, Callable, List

from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.hook_type import HookType


class CheckHook(Hook):
    def __init__(
        self,
        name: str,
        shortname: str,
        call: Callable[..., Awaitable[Any]],
        *names: List[str],
        message: str = "Did not return True.",
        order: int = 1,
        skip: bool = False,
    ) -> None:
        super().__init__(
            name, shortname, call, order=order, skip=skip, hook_type=HookType.CHECK
        )

        self.message = message
        self.names = list(set(names))

    async def call(self, **kwargs):
        if self.skip:
            return kwargs

        passed = await self._call(
            **{name: value for name, value in kwargs.items() if name in self.params}
        )

        result = kwargs.get("result")

        if passed is False and result:
            result.error = f"Check - {self.name} - failed. Context - {self.message}"

        if isinstance(passed, dict):
            return {**kwargs, **passed}

        return {**kwargs, self.shortname: passed}

    def copy(self):
        check_hook = CheckHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            order=self.order,
            skip=self.skip,
        )

        check_hook.stage = self.stage

        return check_hook
