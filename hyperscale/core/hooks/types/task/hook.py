from typing import Any, Awaitable, Callable, Dict, List, Tuple, Type, Union

from hyperscale.core.engines.types.common.base_action import BaseAction
from hyperscale.core.engines.types.common.base_engine import BaseEngine
from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.hook_metadata import HookMetadata
from hyperscale.core.hooks.types.base.hook_type import HookType


class TaskHook(Hook):
    def __init__(
        self,
        name: str,
        shortname: str,
        call: Callable[..., Awaitable[Any]],
        *names: Tuple[str, ...],
        weight: int = 1,
        order: int = 1,
        skip: bool = False,
        metadata: Dict[str, Union[str, int]] = {},
    ) -> None:
        super().__init__(
            name, shortname, call, order=order, skip=skip, hook_type=HookType.TASK
        )

        self.names = list(set(names))
        self.call: Type[self._call] = self._call
        self.session: BaseEngine = None
        self.action: BaseAction = None
        self.order = order
        self.before: List[Any] = []
        self.after: List[Any] = []
        self.is_notifier = False
        self.is_listener = False
        self.checks: List[Any] = []
        self.channels: List[Any] = []
        self.notifiers: List[Any] = []
        self.listeners: List[Any] = []
        self.metadata = HookMetadata(weight=weight, order=order, **metadata)

    def copy(self):
        task_hook = TaskHook(
            self.name,
            self.shortname,
            self._call,
            weight=self.metadata.weight,
            order=self.order,
            skip=self.skip,
            metadata={**self.metadata.copy()},
        )

        task_hook.checks = list(self.checks)
        task_hook.stage = self.stage

        return task_hook

    async def call(self, *args, **kwargs):
        return await self._call(*args, **kwargs)
