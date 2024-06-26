from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Tuple,
    Union,
)

from hyperscale.core.engines.types.common.base_action import BaseAction
from hyperscale.core.engines.types.common.base_engine import BaseEngine
from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.hook_metadata import HookMetadata
from hyperscale.core.hooks.types.base.hook_type import HookType


class ActionHook(Hook):
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
            name,
            shortname,
            call,
            skip=skip,
            order=order,
            hook_type=HookType.ACTION,
        )

        self.names = list(set(names))
        self.session: BaseEngine = None
        self.action: BaseAction = None
        self.checks: List[Any] = []
        self.before: List[str] = []
        self.after: List[str] = []
        self.is_notifier = False
        self.is_listener = False
        self.channels: List[Any] = []
        self.notifiers: List[Any] = []
        self.listeners: List[Any] = []
        self.metadata = HookMetadata(weight=weight, order=order, **metadata)

    def copy(self):
        action_hook = ActionHook(
            self.name,
            self.shortname,
            self._call,
            weight=self.metadata.weight,
            order=self.order,
            skip=self.skip,
            metadata={**self.metadata.copy()},
        )

        action_hook.checks = list(self.checks)
        action_hook.stage = self.stage

        return action_hook

    async def call(self, *args, **kwargs):
        return await self._call(*args, **kwargs)

    def to_dict(self) -> str:
        return {
            "name": self.name,
            "shortname": self.shortname,
            "skip": self.skip,
            "hook_type": HookType.ACTION,
            "names": self.names,
            "checks": [
                check if isinstance(check, str) else check.name for check in self.checks
            ],
            "channels": [
                channel if isinstance(channel, str) else channel.name
                for channel in self.channels
            ],
            "notifiers": [
                notifier if isinstance(notifier, str) else notifier.name
                for notifier in self.notifiers
            ],
            "listeners": [
                listener if isinstance(listener, str) else listener.name
                for listener in self.listeners
            ],
            "order": self.metadata.order,
            "weight": self.metadata.weight,
            "user": self.metadata.user,
            "tags": self.metadata.tags,
        }
