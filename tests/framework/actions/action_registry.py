from typing import Awaitable, Callable

from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec
from tests.framework.results.action_outcome import ActionOutcome

ActionHandler = Callable[[ScenarioRuntime, ActionSpec], Awaitable[ActionOutcome]]


class ActionRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, ActionHandler] = {}

    def register(self, action_type: str, handler: ActionHandler) -> None:
        self._handlers[action_type] = handler

    def get(self, action_type: str) -> ActionHandler:
        if action_type not in self._handlers:
            raise ValueError(f"Unknown action type: {action_type}")
        return self._handlers[action_type]
