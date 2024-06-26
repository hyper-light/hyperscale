import inspect
from typing import Any, Coroutine, Dict, List

from hyperscale.core.engines.types.common.base_action import BaseAction
from hyperscale.core.engines.types.common.hooks import Hooks
from hyperscale.core.engines.types.common.types import RequestTypes


class Task(BaseAction):
    __slots__ = (
        "action_id",
        "protocols",
        "name",
        "is_setup",
        "metadata",
        "hooks",
        "type",
        "source",
        "execute",
        "event",
        "args",
        "params",
        "task_args",
        "mutations",
    )

    def __init__(
        self,
        name: str,
        task_action: Coroutine,
        source: str = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
    ):
        super(Task, self).__init__(name, user, tags)

        self.type = RequestTypes.TASK
        self.source = source
        self.execute = task_action
        self.hooks: Hooks[Task] = Hooks()

        self.args = inspect.signature(task_action)
        self.params = self.args.parameters
        self.task_args: Dict[str, Any] = {}
