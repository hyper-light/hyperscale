from typing import Dict, Union

from hyperscale.core.hooks.types.action.event import ActionEvent
from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.channel.event import ChannelEvent
from hyperscale.core.hooks.types.check.event import CheckEvent
from hyperscale.core.hooks.types.condition.event import ConditionEvent
from hyperscale.core.hooks.types.context.event import ContextEvent
from hyperscale.core.hooks.types.event.event import Event
from hyperscale.core.hooks.types.load.event import LoadEvent
from hyperscale.core.hooks.types.metric.event import MetricEvent
from hyperscale.core.hooks.types.save.event import SaveEvent
from hyperscale.core.hooks.types.task.event import TaskEvent
from hyperscale.core.hooks.types.transform.event import TransformEvent

HedraEvent = Union[
    Event,
    TransformEvent,
    ConditionEvent,
    ContextEvent,
    SaveEvent,
    LoadEvent,
    CheckEvent,
]


def get_event(target: Hook, source: Hook) -> HedraEvent:
    event_types: Dict[HookType, HedraEvent] = {
        HookType.ACTION: ActionEvent,
        HookType.CHANNEL: ChannelEvent,
        HookType.CHECK: CheckEvent,
        HookType.CONDITION: ConditionEvent,
        HookType.CONTEXT: ContextEvent,
        HookType.EVENT: Event,
        HookType.LOAD: LoadEvent,
        HookType.METRIC: MetricEvent,
        HookType.SAVE: SaveEvent,
        HookType.TASK: TaskEvent,
        HookType.TRANSFORM: TransformEvent,
    }

    return event_types.get(source.hook_type, Event)(target, source)
