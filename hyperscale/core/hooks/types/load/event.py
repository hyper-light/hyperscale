from collections import defaultdict

from hyperscale.core.hooks.types.base.event import BaseEvent
from hyperscale.core.hooks.types.base.event_types import EventType
from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.load.hook import LoadHook


class LoadEvent(BaseEvent[LoadHook]):
    def __init__(self, target: Hook, source: LoadHook) -> None:
        super(LoadEvent, self).__init__(target, source)

        self.event_type = EventType.LOAD

    def copy(self):
        load_event = LoadEvent(self.target.copy(), self.source.copy())

        load_event.execution_path = list(self.execution_path)
        load_event.previous_map = list(self.previous_map)
        load_event.next_map = list(self.next_map)
        load_event.next_args = defaultdict(dict)

        return load_event
