from collections import defaultdict
from typing import Callable
from typing import List, Callable, Dict
from .state_types import ActionData


class SubscriptionSet:

    def __init__(self):
        self.updates: Dict[str, List[Callable[[ActionData], None]]] = defaultdict(list)
    
    def add_topic(self, topic: str, update_funcs: List[Callable[[ActionData], None]]):
        self.updates[topic].extend(update_funcs)
