from collections import defaultdict
from collections.abc import Sequence


class LastArgs:
    
    def __init__(self):
        self.data: dict[str, Sequence[object]] = defaultdict(list)
