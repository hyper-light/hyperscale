from collections import defaultdict
from typing import Any


class LastArgs:
    
    def __init__(self):
        self.data: dict[str, list[Any]] = defaultdict(list)
