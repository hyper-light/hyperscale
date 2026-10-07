from collections import defaultdict
from typing import Any


class LastKwargs:
    
    def __init__(self):
        self.data: dict[str, dict[str, Any]] = defaultdict(dict)
