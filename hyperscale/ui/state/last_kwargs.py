from collections import defaultdict


class LastKwargs:
    
    def __init__(self):
        self.data: dict[str, dict[str, object]] = defaultdict(dict)
