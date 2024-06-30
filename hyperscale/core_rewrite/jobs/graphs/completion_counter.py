import itertools


class CompletionCounter:
    def __init__(self) -> None:
        self._incs = itertools.count()
        self._reads = itertools.count()

    def increment(self):
        next(self._incs)

    def value(self):
        return next(self._incs) - next(self._reads)
