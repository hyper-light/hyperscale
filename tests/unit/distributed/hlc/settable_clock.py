class SettableClock:
    """A physical clock that reads exactly what the test sets, in Unix
    milliseconds -- for placing nodes' clocks relative to each other."""

    def __init__(self, unix_ms: int) -> None:
        self.unix_ms = unix_ms

    def time(self) -> float:
        return self.unix_ms / 1000.0

    def monotonic(self) -> float:
        return self.unix_ms / 1000.0
