"""
SimSystemResources — fixed machine telemetry for SIM mode.

Real telemetry drifts with whatever else the host is doing (earlier
test runs included), so SIM children read a constant machine: same
memory, same core count, same utilization, every run, everywhere.
Values are constructor-configurable so scenarios can model small or
loaded machines deliberately — variation must be an explicit scenario
input, never ambient.
"""


class SimSystemResources:
    """Constant ``SystemResources`` for deterministic simulation."""

    __slots__ = (
        "_total_memory_bytes",
        "_available_memory_bytes",
        "_cpu_count",
        "_cpu_percent",
    )

    def __init__(
        self,
        total_memory_bytes: int = 16 * 1024**3,
        available_memory_bytes: int = 8 * 1024**3,
        cpu_count: int = 8,
        cpu_percent: float = 12.5,
    ) -> None:
        self._total_memory_bytes = total_memory_bytes
        self._available_memory_bytes = available_memory_bytes
        self._cpu_count = cpu_count
        self._cpu_percent = cpu_percent

    def total_memory_bytes(self) -> int:
        return self._total_memory_bytes

    def available_memory_bytes(self) -> int:
        return self._available_memory_bytes

    def cpu_count(self, logical: bool = True) -> int:
        return self._cpu_count

    def cpu_percent(self) -> float:
        return self._cpu_percent
