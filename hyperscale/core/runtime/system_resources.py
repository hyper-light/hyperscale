"""
SystemResources — the machine-telemetry seam.

Machine state (memory, CPU counts and utilization) is an environmental
non-determinism input exactly like the wall clock, the RNG seed, hash
randomization, and the disk — the coordinator pins or seams every one
of those, and telemetry belongs on the list: real reads drift with
whatever else the machine is doing, and any production branch or
payload built on them diverges run-to-run. (Found empirically: SIM
replay twins diverged only when earlier runs in the same test process
had shifted the machine's available memory — the reads rode into
registration payloads and flipped downstream scheduling by one poll
quantum.)

Production code takes a ``SystemResources`` (module-seam singleton,
externally rebound by ``swap_defaults``); REAL mode binds the
psutil-backed implementation, SIM mode binds fixed configurable
values.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class SystemResources(Protocol):
    """Machine-telemetry reads production is allowed to perform."""

    def total_memory_bytes(self) -> int:
        """Total physical memory."""
        ...

    def available_memory_bytes(self) -> int:
        """Currently available physical memory."""
        ...

    def cpu_count(self, logical: bool = True) -> int:
        """Number of CPUs (logical or physical cores)."""
        ...

    def cpu_percent(self) -> float:
        """Instantaneous CPU utilization percentage (0-100)."""
        ...
