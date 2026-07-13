"""
RealSystemResources — the psutil-backed ``SystemResources``.

Like ``RealFilesystem``, this adapter is the documented exception to
the interfaces-only rule for ``hyperscale.core.runtime``: it is the
single place production reads real machine telemetry, so the
determinism lint can forbid direct ``psutil`` use everywhere else.
Degrades gracefully when psutil is unavailable (0 / os.cpu_count
fallbacks — the same behavior the scattered call sites implemented
individually).
"""

import os

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:  # pragma: no cover - psutil is a standard dependency
    psutil = None
    HAS_PSUTIL = False


class RealSystemResources:
    """Live machine telemetry via psutil."""

    __slots__ = ()

    def total_memory_bytes(self) -> int:
        if not HAS_PSUTIL:
            return 0
        return int(psutil.virtual_memory().total)

    def available_memory_bytes(self) -> int:
        if not HAS_PSUTIL:
            return 0
        return int(psutil.virtual_memory().available)

    def cpu_count(self, logical: bool = True) -> int:
        if HAS_PSUTIL:
            counted = psutil.cpu_count(logical=logical)
            if counted:
                return counted
        return os.cpu_count() or 1

    def cpu_percent(self) -> float:
        if not HAS_PSUTIL:
            return 0.0
        return float(psutil.cpu_percent(interval=None))
