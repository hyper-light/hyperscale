"""Exceptions raised by the simulation harness."""


class HarnessError(Exception):
    """Base class for all simulation-harness exceptions."""


class PortConflictError(HarnessError):
    """A port the harness wants to bind is already held by another process."""


class ReapError(HarnessError):
    """A process or descendant could not be reaped within the configured budget."""


class LeakedAsyncTasksError(HarnessError):
    """asyncio tasks remained alive after teardown that the harness did not own."""


class PreflightZombieError(HarnessError):
    """Pre-flight detected zombies from a prior harness run that could not be reaped."""
