"""
Exception raised when production code reaches for an asyncio
entry point that SIM mode does not support.

Every external source of asyncio non-determinism is banned at
the ``SimulationLoop`` level by overriding the corresponding
method to raise this exception with a message identifying the
banned operation. ``SimulationConstraintError`` is intentionally
distinct from ``NotImplementedError`` and ``RuntimeError`` so
test failures can match on it precisely and so a regression
("a production code path now calls ``loop.run_in_executor``")
surfaces as a clear, attributable failure rather than getting
lost in a generic stack trace.

The banned surface — and the reason each is banned — is enumerated
in ``simulation_loop.py``. Adding a new banned operation is a
one-line addition there plus a unit test that calls the operation
and asserts ``SimulationConstraintError`` is raised.
"""


class SimulationConstraintError(RuntimeError):
    """Raised when SIM mode receives an unsupported asyncio call.

    Subclasses ``RuntimeError`` so it survives ``except Exception``
    handlers in production code (which would otherwise swallow the
    signal and surface as a mysterious test hang). Callers should
    not catch this exception — it indicates a structural bug.
    """
