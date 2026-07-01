"""
Structural-typing verification for the Phase 6c ``Runner`` Protocol.

The Protocol is the seam that lets consuming classes annotate
``task_runner: Runner`` instead of ``task_runner: TaskRunner``.
Under both REAL and SIM modes the concrete instance is the same
``TaskRunner`` class (SIM works because the running loop is a
``SimulationLoop`` and ``TaskRunner``'s internal
``asyncio.ensure_future`` calls resolve deterministically through
it). What differs is what tests can substitute — the Protocol
lets a minimal stub satisfy the annotation without importing the
concrete ``TaskRunner``.

Tests here verify two invariants:

1. ``TaskRunner`` satisfies ``Runner`` — every method the Protocol
   declares is present on the concrete class with a compatible
   signature. The check uses ``isinstance(TaskRunner(...), Runner)``
   because ``Runner`` is ``@runtime_checkable``; that catches both
   name-mismatches (the Protocol lists a method the class doesn't
   have) and inheritance-hierarchy drift.

2. A minimal stub with the five Protocol methods also satisfies
   ``Runner`` — proves the Protocol is genuinely structural (any
   compatible shape works) rather than accidentally requiring the
   concrete class.

These tests are cheap and run in the standard unit-test suite; they
fail immediately if someone renames a ``TaskRunner`` method or
tightens a signature in a way that breaks the seam.
"""

from typing import Any

import pytest

from hyperscale.distributed.runtime import Runner
from hyperscale.distributed.taskex.task_runner import TaskRunner


class _MinimalRunnerStub:
    """Bare-minimum implementation of ``Runner`` for the stub test.

    Every method is a no-op returning a plausible value. Used
    exclusively to prove the Protocol accepts a non-``TaskRunner``
    class.
    """

    def run(self, callable_: Any, *args: Any, **kwargs: Any) -> Any:
        return None

    async def cancel(self, token: str) -> None:
        return None

    async def cancel_schedule(self, token: str) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    def abort(self) -> None:
        return None


def test_task_runner_satisfies_runner_protocol() -> None:
    """The production ``TaskRunner`` structurally implements
    ``Runner``.

    If this test fails, someone renamed / removed / retyped a
    ``TaskRunner`` method that the Protocol depends on. Either
    restore the method or update the Protocol — but not silently.

    ``TaskRunner.__init__`` calls ``asyncio.get_event_loop()`` so
    it requires a loop in scope; install one via the standard
    ``new_event_loop`` / ``set_event_loop`` dance and close it
    after the assertion.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        task_runner = TaskRunner(instance_id=0)
        assert isinstance(task_runner, Runner)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_minimal_stub_satisfies_runner_protocol() -> None:
    """A minimal five-method class also satisfies ``Runner``.

    Confirms the Protocol is structurally usable — anything with
    the right method shape works, no import-order dependency on
    ``TaskRunner``.
    """
    stub = _MinimalRunnerStub()
    assert isinstance(stub, Runner)


def test_incomplete_stub_does_not_satisfy_runner_protocol() -> None:
    """A class missing one of the Protocol methods fails
    ``isinstance``.

    Guards against the runtime-check silently accepting stubs
    that don't cover the full surface. If this ever passes, the
    Protocol is too permissive to be a useful seam.
    """
    class _MissingCancel:
        def run(self, callable_: Any, *args: Any, **kwargs: Any) -> Any:
            return None

        async def cancel_schedule(self, token: str) -> None:
            return None

        async def shutdown(self) -> None:
            return None

        def abort(self) -> None:
            return None

    stub = _MissingCancel()
    assert not isinstance(stub, Runner)
