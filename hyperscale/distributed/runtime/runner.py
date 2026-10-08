"""
Runner interface — the dependency-injection seam for background
task submission and lifecycle management.

Why a Protocol
--------------

The existing ``TaskRunner`` in ``hyperscale/distributed/taskex/`` is
the production implementation: it accepts async callables, submits
them to the running event loop via ``asyncio.ensure_future``, and
tracks each submission as a ``Run`` under a named ``Task`` for
cancellation and status queries. Under SIM, the same ``TaskRunner``
class works unchanged — its internal ``asyncio.ensure_future`` calls
resolve to ``SimulationLoop.create_task`` when the SimulationLoop is
the running loop, giving the same deterministic ordering guarantees
the rest of the Phase 6 primitives provide.

What the ``Runner`` Protocol adds is a **typing seam**: consuming
classes that today annotate ``task_runner: TaskRunner`` become
``task_runner: Runner``. The concrete class stays the same. The
seam matters for two reasons:

1. It removes the implicit dependency on ``taskex.task_runner.TaskRunner``
   being importable and constructible in tests that mock the runner.
   Callers that only need ``.run(...)`` or ``.cancel(...)`` can pass a
   minimal stub that satisfies the Protocol without importing the
   concrete class.

2. It gives Phase 6 a documented, forward-compatible contract for
   what the SIM harness expects of a task runner. If a future
   Phase 7+ ever introduces a truly distinct SIM-mode runner (e.g.
   a step-based scheduler that decouples from asyncio entirely),
   the Protocol is the swap point — no consumer-side changes
   required.

Existing precedent
------------------

An analogous ``TaskRunnerProtocol`` already exists at
``hyperscale/distributed/swim/core/protocols.py:36`` and is used by
two SWIM classes (``HealthMonitor``, ``LocalLeaderElection``). Phase 6c
promotes that Protocol to the runtime layer so every consumer under
``hyperscale/distributed/`` can import from one canonical location,
consistent with the Phase 5 ``Clock`` / ``Random`` / ``Transport``
seams.

What's on the Protocol
----------------------

Exactly the methods that production callsites actually call. A
survey of every ``self._task_runner.X`` reference under
``hyperscale/distributed/`` (excluding the debug ``.tasks`` dict
access, which two callsites use for a length count and which is
better left as a concrete-class-specific detail):

* ``run(call, *args, **kwargs)`` — submit a callable for async
  execution. Returns a ``Run`` object with a ``.token`` field for
  later ``cancel`` lookup.
* ``cancel(token)`` — cancel a specific ``Run`` by its token.
  Returns True if the run was found and cancelled.
* ``cancel_schedule(token)`` — cancel a scheduled (periodic /
  repeating) task run.
* ``shutdown()`` — graceful shutdown; cancels all outstanding
  runs and awaits their completion.
* ``abort()`` — forceful shutdown; kills executor threads /
  processes without draining.

The Protocol is ``@runtime_checkable`` so tests can
``isinstance(runner, Runner)`` when needed, at the cost of a
per-check hasattr scan. The existing ``TaskRunnerProtocol`` in the
SWIM tree already carries the ``@runtime_checkable`` decorator and
those callsites depend on it.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    # ``taskex`` imports the runtime seams; a runtime import back would cycle.
    from hyperscale.distributed.taskex.run import Run


@runtime_checkable
class Runner(Protocol):
    """Submit async work to the event loop and manage its lifecycle.

    The concrete production implementation is
    ``hyperscale.distributed.taskex.task_runner.TaskRunner``; SIM
    mode uses the same class under a ``SimulationLoop`` (see
    ``docs/dev/simulation_framework.md`` §16). This Protocol is a
    typing seam — consuming classes narrow their ``task_runner``
    parameter to ``Runner`` so tests can substitute lightweight
    stubs without importing the concrete runner.
    """

    def run(
        self,
        call: Callable[..., Awaitable[object]],
        *args: object,
        alias: str | None = None,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        **kwargs: object,
    ) -> "Run[object] | None":
        """Submit ``call`` for async execution.

        The signature is ``TaskRunner.run``'s (and ``RunTask``'s, its
        bound-method form): ``call`` runs with ``args`` and ``kwargs``
        under the task named ``alias`` (else ``call``'s name), once or
        on ``schedule``. A task already registered under that name runs
        its own call, so the result is typed ``object``.

        Returns the submitted ``Run`` -- whose ``.token`` callers use to
        reference the submission later -- or ``None`` when the runner
        skips the task or nothing ran.
        """
        ...

    async def cancel(self, token: str) -> None:
        """Cancel a submitted ``Run`` by its token.

        The concrete implementation is fire-and-forget: it looks
        up the run and calls ``cancel`` on the underlying asyncio
        task; if the token doesn't match, the call silently no-ops.
        Every production callsite ignores the return value.
        """
        ...

    async def cancel_schedule(self, token: str) -> None:
        """Cancel a scheduled (repeating) ``Run`` by its token.

        Async (matching ``cancel``) because the concrete
        implementation awaits the underlying task's cleanup.
        """
        ...

    async def shutdown(self) -> None:
        """Graceful shutdown: cancel outstanding runs and await
        their completion.

        Async so the caller can compose it with other teardown
        awaitables.
        """
        ...

    def abort(self) -> None:
        """Forceful shutdown: kill executor threads / processes
        without draining.

        Sync because it's called from cleanup paths where blocking
        on an event loop would be wrong.
        """
        ...
